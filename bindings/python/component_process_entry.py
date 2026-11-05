"""Actual app-owned Python host executable used by test_component_process."""

import json
import os
from pathlib import Path
import threading
import traceback

import reiny_component as r


NS = 15_000_000_000
PAYLOAD = b"\x0a\x03\x00\xff\x00"
RESPONSE = b"\x08\x07"
_output = threading.Lock()


def event(name, **fields):
    with _output:
        print(json.dumps({"event": name, "pid": os.getpid(), **fields}), flush=True)


def listening_endpoint():
    """Discover the actual kernel-assigned listener without a free-port race."""
    inodes = set()
    for fd in Path("/proc/self/fd").iterdir():
        try:
            target = os.readlink(fd)
        except FileNotFoundError:
            continue
        if target.startswith("socket:["):
            inodes.add(target[8:-1])
    endpoints = []
    for line in Path("/proc/net/tcp").read_text().splitlines()[1:]:
        fields = line.split()
        address, state, inode = fields[1], fields[3], fields[9]
        host, port = address.split(":")
        if state == "0A" and inode in inodes and host == "0100007F":
            endpoints.append(f"tcp/127.0.0.1:{int(port, 16)}")
    if len(endpoints) != 1:
        raise AssertionError(f"expected one owned loopback listener, got {endpoints}")
    return endpoints[0]


class Foreign(r.Component):
    def __init__(self, context, library):
        self.library = library
        self.role = context.info["id"]
        self.mode = os.environ["REINY_PROCESS_MODE"]
        self.lane = threading.get_ident()
        self.endpoints = []
        self.stop_count = 0
        event("created", role=self.role)

    def on_lane(self):
        assert self.lane == threading.get_ident()

    def prepare(self, context):
        self.on_lane()
        if self.mode == "prepare_failure":
            event("prepare_failed", role=self.role)
            raise ValueError("intentional external Python preparation failure")
        if self.role in ("producer", "consumer"):
            with self.library.schema("fixture.Packet", 42) as packet, \
                    self.library.schema("fixture.Response", 99) as response:
                if self.role == "producer":
                    self.stream = context.output("outgoing", packet)
                    self.rpc = context.rpc_client("echo", packet, response)
                    self.presence = context.watch_input_ready("echo")
                else:
                    self.stream = context.input("incoming", packet)
                    self.rpc = context.rpc_server("echo", packet, response)
                    self.presence = context.watch_input_ready("incoming")
                self.endpoints.extend([self.stream, self.rpc, self.presence])
            if self.role == "consumer":
                event("data_listener", endpoint=listening_endpoint())
        event("prepared", role=self.role)

    def run(self, context):
        self.on_lane()
        event("running", role=self.role)
        if self.mode == "unresponsive":
            threading.Event().wait()  # Only executable exit may end this callback.
            raise AssertionError("unreachable")
        if self.role in ("producer", "consumer"):
            assert self.presence.next(NS)["joined"]
            if self.role == "producer":
                # The first actual RPC is an event gate, not a timed discovery guess.
                with self.rpc.call_start(b"begin", NS) as operation:
                    assert operation.wait(NS) == b"ready"
                for payload in (PAYLOAD, b""):
                    self.stream.send(payload)
                for expected in (RESPONSE, r.RemoteError, r.NoReplyError):
                    with self.rpc.call_start(PAYLOAD, NS) as operation:
                        if isinstance(expected, bytes):
                            assert operation.wait(NS) == expected
                            event("rpc_success", payload=list(expected))
                        else:
                            try:
                                operation.wait(NS)
                            except expected as error:
                                event("rpc_error", status=int(error.status))
                            else:
                                raise AssertionError("incorrect RPC error propagation")
            else:
                with self.rpc.receive(NS) as request:
                    assert request.payload == b"begin"
                    request.reply(b"ready")
                for payload in (PAYLOAD, b""):
                    with self.stream.receive(NS) as message:
                        assert message.payload == payload
                        assert message.source == "producer"
                        assert message.source_port == "outgoing"
                        assert message.info["schema"] == 42
                        event("delivered", payload=list(message.payload),
                              source=message.source, source_port=message.source_port,
                              schema=message.info["schema"])
                for index in range(3):
                    with self.rpc.receive(NS) as request:
                        assert request.payload == PAYLOAD
                        if index == 0:
                            request.reply(RESPONSE)
                        elif index == 1:
                            request.reply_error("external application failure")
                        # Last unanswered release must finalize NoReply remotely.
            event("done", role=self.role)
        context.wait_stop(NS)

    def stop(self, context, reason):
        self.on_lane()
        assert context.is_stopping
        self.stop_count += 1
        assert self.stop_count == 1
        for endpoint in self.endpoints:
            endpoint.close()
        event("stopped", role=self.role, reason=int(reason))

    def destroy(self):
        self.on_lane()
        assert self.stop_count == 1
        event("destroyed", role=self.role)


def main():
    library = r.Library(os.environ["REINY_COMPONENT_LIBRARY"])
    registry = library.registry()
    errors = registry.register("foreign", "1", lambda context: Foreign(context, library))
    registry.seal()
    # The actual native entry consumes its private inherited framed Bootstrap.
    outcome = registry.process_host_run(NS, 1_000_000_000, 2_000_000_000, 8)
    if outcome["kind"] != "unresponsive":
        registry.close()
        library.collect_factories()
        assert errors == []
    event("outcome", outcome=outcome)
    # Contract: exit after entry returns, including UNRESPONSIVE. Do not run
    # Python interpreter finalization concurrently with a blocked native lane.
    os._exit(0)


if __name__ == "__main__":
    try:
        main()
    except BaseException:
        traceback.print_exc()
        os._exit(2)
