"""Native integration: REINY_COMPONENT_LIBRARY=/absolute/libreiny_ffi.so."""

import threading

import reiny_component as r


NS = 5_000_000_000
PAYLOAD = b"\x0a\x03\x00\xff\x00"
RESPONSE = b"\x08\x07"


def bindings(name, inputs=None, outputs=None):
    return {"version": 3, "namespace": name, "endpoint_namespace": name,
            "inputs": inputs or {}, "outputs": outputs or {}}


def graph(instances, stop_ns=NS):
    return {"version": 1, "domain": "python-component",
            "startup_timeout_ns": NS, "stop_timeout_ns": stop_ns,
            "instances": [{"factory_id": name, "bindings": bound}
                          for name, bound in instances]}


class State:
    def __init__(self):
        self.entered = threading.Event()
        self.gate = threading.Event()
        self.done = threading.Event()
        self.destroyed = threading.Event()
        self.stop_count = 0
        self.lane = None
        self.borrowed = None
        self.context = None
        self.errors = []

    def check_lane(self):
        assert self.lane == threading.get_ident()


class Idle(r.Component):
    def __init__(self, context, state):
        self.state = state
        state.lane = threading.get_ident()
        state.borrowed = context
        state.context = context.clone()
        assert context.info["id"]
        assert not context.is_stopping

    def prepare(self, context):
        self.state.check_lane()

    def run(self, context):
        self.state.check_lane()
        self.state.entered.set()
        context.wait_stop(NS)

    def stop(self, context, reason):
        self.state.check_lane()
        assert context.is_stopping
        assert isinstance(reason, r.StopReason)
        self.state.stop_count += 1
        assert self.state.stop_count == 1

    def destroy(self):
        self.state.check_lane()
        assert self.state.stop_count == 1
        self.state.destroyed.set()


class Ports(Idle):
    def __init__(self, context, state, library, sender):
        super().__init__(context, state)
        self.library, self.sender = library, sender
        self.endpoints = []

    def prepare(self, context):
        super().prepare(context)
        with self.library.schema("fixture.Packet", 42) as packet, \
                self.library.schema("fixture.Response", 99) as response:
            try:
                context.output("undeclared", packet)
            except r.ReinyError:
                pass
            else:
                raise AssertionError("undeclared port accepted")
            if self.sender:
                self.stream = context.output("outgoing", packet)
                self.rpc = context.rpc_client("echo", packet, response)
                try:
                    self.stream.send(PAYLOAD)
                except r.ReinyError as error:
                    assert error.status == r.Status.INVALID_STATE
                else:
                    raise AssertionError("send admitted during prepare")
            else:
                self.stream = context.input("incoming", packet)
                self.rpc = context.rpc_server("echo", packet, response)
                self.presence = context.watch_input_ready("incoming")
                self.endpoints.append(self.presence)
            self.endpoints.extend([self.stream, self.rpc])

    def run(self, context):
        self.state.check_lane()
        if self.sender:
            for payload in (PAYLOAD, b""):
                self.stream.send(payload)
            for expected in (RESPONSE, r.RemoteError, r.NoReplyError):
                with self.rpc.call_start(PAYLOAD, NS) as operation:
                    if isinstance(expected, bytes):
                        assert operation.wait(NS) == expected
                        with operation.clone() as observer:
                            assert observer.wait(NS) == expected
                    else:
                        try:
                            operation.wait(NS)
                        except expected:
                            pass
                        else:
                            raise AssertionError("incorrect RPC outcome")
            self.state.done.set()
        else:
            assert self.presence.next(NS)["joined"]
            for payload in (PAYLOAD, b""):
                with self.stream.receive(NS) as message:
                    assert message.payload == payload
                    assert message.source == "producer"
                    assert message.source_port == "outgoing"
                    assert message.info["schema"] == 42
            assert self.stream.stats(NS)["received"] == 2
            for index in range(3):
                with self.rpc.receive(NS) as request:
                    assert request.payload == PAYLOAD
                    if index == 0:
                        request.reply(RESPONSE)
                    elif index == 1:
                        request.reply_error("application failure")
                    if index < 2:
                        try:
                            request.reply(b"again")
                        except r.AlreadyCompletedError:
                            pass
                        else:
                            raise AssertionError("second reply accepted")
            self.state.done.set()
        context.wait_stop(NS)

    def stop(self, context, reason):
        super().stop(context, reason)
        if self.sender:
            try:
                self.stream.send(b"after stop")
            except r.ClosedError:
                pass
            else:
                raise AssertionError("send accepted after stop")
        for endpoint in self.endpoints:
            endpoint.close()
