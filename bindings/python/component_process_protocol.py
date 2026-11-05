"""Native process-host framing and exact fixture reports."""

import json
from pathlib import Path
import struct

BUDGET = 20
LIBRARY_SHA = "8e8341ada1f5b8399d5413ba42074ae48daffc24ff6bf028d2fcdce157816b6c"
HEADER_SHA = "978db314fea91fe62800565890d19bad315a68e3440c4d08c0ce9522752a75ed"
HERE = Path(__file__).resolve().parent
COMPATIBILITY = {"reiny_release": "0.11.0", "bindings_version": 3,
                 "report_version": 3, "fingerprint_version": 2, "named_key_epoch": 3}
FACTORY = {"factory": "foreign", "revision": "1"}




def frame(value):
    body = json.dumps(value, separators=(",", ":")).encode()
    assert 0 < len(body) <= 8 * 1024 * 1024
    return struct.pack(">I", len(body)) + body


def exact(connection, length):
    result = bytearray()
    while len(result) < length:
        part = connection.recv(length - len(result))
        if not part:
            raise EOFError("native process control closed")
        result.extend(part)
    return bytes(result)


def binding(name, inputs=None, outputs=None):
    return {"version": 3, "namespace": name, "endpoint_namespace": name,
            "inputs": inputs or {}, "outputs": outputs or {}}


def contract(rpc=False):
    result = {"type": "fixture.Packet", "kind": "rpc" if rpc else "stream",
              "qos": "default", "retention": "volatile", "replay": "live", "buffer": "fifo"}
    if rpc:
        result["response"] = "fixture.Response"
    return result


def expected_report(bound):
    """Exact fixture assertion, not a public replacement for validate_graph."""
    result = {"version": 3, "fingerprint_version": 2, "namespace": bound["namespace"],
              "endpoint_namespace": bound["endpoint_namespace"], "inputs": {}, "outputs": {}}
    for side in ("inputs", "outputs"):
        for name, port in bound[side].items():
            endpoint = ({"direction": "input", "sources": port["sources"]}
                        if side == "inputs" else
                        {"direction": "output", "address": {"namespace": bound["namespace"], "port": name}})
            result[side][name] = {
                "endpoint": endpoint, "type": port["contract"]["type"], "schema": 42,
                "contract": port["contract"],
                "response": ({"type": "fixture.Response", "schema": 99}
                             if port["contract"]["kind"] == "rpc" else None),
            }
    return result
