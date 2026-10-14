"""Exercise the generated UniFFI API with a real in-process reiny bus."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from reiny_ffi import FfiError, LocalBus, Session


def main() -> None:
    """Verify bytes, provenance, routing, readiness, and shutdown."""
    fixtures = Path(__file__).resolve().parents[1] / "fixtures"
    bus = LocalBus()
    sender = bus.connect("sender", "smoke", str(fixtures / "sender.json"))
    receiver = bus.connect("receiver", "smoke", str(fixtures / "receiver.json"))
    subscription = receiver.subscriber("in", "Binary", 42)
    publisher = sender.publisher("out", "Binary", 42)
    sender.ready()
    receiver.ready()
    payload = bytes([0x0A, 0x04, 0, 1, 255, 0])  # Ping { bytes data = 1; }

    publisher.send(payload)
    message = subscription.receive(1000)
    assert message is not None
    assert message.payload == payload
    assert message.source == "sender"
    assert message.source_port == "out"
    assert message.schema == 42

    try:
        sender.publisher("out", "invalid/type", 42)
    except FfiError.Failure:
        pass
    else:
        raise AssertionError("invalid type name was accepted")

    receiver.shutdown()
    assert subscription.receive(1000) is None

    # Exercise the generated network constructor and config-file contract too.
    with TemporaryDirectory() as directory:
        config = Path(directory) / "zenoh.json"
        with config.open("w", encoding="utf-8") as output:
            json.dump(
                {
                    "mode": "peer",
                    "listen": {"endpoints": ["tcp/127.0.0.1:0"]},
                    "scouting": {"multicast": {"enabled": False}},
                },
                output,
            )
        network = Session.open("network", "smoke", str(config), str(fixtures / "network.json"))
        network_subscriber = network.subscriber("in", "Binary", 42)
        network_publisher = network.publisher("out", "Binary", 42)
        network.ready()
        network_publisher.send(payload)
        network_message = network_subscriber.receive(1000)
        assert network_message is not None
        assert network_message.payload == payload
        assert network_message.source == "network"
        assert network_message.source_port == "out"
        network.shutdown()
    print("Python UniFFI smoke passed")


if __name__ == "__main__":
    main()
