"""Exercise the generated UniFFI API with a real in-process reiny bus."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from reiny_ffi import FfiError, LocalBus, Session


def main() -> None:
    """Verify bytes, provenance, filtering, presence, and shutdown."""
    bus = LocalBus()
    sender = bus.connect("python-sender", "smoke")
    receiver = bus.connect("python-receiver", "smoke")
    subscription = receiver.subscriber("Ping", "python-sender")
    publisher = sender.publisher("Ping", (1 << 64) - 1)
    payload = bytes([0x0A, 0x04, 0, 1, 255, 0])  # Ping { bytes data = 1; }

    publisher.send(payload)
    message = subscription.receive(1000)
    assert message is not None
    assert message.payload == payload
    assert message.source == "python-sender"
    assert message.schema == (1 << 64) - 1
    assert receiver.publishers("Ping", 1000) == ["python-sender"]

    try:
        sender.publisher("invalid/type", None)
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
        network = Session.open("python-network", "smoke", str(config))
        network_subscriber = network.subscriber("Ping", None)
        network_publisher = network.publisher("Ping", None)
        network_publisher.send(payload)
        network_message = network_subscriber.receive(1000)
        assert network_message is not None
        assert network_message.payload == payload
        assert network_message.source == "python-network"
        network.shutdown()
    print("Python UniFFI smoke passed")


if __name__ == "__main__":
    main()
