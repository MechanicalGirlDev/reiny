"""Linux real Python ProcessHost acceptance; no SDK/deployment-owner substitute."""

import hashlib
import os
from pathlib import Path
import sys
import unittest

from component_process_protocol import (
    BUDGET, LIBRARY_SHA, HEADER_SHA, HERE, COMPATIBILITY, FACTORY, frame, exact,
    binding, contract, expected_report,
)
from component_process_support import OwnedHost


class ProcessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not sys.platform.startswith("linux"):
            raise RuntimeError("real process acceptance requires Linux /proc evidence")
        cls.library = Path(os.environ["REINY_COMPONENT_LIBRARY"]).resolve(strict=True)
        if hashlib.sha256(cls.library.read_bytes()).hexdigest() != LIBRARY_SHA:
            raise RuntimeError("process acceptance requires the exact frozen native library")
        header = HERE.parents[1] / "crates/reiny-ffi/include/reiny_component.h"
        if hashlib.sha256(header.read_bytes()).hexdigest() != HEADER_SHA:
            raise RuntimeError("process acceptance requires the exact frozen native header")

    def test_two_python_processes_exchange_named_stream_rpc_and_reap(self):
        source = lambda namespace, port: [{"namespace": namespace, "port": port}]
        consumer_bound = binding("consumer",
            {"incoming": {"contract": contract(), "sources": source("producer", "outgoing")}},
            {"echo": {"contract": contract(True)}})
        producer_bound = binding("producer",
            {"echo": {"contract": contract(True), "sources": source("consumer", "echo")}},
            {"outgoing": {"contract": contract()}})
        consumer = OwnedHost(self, "consumer-host")
        consumer.prepare(consumer_bound, listen=["tcp/127.0.0.1:0"])
        consumer.event("created")
        endpoint = consumer.event("data_listener")["endpoint"]
        consumer.event("prepared")
        producer = OwnedHost(self, "producer-host")
        self.assertNotEqual(producer.pid, consumer.pid)
        producer.prepare(producer_bound, connect=[endpoint])
        producer.event("created")
        producer.event("prepared")
        # Exact closed fixture edges were checked in both reports before commit.
        # No Run is sent until EVERY real host acknowledges Commit.
        consumer.commit()
        producer.commit()
        consumer.run()
        producer.run()
        for payload in ([10, 3, 0, 255, 0], []):
            delivered = consumer.event("delivered")
            self.assertEqual(delivered["payload"], payload)
            self.assertEqual((delivered["source"], delivered["source_port"], delivered["schema"]),
                             ("producer", "outgoing", 42))
        self.assertEqual(producer.event("rpc_success")["payload"], [8, 7])
        self.assertEqual(producer.event("rpc_error")["status"], -7)
        self.assertEqual(producer.event("rpc_error")["status"], -8)
        producer.event("done")
        consumer.event("done")
        for host in (consumer, producer):
            host.stop()
        for host in (consumer, producer):
            self.assertEqual(host.terminal(), {"kind": "stopped"})
            host.reap(["stopped", "destroyed"])

    def test_python_process_rejects_run_before_commit_and_reaps(self):
        host = OwnedHost(self, "premature-host")
        host.prepare(binding("foreign"))
        host.event("created")
        host.event("prepared")
        host.send("run", {})
        outcome = host.terminal()
        self.assertEqual(outcome["kind"], "failed")
        self.assertEqual(outcome["primary"]["code"], "transition")
        host.reap(["stopped", "destroyed"])
        self.assertNotIn("running", [value["event"] for value in host.events])

    def test_python_prepare_exception_propagates_and_reaps(self):
        host = OwnedHost(self, "failure-host", "prepare_failure")
        host.prepare(binding("foreign"), succeeds=False)
        host.event("created")
        host.event("prepare_failed")
        outcome = host.terminal()
        self.assertEqual(outcome["kind"], "failed")
        self.assertEqual(outcome["primary"]["code"], "prepare")
        self.assertEqual(outcome["primary"]["instance"], "foreign")
        host.reap(["stopped", "destroyed"])
        self.assertNotIn("running", [value["event"] for value in host.events])

    def test_unresponsive_python_callback_exits_without_false_cleanup_and_reaps(self):
        host = OwnedHost(self, "unresponsive-host", "unresponsive")
        host.prepare(binding("foreign"))
        host.event("created")
        host.event("prepared")
        host.commit()
        host.run()
        host.stop()
        outcome = host.terminal()
        self.assertEqual(outcome["kind"], "unresponsive")
        self.assertEqual(outcome["instances"], ["foreign"])
        host.reap([])
        self.assertNotIn("stopped", [value["event"] for value in host.events])
        self.assertNotIn("destroyed", [value["event"] for value in host.events])


if __name__ == "__main__":
    unittest.main()
