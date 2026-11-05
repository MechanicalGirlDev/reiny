"""Test ownership of authenticated real native Python child hosts."""

import hashlib
import json
import os
from pathlib import Path
import queue
import secrets
import socket
import struct
import subprocess
import sys
import threading

from component_process_protocol import (
    BUDGET, LIBRARY_SHA, HERE, COMPATIBILITY, FACTORY, frame, exact, expected_report,
)


class OwnedHost:
    """A test owner of one actual child and one authenticated control socket."""

    def __init__(self, test, name, mode="normal"):
        self.test, self.name = test, name
        self.identity = {"owner_generation": 1, "graph_generation": 2, "host_id": name,
                         "host_generation": 3, "graph_digest": "python-process-graph",
                         "bundle_digest": "frozen-python-component"}
        self.nonce = list(secrets.token_bytes(32))
        self.sent, self.received = 0, -1
        self.events, self.failures, self.stderr, self.stdout = [], [], [], []
        self.lines = queue.Queue()
        self.exited = threading.Event()
        self.connection = None
        self.child = None
        self.outcome = None
        listener = socket.socket()
        listener.settimeout(BUDGET)
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        test.addCleanup(self.cleanup)
        try:
            environment = os.environ.copy()
            environment.update(PYTHONPATH=str(HERE), PYTHONDONTWRITEBYTECODE="1",
                               REINY_PROCESS_MODE=mode)
            self.child = subprocess.Popen(
                [sys.executable, "-u", str(HERE / "component_process_entry.py")],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=environment,
            )
            self.pid = self.child.pid
            test.assertNotEqual(self.pid, os.getpid())
            self.stdout_thread = threading.Thread(target=self.read_stdout)
            self.stderr_thread = threading.Thread(target=self.read_stderr)
            self.wait_thread = threading.Thread(target=self.wait_child)
            self.stdout_thread.start()
            self.stderr_thread.start()
            self.wait_thread.start()
            address = listener.getsockname()
            self.child.stdin.write(frame({
                "address": f"{address[0]}:{address[1]}",
                "identity": self.identity, "nonce": self.nonce,
            }))
            self.child.stdin.close()  # The inherited bootstrap has exactly one owner.
            self.connection, _ = listener.accept()
            self.connection.settimeout(BUDGET)
            proof = self.receive()
            test.assertEqual(proof["kind"], "authenticate")
            test.assertEqual(proof["payload"]["nonce"], self.nonce)
            test.assertEqual(proof["payload"]["compatibility"], COMPATIBILITY)
            test.assertEqual(proof["payload"]["registrations"], [FACTORY])
            self.verify_library()  # Before any factory can fail or exit.
            self.send("authenticated", {"compatibility": COMPATIBILITY,
                                        "required_factories": [FACTORY]})
        finally:
            listener.close()

    def read_stdout(self):
        try:
            for line in self.child.stdout:
                self.stdout.append(line.decode(errors="replace"))
                self.lines.put(json.loads(line))
        except BaseException as error:
            self.lines.put(error)
        finally:
            self.lines.put(None)

    def read_stderr(self):
        for line in self.child.stderr:
            self.stderr.append(line.decode(errors="replace"))

    def wait_child(self):
        self.child.wait()  # Blocking waitpid, not Popen's timed polling path.
        self.exited.set()

    def event(self, expected):
        value = self.lines.get(timeout=BUDGET)
        if isinstance(value, BaseException):
            raise value
        self.test.assertIsNotNone(value, f"child EOF before {expected}: {''.join(self.stderr)}")
        self.events.append(value)
        self.test.assertEqual(value["pid"], self.pid)
        self.test.assertEqual(value["event"], expected)
        return value

    def send(self, kind, payload):
        self.connection.sendall(frame({
            "version": 1, "session": self.identity, "sequence": self.sent,
            "body": {"kind": kind, "payload": payload},
        }))
        self.sent += 1

    def receive(self):
        length, = struct.unpack(">I", exact(self.connection, 4))
        self.test.assertTrue(0 < length <= 8 * 1024 * 1024)
        envelope = json.loads(exact(self.connection, length))
        self.test.assertEqual(envelope["version"], 1)
        self.test.assertEqual(envelope["session"], self.identity)
        self.test.assertGreater(envelope["sequence"], self.received)
        if self.received == -1:
            self.test.assertEqual(envelope["sequence"], 0)
        self.received = envelope["sequence"]
        return envelope["body"]

    def prepare(self, bound, *, connect=(), listen=(), succeeds=True):
        self.send("prepare", {
            "assignments": [{"implementation": FACTORY, "bindings": bound, "config_origin": None}],
            "transport": {"kind": "zenoh", "domain": "python-real-process",
                          "connect": list(connect), "listen": list(listen), "multicast_scouting": False},
            "remaining_budget": 15000,
        })
        if succeeds:
            self.test.assertEqual(self.receive(), {"kind": "report", "payload": expected_report(bound)})
            self.test.assertEqual(self.receive(), {"kind": "prepared_complete", "payload": {}})

    def verify_library(self):
        library = str(self.test.library)
        maps = Path(f"/proc/{self.pid}/maps").read_text()
        self.test.assertTrue(any(line.endswith(" " + library) for line in maps.splitlines()),
                             f"child did not map frozen library: {library}")
        self.test.assertEqual(hashlib.sha256(self.test.library.read_bytes()).hexdigest(), LIBRARY_SHA)
        print(f"PYTHON_PROCESS_LIBRARY pid={self.pid} sha256={LIBRARY_SHA} path={library}", flush=True)

    def commit(self):
        self.send("commit", {})
        self.test.assertEqual(self.receive(), {"kind": "committed", "payload": {}})

    def run(self):
        self.send("run", {})
        self.test.assertEqual(self.receive(), {"kind": "running", "payload": {}})
        self.event("running")

    def stop(self):
        self.send("stop", {"reason": "requested", "remaining_budget": 300})

    def terminal(self):
        while True:
            message = self.receive()
            if message["kind"] == "failure":
                self.failures.append(message["payload"])
            elif message["kind"] == "outcome":
                self.outcome = message["payload"]
                return self.outcome
            else:
                raise AssertionError(f"unexpected terminal native message: {message}")

    def reap(self, callback_events):
        for name in callback_events:
            self.event(name)
        copied = self.event("outcome")["outcome"]
        self.test.assertEqual(copied, self.outcome)
        self.test.assertTrue(self.exited.wait(BUDGET), "native child did not exit")
        self.test.assertEqual(self.child.returncode, 0, "".join(self.stderr))
        self.wait_thread.join(timeout=BUDGET)
        self.stdout_thread.join(timeout=BUDGET)
        self.stderr_thread.join(timeout=BUDGET)
        self.test.assertFalse(self.stdout_thread.is_alive())
        self.test.assertFalse(self.stderr_thread.is_alive())
        self.test.assertIsNone(self.lines.get_nowait())
        self.test.assertEqual(self.stderr, [])
        self.test.assertFalse(Path(f"/proc/{self.pid}").exists())
        with self.test.assertRaises(ChildProcessError):
            os.waitpid(self.pid, os.WNOHANG)  # Popen already reaped this exact owned child.
        print(f"PYTHON_PROCESS_REAPED pid={self.pid} outcome={copied['kind']}", flush=True)

    def cleanup(self):
        if self.connection is not None:
            self.connection.close()
        if self.child is None:
            return
        if not self.exited.is_set():
            self.child.kill()
            self.test.assertTrue(self.exited.wait(BUDGET), "owned child was not reaped after kill")
        for thread in (self.stdout_thread, self.stderr_thread, self.wait_thread):
            thread.join(timeout=BUDGET)
        for stream in (self.child.stdin, self.child.stdout, self.child.stderr):
            if stream is not None:
                stream.close()
        # Failure transcripts are retained in test output, without bootstrap secrets.
        print(json.dumps({"host": self.name, "pid": self.pid,
                          "events": self.events, "failures": self.failures,
                          "outcome": self.outcome, "stdout": self.stdout,
                          "stderr": self.stderr}), flush=True)
