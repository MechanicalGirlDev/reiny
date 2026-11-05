"""Native integration: REINY_COMPONENT_LIBRARY=/absolute/libreiny_ffi.so."""

import copy
import ctypes
import gc
import os
import threading
import unittest
import weakref

import reiny_component as r


NS = 5_000_000_000
PAYLOAD = b"\x0a\x03\x00\xff\x00"
RESPONSE = b"\x08\x07"


from component_service_support import bindings, graph, State, Idle, Ports


class NativeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Missing native artifacts are a failure, not a silently skipped matrix.
        cls.library = r.Library(os.environ["REINY_COMPONENT_LIBRARY"])

    def tearDown(self):
        self.library.collect_factories()

    def await_event(self, event):
        self.assertTrue(event.wait(10), "native callback did not signal")

    def test_real_stream_rpc_lifecycle(self):
        producer, consumer = State(), State()
        contract = {"type": "fixture.Packet"}
        rpc = {"type": "fixture.Packet", "kind": "rpc", "response": "fixture.Response"}
        source = lambda name, port: [{"namespace": name, "port": port}]
        bound = [
            ("producer", bindings("producer",
                {"echo": {"contract": rpc, "sources": source("consumer", "echo")}},
                {"outgoing": {"contract": contract}})),
            ("consumer", bindings("consumer",
                {"incoming": {"contract": contract, "sources": source("producer", "outgoing")}},
                {"echo": {"contract": rpc}})),
        ]
        with self.library.registry() as registry, self.library.graph(graph(bound)) as spec:
            diagnostics = [
                registry.register("producer", "v1", lambda ctx: Ports(ctx, producer, self.library, True)),
                registry.register("consumer", "v1", lambda ctx: Ports(ctx, consumer, self.library, False)),
            ]
            registry.seal()
            with registry.launch_local(spec) as host:
                registry.close()  # Callback code must remain rooted by native ownership.
                gc.collect()
                host.wait_running(NS)
                self.await_event(producer.done)
                self.await_event(consumer.done)
                self.assertEqual(host.phase, r.Phase.RUNNING)
                self.assertEqual(host.snapshot["phase"], r.Phase.RUNNING)
                with host.subscribe() as updates:
                    self.assertEqual(updates.next(NS)["phase"], r.Phase.RUNNING)
                    host.request_stop()
                    self.assertGreaterEqual(updates.next(NS)["phase"], r.Phase.STOPPING)
                    outcome = host.wait_terminal(NS)
                    self.assertEqual(outcome["phase"], r.Phase.STOPPED)
                    self.assertEqual(outcome["unjoined"], [])
                    host.wait_joined(NS)
                for state in (producer, consumer):
                    self.assertTrue(state.destroyed.is_set())
                    self.assertTrue(state.context.is_stopping)
                    with self.assertRaises(r.ReinyError):
                        state.borrowed.info
                    state.context.close()
                self.assertEqual(diagnostics, [[], []])

    def test_unresponsive_stop_then_real_join(self):
        state = State()

        class Blocked(Idle):
            def run(self, context):
                state.check_lane()
                state.entered.set()
                assert state.gate.wait(10)
                assert context.is_stopping

        with self.library.registry() as registry, self.library.graph(
                graph([("blocked", bindings("blocked"))], stop_ns=20_000_000)) as spec:
            errors = registry.register("blocked", "v1", lambda ctx: Blocked(ctx, state))
            registry.seal()
            with registry.launch_local(spec) as host:
                host.wait_running(NS)
                self.await_event(state.entered)
                registry.close()
                host.request_stop()
                try:
                    self.assertEqual(host.wait_terminal(NS)["phase"], r.Phase.UNRESPONSIVE)
                    self.assertFalse(state.destroyed.is_set())
                    self.assertEqual(state.stop_count, 0)
                    with r._registration_lock:
                        registration = next(value for value in r._registrations.values()
                                            if state in [getattr(instance, "state", None)
                                                         for instance in value.instances.values()])
                    token = registration.token
                    registration_ref = weakref.ref(registration)
                    factory_ref = weakref.ref(registration.factory)
                    callback_ref = weakref.ref(registration.callbacks[0])
                    dll_ref = weakref.ref(registration.library._dll)
                    self.assertFalse(registration.released)
                    del registration
                    self.library.collect_factories()
                    gc.collect()
                    with r._registration_lock:
                        self.assertIs(r._registrations[token], registration_ref())
                    self.assertIsNotNone(factory_ref())
                    self.assertIsNotNone(callback_ref())
                    self.assertIsNotNone(dll_ref())
                    with self.assertRaises(r.DeadlineError):
                        host.wait_joined(1_000_000)
                finally:
                    state.gate.set()
                host.wait_joined(NS)
                self.assertTrue(state.destroyed.is_set())
                self.assertEqual(state.stop_count, 1)
                self.assertEqual(errors, [])
                state.context.close()
        self.library.collect_factories()
        gc.collect()
        self.assertIsNone(registration_ref())
        self.assertIsNone(factory_ref())
        self.assertIsNone(callback_ref())

    def test_callback_exception_and_expired_borrow(self):
        state = State()

        class Failing(Idle):
            def prepare(self, context):
                raise KeyboardInterrupt("callback exception boundary")

        with self.library.registry() as registry, self.library.graph(
                graph([("failing", bindings("failing"))])) as spec:
            errors = registry.register("failing", "v1", lambda ctx: Failing(ctx, state))
            registry.seal()
            with registry.launch_local(spec) as host:
                self.assertEqual(host.wait_terminal(NS)["phase"], r.Phase.FAILED)
                host.wait_joined(NS)
                self.assertTrue(state.destroyed.is_set())
                self.assertEqual(errors, [])
                state.context.close()

    def test_native_release_token_ownership(self):
        registration = r._Registration(self.library, lambda ctx: r.Component())
        token = registration.release_token
        self.assertFalse(registration.table.release_factory)
        self.assertFalse(token.released)
        with self.library.registry() as registry:
            self.library._call("register_notified", registry._id(), r._text("token"),
                               r._text("v1"), ctypes.byref(registration.table), token._id())
            self.assertFalse(token.released)
            with self.assertRaises(r.ReinyError):
                self.library._call("register_notified", registry._id(), r._text("another"),
                                   r._text("v1"), ctypes.byref(registration.table), token._id())
            self.assertFalse(token.released)  # Failed registration did not consume token.
        self.assertTrue(token.released)
        token.close()
        failed = r._Registration(self.library, lambda ctx: r.Component())
        with self.library.registry() as sealed:
            sealed.seal()
            with self.assertRaises(r.ReinyError):
                self.library._call("register_notified", sealed._id(), r._text("rejected"),
                                   r._text("v1"), ctypes.byref(failed.table),
                                   failed.release_token._id())
            self.assertFalse(failed.release_token.released)
        failed.release_token.close()

    def test_owned_buffers_validation_and_registration_failure(self):
        with self.library.registry() as registry:
            registry.register("idle", "v1", lambda ctx: r.Component())
            with self.assertRaises(r.ReinyError) as duplicate:
                registry.register("idle", "v1", lambda ctx: r.Component())
            self.assertEqual(duplicate.exception.status, r.Status.INVALID_STATE)
            with self.assertRaises(ValueError):
                registry.register("nul\0factory", "v1", lambda ctx: r.Component())
            with self.assertRaises(r.ReinyError):
                self.library.graph(b'{"version":1,"version":1}')
            with self.assertRaises(r.SchemaError):
                self.library.schema("fixture.Packet", 42, fingerprint_version=1)
            with self.library.schema("fixture.Packet", 0) as schema:
                with self.assertRaises(TypeError):
                    copy.copy(schema)
                with schema.clone():
                    pass
                with self.assertRaises(r.ReinyError) as wrong:
                    self.library._call("registry_seal", schema._id())
                self.assertEqual(wrong.exception.status, r.Status.WRONG_KIND)
            with self.assertRaises(r.ReinyError):
                schema.clone()
            for timeout in (0, -1, 1 << 64, True, 0.5):
                with self.assertRaises(ValueError):
                    r._uint(timeout, positive=True)
            # Verify exact table layout independently of native callback execution.
            self.assertEqual(ctypes.sizeof(r._Factory), 16 + 7 * ctypes.sizeof(ctypes.c_void_p))


if __name__ == "__main__":
    unittest.main()
