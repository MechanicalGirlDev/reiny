"""Explicit resources and graph fixtures for Local-step acceptance."""

from contextlib import ExitStack
import gc
import threading
import weakref

import reiny_component as base

from component_step_node import (
    NS, INITIAL, DT, encoded, NodeState, Node,
)


class Fixture:
    def __init__(self, test, *, mode_a="normal", mode_b="normal",
                 destroy_blocked=False, launch=True, stop_ns=NS):
        self.test, self.library, self.api = test, test.library, test.api
        self.resources = ExitStack()
        self.host = self.scheduler = None
        self.closed = False
        self.states = [NodeState("a", mode_a, destroy_blocked), NodeState("b", mode_b)]
        self.diagnostics = []
        self.registration_refs, self.callback_refs, self.tokens = [], [], []
        test.addCleanup(self.close)
        self.source_schema = self.resources.enter_context(self.library.schema("test.Number", 42))
        # Application codecs explicitly retain their own schema claim.
        self.schema = self.resources.enter_context(self.source_schema.clone())
        self.wrong = self.resources.enter_context(self.library.schema("test.Other", 42))
        self.registry = self.resources.enter_context(self.library.registry())
        self.schemas = [
            self.resources.enter_context(self.api.schema(
                {"x": self.source_schema, "prev": self.source_schema}, {"out": self.source_schema})),
            self.resources.enter_context(self.api.schema(
                {"x": self.source_schema}, {"out": self.source_schema})),
        ]
        for state, schema in zip(self.states, self.schemas):
            with base._registration_lock:
                before = set(base._registrations)
            self.diagnostics.append(self.api.register(
                self.registry, state.role, "1", schema,
                lambda context, state=state: Node(context, state, self.api, self.schema, self.wrong)))
            with base._registration_lock:
                token, = set(base._registrations) - before
                registration = base._registrations[token]
            test.assertFalse(registration.table.release_factory)
            test.assertFalse(registration.released)
            self.tokens.append(token)
            self.registration_refs.append(weakref.ref(registration))
            self.callback_refs.append(weakref.ref(registration.callbacks[2]))
            del registration
        self.registry.seal()
        self.service_graph = self.resources.enter_context(self.library.graph({
            "version": 1, "domain": "python-managed-step",
            "startup_timeout_ns": NS, "stop_timeout_ns": stop_ns,
            "instances": [
                {"factory_id": role, "bindings": {
                    "version": 3, "namespace": role, "endpoint_namespace": role,
                    "inputs": {}, "outputs": {},
                }} for role in ("a", "b")
            ],
        }))
        with self.api.value(self.source_schema, encoded(10)) as seed:
            self.graph = self.resources.enter_context(self.api.graph(
                {"a": self.schemas[0], "b": self.schemas[1]}, [
                    {"instance": "a", "port": "x", "kind": "external"},
                    {"instance": "a", "port": "prev", "kind": "previous_tick",
                     "source": {"instance": "b", "port": "out"}, "initial": seed},
                    {"instance": "b", "port": "x", "kind": "same_tick",
                     "source": {"instance": "a", "port": "out"}},
                ]))
        if launch:
            self.launch()

    def launch(self):
        self.host, self.scheduler = self.api.launch_local(
            self.registry, self.service_graph, self.graph, INITIAL)
        self.host.wait_running(NS)
        self.test.assertNotEqual(self.states[0].lane, threading.get_ident())
        self.test.assertNotEqual(self.states[0].lane, self.states[1].lane)
        # Every native import/registration owns independent immutable copies.
        self.registry.close()
        self.source_schema.close()
        self.service_graph.close()
        self.graph.close()
        for schema in self.schemas:
            schema.close()

    def request(self, *, wrong=False, dt=DT):
        with self.api.value(self.wrong if wrong else self.schema, encoded(1)) as value:
            return self.api.request(dt, [("a", "x", value)])

    def assert_a_pinned(self):
        self.library.collect_factories()
        gc.collect()
        with base._registration_lock:
            self.test.assertIn(self.tokens[0], base._registrations)
        self.test.assertIsNotNone(self.callback_refs[0]())
        self.test.assertFalse(self.registration_refs[0]().released)

    def close(self):
        if self.closed:
            return
        self.closed = True
        for state in self.states:
            state.gate.set()
            state.destroy_gate.set()
        if self.host is not None:
            self.host.request_stop()
            self.host.wait_joined(NS)
            for state in self.states:
                self.test.assertTrue(state.stopped.is_set())
                self.test.assertTrue(state.destroyed.is_set())
                self.test.assertEqual((state.stop_count, state.destroy_count), (1, 1))
            self.host.close()
            self.scheduler.close()
        for state in self.states:
            for inputs in state.retained_inputs:
                inputs.close()
        self.resources.close()
        self.library.collect_factories()
        gc.collect()
        self.test.assertEqual(self.diagnostics, [[], []])
        for reference in self.registration_refs + self.callback_refs:
            self.test.assertIsNone(reference(), "callback root survived confirmed join/release")
