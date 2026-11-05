"""Real managed Step assertions against REINY_STEP_LIBRARY, without frozen pins."""

import os
import json
from contextlib import ExitStack
import unittest

import reiny_component as base
from reiny_component import steps


BUDGET = 5_000_000_000
ASSERTIONS = {
    "units": "rad", "identity": "arm.left", "order": "joint.v1",
    "time": "logical", "update": "replace",
}


class Sum(steps.StepComponent):
    def __init__(self, api, schema, assertions, effects):
        self.api, self.schema = api, schema
        self.assertions, self.effects = assertions, effects

    def step(self, context, tick, inputs, outputs):
        self.effects.append(tick.index)
        with inputs.get("x") as value:
            payload = value.encoded(self.schema)
            assert payload[0] == 8
            result = payload[1] + 1
        with self.api.value(self.schema, bytes([8, result]),
                            semantics=self.assertions) as value:
            outputs.set("out", value)


class SemanticTests(unittest.TestCase):
    def setUp(self):
        self.library = base.Library(os.environ["REINY_STEP_LIBRARY"])
        self.api = steps.LocalSteps(self.library)
        self.resources = ExitStack()
        self.addCleanup(self.cleanup)
        self.host = None
        self.effects = []
        self.created = []
        self.schema = self.resources.enter_context(self.library.schema("test.Number", 42))
        self.wrong = self.resources.enter_context(self.library.schema("test.Other", 42))
        self.wrong_fingerprint = self.resources.enter_context(self.library.schema("test.Number", 43))

    def cleanup(self):
        if self.host is not None:
            self.host.request_stop()
            self.host.wait_joined(BUDGET)
        self.resources.close()
        self.library.collect_factories()

    def launch(self, assertions, output_assertions, *, graph_assertions=None):
        registry = self.resources.enter_context(self.library.registry())
        port = {"schema": self.schema, "semantics": assertions}
        schema = self.resources.enter_context(self.api.schema({"x": port}, {"out": port}))
        def create(_):
            self.created.append("node")
            return Sum(self.api, self.schema, output_assertions, self.effects)

        self.api.register(registry, "node", "1", schema, create)
        registry.seal()
        service = self.resources.enter_context(self.library.graph({
            "version": 1, "domain": "step-semantics", "startup_timeout_ns": BUDGET,
            "stop_timeout_ns": BUDGET, "instances": [{
                "factory_id": "node", "bindings": {
                    "version": 3, "namespace": "node", "endpoint_namespace": "node",
                    "inputs": {}, "outputs": {},
                },
            }],
        }))
        graph_schema = schema
        if graph_assertions is not None:
            graph_port = {"schema": self.schema, "semantics": graph_assertions}
            graph_schema = self.resources.enter_context(
                self.api.schema({"x": graph_port}, {"out": graph_port}))
        graph = self.resources.enter_context(self.api.graph(
            {"node": graph_schema}, [{"instance": "node", "port": "x", "kind": "external"}]))
        host, scheduler = self.api.launch_local(registry, service, graph)
        self.host = self.resources.enter_context(host)
        scheduler = self.resources.enter_context(scheduler)
        self.host.wait_running(BUDGET)
        registry.close()
        graph.close()
        schema.close()
        service.close()
        return scheduler

    def test_equal_assertions_owned_values_and_wrong_schema(self):
        scheduler = self.launch(ASSERTIONS, ASSERTIONS)
        assertions = dict(ASSERTIONS)
        payload = bytearray([8, 11])
        with self.api.value(self.schema, bytes(payload), semantics=assertions) as original:
            value = original.clone()
        with value:
            assertions.clear()
            payload[:] = b"\0\0"
            self.assertEqual(value.semantics, ASSERTIONS)
            for schema in [self.wrong, self.wrong_fingerprint]:
                with self.assertRaises(base.SchemaError):
                    value.encoded(schema)
            request = self.api.request(steps.Duration(nanos=1), [("node", "x", value)])
        with request:
            for index in range(2):
                with scheduler.try_step(request, BUDGET) as run, run.wait(BUDGET) as result:
                    self.assertEqual(result.kind, steps.ResultKind.COMMITTED)
                    self.assertEqual(result.tick.index, index)
                    with result.output("node", "out") as output:
                        retained = output.clone()
                with retained:
                    self.assertEqual(retained.encoded(self.schema), bytes([8, 12]))
                    self.assertEqual(retained.semantics, ASSERTIONS)
        self.assertEqual(self.effects, [0, 1])

    def test_external_mismatch_rejects_before_effects(self):
        scheduler = self.launch(ASSERTIONS, ASSERTIONS)
        for assertions in [None, {}, {"units": "deg"}, {"units": "rad"}]:
            with self.subTest(assertions=assertions):
                with self.api.value(self.schema, b"\x08\x01", semantics=assertions) as value:
                    with self.api.request(steps.Duration(nanos=1), [("node", "x", value)]) as request:
                        with self.assertRaises(steps.InvalidRequestError):
                            scheduler.try_step(request, BUDGET)
                self.assertEqual(self.effects, [])
                self.assertEqual(scheduler.clock.next_index, 0)

    def test_output_mismatch_faults_without_commit(self):
        scheduler = self.launch(ASSERTIONS, {})
        with self.api.value(self.schema, b"\x08\x01", semantics=ASSERTIONS) as value:
            with self.api.request(steps.Duration(nanos=1), [("node", "x", value)]) as request:
                with scheduler.try_step(request, BUDGET) as run, run.wait(BUDGET) as result:
                    self.assertEqual(result.kind, steps.ResultKind.FAILED_AFTER_DISPATCH)
                    self.assertEqual(result.failure.kind, steps.FailureKind.INVALID_OUTPUTS)
        self.assertEqual(self.effects, [0])
        self.assertEqual(scheduler.clock.next_index, 0)

    def test_graph_cannot_erase_registered_assertions_before_construction(self):
        with self.assertRaises(base.ReinyError):
            self.launch(ASSERTIONS, ASSERTIONS, graph_assertions={})
        self.assertEqual(self.created, [])
        self.assertEqual(self.effects, [])

    def test_asserted_input_cannot_enter_an_unspecified_port(self):
        scheduler = self.launch({}, None)
        with self.api.value(self.schema, b"\x08\x01", semantics=ASSERTIONS) as value:
            with self.api.request(steps.Duration(nanos=1), [("node", "x", value)]) as request:
                with self.assertRaises(steps.InvalidRequestError):
                    scheduler.try_step(request, BUDGET)
        self.assertEqual(self.effects, [])

    def test_empty_assertions_keep_legacy_value_creation_working(self):
        scheduler = self.launch({}, None)
        with self.api.value(self.schema, b"\x08\x01") as value:
            with self.api.request(steps.Duration(nanos=1), [("node", "x", value)]) as request:
                with scheduler.try_step(request, BUDGET) as run, run.wait(BUDGET) as result:
                    with result.output("node", "out") as output:
                        self.assertEqual(output.encoded(self.schema), b"\x08\x02")
                        self.assertEqual(output.semantics, {})

    def test_blank_and_duplicate_assertions_rejected(self):
        for assertions in [{"": "rad"}, {"units": " \t"}]:
            with self.assertRaises(base.ReinyError):
                self.api.value(self.schema, b"\x08\x01", semantics=assertions)
            with self.assertRaises(base.ReinyError):
                self.api.schema({"x": {"schema": self.schema, "semantics": assertions}}, {})
        document = json.dumps({
            "version": 1, "inputs": {
                "x": {"schema": self.schema._id(), "semantics": {"units": "rad"}},
            }, "outputs": {},
        }).replace('"units": "rad"', '"units": "rad", "units": "deg"').encode()
        with self.assertRaises(base.ReinyError):
            self.api.import_schema(document)


if __name__ == "__main__":
    unittest.main()
