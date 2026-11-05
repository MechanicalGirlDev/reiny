"""Local-step value acceptance cases."""

import ctypes as C

import reiny_component as base
from reiny_component import steps

from component_step_node import (
    NS, INITIAL, DT, encoded, number,
)
from component_step_support import Fixture


class ValueCases:
    def test_real_multi_tick_routes_values_retained_results_and_clock(self):
        fixture = Fixture(self, mode_a="thread_probe")
        with fixture.request(wrong=True) as invalid:
            with self.assertRaises(steps.InvalidRequestError) as mismatch:
                fixture.scheduler.try_step(invalid, NS)
            self.assertIsInstance(mismatch.exception, base.ReinyError)
            self.assertEqual(mismatch.exception.status, steps.StepStatus.INVALID_REQUEST)
        with fixture.request() as request:
            with self.assertRaises(steps.InvalidTimeoutError):
                fixture.scheduler.try_step(request, 0)
            previous_sequence = None
            for index in range(3):
                before = fixture.scheduler.clock
                with fixture.scheduler.try_step(request, NS) as run:
                    run_id = run.id
                    self.assertEqual((run_id.epoch, run_id.tick_index), (before.epoch, index))
                    if previous_sequence is not None:
                        self.assertGreater(run_id.sequence, previous_sequence)
                    previous_sequence = run_id.sequence
                    with run.wait(NS) as result:
                        self.assertEqual(result.kind, steps.ResultKind.COMMITTED)
                        self.assertIsNone(result.failure)
                        self.assertEqual(result.tick, steps.Tick(
                            before.epoch, index, steps.Duration(INITIAL.secs, index * 7),
                            DT, steps.Duration(INITIAL.secs, (index + 1) * 7)))
                        with result.output("b", "out") as value:
                            self.assertEqual(number(value, fixture.schema), 12 + index * 2)
                            with self.assertRaises(base.SchemaError):
                                value.encoded(fixture.wrong)
                        with result.clone() as retained:
                            self.assertEqual(retained.tick, result.tick)
                        info = result.info
                    with run.clone() as observer, observer.wait(NS) as retained:
                        self.assertEqual(retained.info, info)
                    self.assertEqual(run.cancel(), steps.CancelStatus.FINISHED)
                after = fixture.scheduler.clock
                self.assertEqual((after.next_index, after.time),
                                 (index + 1, steps.Duration(INITIAL.secs, (index + 1) * 7)))
        state = fixture.states[0]
        with state.retained_inputs[0].get("prev") as value:
            self.assertEqual(number(value, fixture.schema), 10)
        for borrowed in (state.borrowed_inputs, state.borrowed_outputs, state.borrowed_context):
            with self.assertRaises(base.ReinyError) as expired:
                borrowed._id()
            self.assertEqual(expired.exception.status, base.Status.INVALID_HANDLE)
        with self.api.value(fixture.schema, encoded(1)) as value:
            with self.assertRaises(base.ReinyError) as stale:
                self.api._call("output_set", state.native_sink, base._text("out"), value._id())
            self.assertEqual(stale.exception.status, base.Status.INVALID_HANDLE)
        fixture.close()

    def test_graph_validation_imports_and_duration_boundaries(self):
        fixture = Fixture(self, launch=False)
        with self.assertRaises(base.ReinyError) as service_only:
            fixture.registry.launch_local(fixture.service_graph)
        self.assertEqual(service_only.exception.status, base.Status.UNSUPPORTED)
        with self.api.graph({"a": fixture.schemas[1], "b": fixture.schemas[1]}, []) as invalid:
            with self.assertRaises(base.ReinyError) as graph_error:
                self.api.launch_local(fixture.registry, fixture.service_graph, invalid, INITIAL)
            self.assertEqual(graph_error.exception.status, base.Status.FAILURE)
        self.assertTrue(all(not state.created.is_set() for state in fixture.states))
        for document in (
                b'{"version":1,"version":1,"inputs":{},"outputs":{}}',
                b'{"version":1,"inputs":{},"outputs":{},"unknown":0}'):
            with self.assertRaises(base.ReinyError) as invalid:
                self.api.import_schema(document)
            self.assertEqual(invalid.exception.status, base.Status.INVALID_ARGUMENT)
        with self.api.value(fixture.schema, b"") as empty:
            self.assertEqual(empty.encoded(fixture.schema), b"")
        for secs, nanos in ((-1, 0), (1 << 64, 0), (0, 1_000_000_000), (True, 0)):
            with self.assertRaises(ValueError):
                steps.Duration(secs, nanos)
        self.assertEqual(C.sizeof(steps._Duration), 16)
        self.assertEqual(C.sizeof(steps._Tick), 64)
        self.assertEqual(C.sizeof(steps._Clock), 40)
        self.assertEqual(C.sizeof(steps._StepFactory), 16 + 7 * C.sizeof(C.c_void_p))
        fixture.launch()
        with fixture.request(dt=steps.Duration()) as zero:
            with self.assertRaises(steps.InvalidRequestError):
                fixture.scheduler.try_step(zero, NS)
        fixture.close()
