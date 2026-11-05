# C/C++ source decomposition

The original public entrypoints remain `reiny_component.hpp`,
`reiny_component_step.hpp`, and `reiny_job.hpp`. Install or copy the complete
`include/` directory, including the internal `.hpp` helpers beside them.
The crate has no package include/exclude allowlist; these helpers ship with
the other headers. Copy the complete C/C++ test sources and `.inc` fragments
when building the native clients in a private snapshot.

## Codemap and movement plan

The service header owns common byte/error/handle utilities, endpoint/context
capabilities, and service factory/host lifecycle. Extract the first two
responsibilities into guarded sibling headers and keep lifecycle at the public
entrypoint. The step header depends on that service entrypoint; move immutable
step values/input views to a sibling header, keeping callbacks and scheduling
at its original entrypoint. The job header similarly separates callback work
construction from observation/admission. Preserve namespace order, definitions,
layouts, callback conversion, and ownership transfer byte for byte.

Native caller movement follows the existing textual `.inc` convention:
service callback fixture, finalization scenarios, and handle-race scenarios;
C step callbacks; C++ step node callbacks and graph fixture. The original
translation units retain their includes and complete original `main` case
collection. No assertion, expected value, callback gate, or timeout changes.
Baseline all six C/C++ service/step/job clients before movement, then repeat
strict compilation and actual calls after header and caller movements.

## Verification identity

Use the fresh library recorded in
`/mnt/workspace/nop/reiny-final-commits-check/foreign/native-library.sha256`:
`949c232f92aa66e359745b3dae603ca6df6708b00a6659976afe5c2c269c4160`.
Current native checks use an absolute link to that library, strict C11/C++17
warnings, pthreads, and a 60-second failure bound for each client.

The historical Python acceptance runners have explicit snapshot file lists
that predate these internal fragments. They are preserved unchanged, as are
their historical artifact hashes. They do not snapshot this decomposed source
tree completely; current native verification must copy the complete include
directory and C/C++ caller fragments instead. Their historical receipts are
independent evidence, not current-source native PASS evidence.

## November 5, 2026 movement result

All six strict C/C++ service, managed-step, and owned-job callers passed before
the move, after the header move, and after the caller move. Each stage exited
0. The final stage compiled and ran a private copy of the complete include
directory and all C/C++ test fragments. All original `main` case collections
and assertion bodies were preserved by expanding the new textual includes
and comparing to the original sources, ignoring only blank/comment lines.
Reassembled wrapper declaration bodies likewise matched their originals.
The three ABI `.h` files matched the frozen foreign source snapshot.

All three original C++ entrypoints also passed strict standalone syntax checks,
as did mixed/repeated header inclusion. LSP diagnostics were unavailable because
`clangd` is not installed; strict compiler diagnostics are the executed check.
Locked, offline `cargo +1.99 package --list --allow-dirty -p reiny-ffi`
exited 0 and listed every new header and caller fragment in `package-list.log`.
The original C++ service/job clients,
existing lifecycle fragments, native library, and acceptance scripts were not
edited. The earlier nine foreign gates remain the independent receipt's
evidence; this decomposition did not rerun Rust or Python suites.

Evidence root: `/mnt/workspace/nop/reiny-cpp-decompose-st_01a1aa58`.
`headers.log` and `final.log` retain the per-caller native outcomes.
`source.sha256` records the final changed-file inventory; `executables.sha256`
records the compiled binaries before cleanup. `dynamic-links.log` records
absolute native-library dependencies. Baseline output is retained in the
child session's `bash_2` transcript. Temporary executables and preprocessing
outputs are removed after their evidence is preserved.

The strict commands use `g++ -std=c++17 -Wall -Wextra -Werror -pedantic
-pthread` for `component_cpp_client.cpp`, `component_step_cpp_client.cpp`,
and `job_cpp_client.cpp`. The C callers `component_client.c`,
`component_step.c`, and `job_client.c` use `cc -std=c11 -Wall -Wextra
-Werror -pedantic -pthread`. Each includes the private include directory,
links the absolute verified `libreiny_ffi.so`, and runs under `timeout 60`.
The step/job C++ runs set `REINY_STEP_CPP_LIBRARY` and `REINY_JOB_CPP_LIBRARY`
to that exact path, preserving their `/proc/self/maps` assertions.

### Pure source lines

Counts exclude blank lines and lines beginning with `//`, `#`, or `--`,
using the same rule as the original inventory.

| File | Lines |
| --- | ---: |
| include/reiny_component.hpp | 129 |
| include/reiny_component_base.hpp | 88 |
| include/reiny_component_ports.hpp | 150 |
| include/reiny_component_step.hpp | 148 |
| include/reiny_component_step_values.hpp | 122 |
| include/reiny_job.hpp | 118 |
| include/reiny_job_work.hpp | 133 |
| tests/component_client.c | 162 |
| tests/component_callbacks.inc | 158 |
| tests/component_lifecycle.inc | 127 |
| tests/component_step.c | 169 |
| tests/component_step_callbacks.inc | 83 |
| tests/component_step_cpp_client.cpp | 114 |
| tests/component_step_cpp_callbacks.inc | 84 |
| tests/component_step_cpp_fixture.inc | 106 |

### Architectural review

Each helper owns one existing responsibility: handle utilities, endpoint
capabilities, immutable step values, work construction, callback fixtures,
graph fixture, or lifecycle scenarios. Parsing boundaries, exhaustive admission
switches, casts, ABI-required callback signatures, error conversion, RAII,
factory transfer and finalization remain byte-for-byte original declarations.
There are no new runtime helpers, branches, checks, logging, semantic names,
or test contracts. Existing callback parameters are fixed ABI contracts, not
new parameter bloat. No tests were added, removed, reshaped, skipped, or
weakened, so no mutation-only test or new production seam was introduced.
