# Service component ABI checks

`include/reiny_component.h` is the additive ABI contract. The old `reiny.h`
API and its `0/-1` statuses are unchanged. The native executable ABI supports
explicit service registration, immutable schema identities, strict resolved
graph import, Local hosting, named byte streams/RPC, lifecycle observation,
and the existing authenticated process-host executable entry.

Run from a private source snapshot with a private `CARGO_TARGET_DIR`:

```sh
cargo +1.99 clippy -p reiny-ffi --no-default-features \
  --all-targets -- -D warnings
cargo +1.99 test -p reiny-ffi --no-default-features
cargo +1.99 clippy -p reiny-ffi --all-targets -- -D warnings
cargo +1.99 test -p reiny-ffi
cargo +1.99 fmt -p reiny-ffi -- --check
```

The native integration tests use Linux `cc`, pthreads and `timeout`.
Copy the complete C/C++ test sources with their `.inc` fragments when using a
private snapshot. `component_client.c` includes `component_callbacks.inc` and
`component_lifecycle.inc`; `component_step.c` includes
`component_step_callbacks.inc` and `component_step_lifecycle.inc`.
`component_client.c` checks real binary stream delivery/provenance, RPC success,
application error and unanswered-request finalization, invalid/stale/borrowed
handles, preparation restrictions, cooperative shutdown, failed preparation,
unresponsive callbacks followed by late teardown, release races, and finalizer
quiescence. Condition variables and barriers establish every concurrency event;
deadlines only bound failures or test the timeout behavior itself.

`component_process.c` runs as a separate executable. Its Rust integration test
writes the inherited framed bootstrap, authenticates the exact nonce and
revision, validates the actual compiled report, commits, runs, stops, and reaps
the child. It also rejects Run before Commit and proves no run callback happened.
This tests the process endpoint, not a deployment owner, descendant process
cleanup, remote process ownership, or cross-process stream/RPC delivery.
The C test binaries link an absolute test-library path so Cargo's
`LD_LIBRARY_PATH` cannot substitute a stale feature build. The process tests
verify the child's `/proc/<pid>/maps` and print the mapped library's SHA-256.
`REINY_FFI_TEST_LIBRARY=/absolute/library.so` selects a separately built artifact
for that same process acceptance test. This path is used only by test binaries;
the distributed ABI library has no private absolute dependency or runtime path.

The opt-in `component-ffi-skip-miri` excludes native subprocess tests only under
`cfg(miri)`. Ordinary test commands always execute them, regardless of that
feature. Pure ABI memory checks remain in
`component::tests` and use the production pointer and handle helpers:

```sh
cargo +nightly miri test -p reiny-ffi --no-default-features \
  --lib component::tests
MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-symbolic-alignment-check -Zmiri-preemption-rate=0.1 -Zmiri-backtrace=full" \
  cargo +nightly miri test -p reiny-ffi --no-default-features \
  --lib component::tests
MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-strict-provenance -Zmiri-symbolic-alignment-check -Zmiri-preemption-rate=0.1 -Zmiri-backtrace=full" \
  cargo +nightly miri test -p reiny-ffi --no-default-features \
  --lib component::tests
```

For native address checking, build the Rust cdylib using
`RUSTFLAGS="-Zsanitizer=address"` and `cargo +nightly build -p reiny-ffi
--no-default-features --target x86_64-unknown-linux-gnu` in a separate target.
Compile `component_client.c` with `-fsanitize=address,undefined
-fno-omit-frame-pointer` and link it to that instrumented cdylib. Miri checks
only the named Rust tests; sanitizers check only executed native paths. Neither
is proof of whole-ABI soundness.

Instance callbacks run serially on their dedicated ordinary thread.
`release_factory` is different: it runs synchronously on the last-owner thread
and must not block or unwind. The adversarial finalizer test deliberately
exceeds that contract to check that `host_wait_joined` does not report
quiescence early. It does not establish scheduling isolation for arbitrary
blocking finalizers. Rust owner/adapter claims and native lane claims both
participate in quiescence accounting.

Python cannot implement the nonblocking native finalizer contract because a
ctypes callback acquires the GIL before executing its Python body. The
`register_notified` path requires a NULL finalizer and publishes a native atomic
release token instead. Python keeps its callback roots while the token is
pending, then collects them on a caller thread after `factory_released` reports
completion. The native finalization path never acquires a GIL or calls Python.
The C test covers pending notification during an unresponsive callback, late
release after teardown, one-use binding and rejection of a foreign finalizer.

The public header documents the remaining limits: raw bytes are opaque,
fingerprints do not validate payload decoding, admitted effects can race stop,
context clones retain resources without keeping the host running, and timeout
does not kill native callbacks. Steps, jobs, distributed scheduling, mixed-route
deployment selection, semantic contracts, HS Project execution and remote
machine ownership are not implemented by this service ABI.
