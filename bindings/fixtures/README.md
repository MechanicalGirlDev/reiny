# Prepared smoke bindings

These static version-2 contracts match `reiny-core::bindings::ModuleBindings`.
The smokes use type `Binary` and register its compiled schema fingerprint as 42;
fingerprints belong in registration/reports, not the prepared endpoint contract.

`sender` and `other` each publish `out`. `receiver` subscribes on `in` only to
`sender`. `receiver-multi` registers `all` (sender and other) and `filtered`
(sender only), exercising exact source isolation. `isolated` registers `out`
and an `in` from sender and itself in another domain, so the self-sent sentinel
also detects leaked sender traffic. `network` registers `out` and a self-routed
`in`. All paths passed to constructors must be
absolute; namespace and endpoint namespace match the smoke session ids.

C/C++ smokes take absolute sender and receiver fixture paths as two arguments.
JavaScript, TypeScript, Python, and Ruby resolve fixtures from their source file.
C# defaults to `../fixtures` when run from `bindings/csharp`, or accepts a fixture
directory argument. Kotlin and Swift default to `bindings/fixtures` from the
repository root, or accept a fixture directory argument.

Regenerate untracked UniFFI wrappers after rebuilding the migrated Rust library
before running the Python, Ruby, Kotlin, or Swift smokes.
