# Explicit SSH process hosts

This slice adds the Rust `reiny_launch::serve_remote` entry point. It accepts the
same verified `PreparedDeployment` as `serve`, plus an explicit host-name-to-
`SshTarget` map. Hosts absent from the map still execute locally. The
`ssh_owner` example reads these two inputs from JSON files. Existing CLI
manifests and prepared-file versions do not acquire an implicit remote provider.

## Execution and verification

Each target supplies an absolute local OpenSSH executable and identity-key path,
an explicit server address, port and account, a pinned public host key, an
absolute remote executable/bundle/working directory, exact configuration-bundle
root mappings, and a reserved remote control port.

The remote executable must have exactly the prepared bundle-relative name.
Before starting SSH, the owner verifies the ordinary prepared deployment and
constructs hashes for every regular file in each selected executable and config
bundle, including the artifact manifests. The remote shell checks the exact file
count, rejects symlinks and special files, and compares every file's SHA-256.
Only its successful verification marker permits the owner to write bootstrap.
There is no upload, remote build, executable fallback or repair.

The remote account, its shell/utilities, the SSH server and immutable installed
files are trusted, as is application code in the existing local execution model.
This is verification of preinstalled bytes, not hostile-host attestation.
Applications must keep verified files immutable for execution; verification is
not an atomic filesystem snapshot against concurrent trusted-user mutation.
Empty extra directories have no application-file identity.

OpenSSH runs without user/system client configuration, connection sharing,
interactive authentication, agent forwarding or X11 forwarding. The private
known-hosts file contains only the pinned server key; unknown or changed keys
fail. The owner requests a loopback-only reverse forward to its already-bound
loopback control listener. The remote SSH server must honor the requested
loopback bind rather than override it with a wildcard GatewayPorts policy.
Application stdout/stderr go to the generation's stderr log because SSH stdout
is reserved for the verification marker.
The remote command explicitly execs `/bin/sh` for verification, then execs the
verified native executable; it does not interpret the program in a fish login shell.

Spaces in the local known-hosts path are quoted. Project-root spellings containing
SSH config expansion/escape characters (`%`, `$`, `"`, `\`, or line breaks) are
rejected before remote effects so they cannot redirect pinned trust to another file.

The inherited nonce never appears in arguments, scripts, prepared files or
ownership records. Existing session identity, factory revision, schema checks,
whole-graph Prepare/Commit/Run admission, deadlines and cooperative stop remain
on the existing ProcessHost protocol.

Data remains on the explicit prepared Local/Zenoh routes. SSH forwards control,
not arbitrary application data. Network locators do not grant endpoint
authority. This slice does not add Zenoh encryption, network peer attestation or
physical-device safety guarantees.

## Durable ownership and manual reconciliation

Before spawning an SSH client, the owner durably publishes a reservation under
`<project>/.reiny/remote-ownership/`. It syncs the file and containing directories.
A record contains the exact session identity and a target/manifest digest, never
credentials. Files are retained across normal exit, failed startup and owner
crash. A failed reservation or directory sync prevents the SSH spawn.

Four observations remain separate:

- `local_client_reaped`: the owner reaped only its local SSH-client process tree.
- `cooperative_outcome`: the authenticated ProcessHost's component-task outcome.
- `remote_command_exit`: SSH's command exit status after bootstrap verification;
  transport failures, signals, forced local termination and status 255 leave it
  absent. This is not evidence about remote descendants.
- `tree_cleanup`: always `Unknown` in this transport; SSH cannot prove native
  descendant containment or reaping.

Thus even cooperative success retains Unknown. Final deployment status is
Failed, and remote host status is Unresponsive with the separate cooperative
outcome preserved. `serve_remote` returns an error rather than a clean-stop
claim. Local hosts and local-only `serve` calls retain their prior semantics.

Any unresolved record blocks a new owner at that project root, including an old
local `serve` caller. A changed apply on a remote owner is refused before
replacing anything; an identical live apply remains a no-op. This is a
project-root ownership ledger, not a global distributed lock: copying a project
to a different root or deleting its retained files is not reconciliation.

After stopping the local owner, inspect `remote_ownership(root)` and explicitly
call `reconcile_remote(root, exact_session_identity, exact_target_digest, note)`.
It acquires the same owner lock, rejects stale identity, wrong target, repeated
reconciliation and blank notes, and durably records the operator's release
decision. The record is never deleted and `tree_cleanup` remains Unknown.
Reconciliation is authorization to attempt replacement despite unresolved work,
not acknowledgment that cleanup was proven. The framework neither reconnects
nor signals a remote PID, and provides no automatic recovery or supervisor.

## Supported environment and evidence limits

The initial transport supports Linux owners and targets with compatible native
executables/runtime libraries, OpenSSH, `/bin/sh`, and GNU `find`,
and `sha256sum`. Installed application dependencies outside declared
bundles remain application/environment obligations. A target-native rebuild
with merely the same factory revision does not satisfy byte equality.
Verification commands are bounded to 96 KiB; oversized manifests fail before
remote effects. Control ports are explicit, and conflicts fail rather than
select an undisclosed port.

The integration fixture uses temporary keys, an unprivileged private loopback
sshd and the actual native component-placement graph. Its own Linux pidfds and
subreaper are test containment/observation only, not production features.
Same-machine SSH exercises the mechanism and failure contracts; it does not
establish cross-architecture compatibility, real-machine availability or
hardware stop behavior.
