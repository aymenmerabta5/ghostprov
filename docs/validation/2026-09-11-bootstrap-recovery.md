# Interrupted supervisor bootstrap recovery

## Incident and repair

The selected repository is `D:/MyWork/AI/ghostprov`. Production state comes from
`C:/Users/Aymen/.ghostprov/fresh-v2/fleet.json`. The unrelated local
`~/.ghostprov-v2` directory was not used. `.ghostprov-v2` below refers to paths
inside the Prism projects created by this repository.

Workers 1, 2, 6, and 8 retained generated bootstrap installers from supervisor
generations 146, 148, 148, and 164 respectively. Workers 2 and 6 also retained
`exec`. Their sidecar launch barriers remained at generations 143, 146, 147,
and 162. The installers matched the registered R10 generator and their saved
bindings; the immutable bundles and manifests also matched.

The direct bootstrap guard requires the launcher and installer to be absent.
The existing files therefore rejected every new attempt before staging.
Cleanup belonging to the rejected attempt could not remove the older files.
The outer deployment error omitted the nested ownership-conflict reason.

The incident-specific live repair verified the exact original manifest IDs,
the prior host's exit, its frozen zero-commit run, the unadvanced sidecar
barriers, zero current active/quarantined/ambiguous work, and exact file
ownership. It tombstoned only those bootstrap artifacts. No original project
was deleted or replaced, and the production listener was not restarted.
Worker 6's first readback encountered a transport error; an independent read
confirmed the new generation instead of replaying the mutation.

The existing recovery loop restored all eight original projects. An
authenticated read-only observation then recorded 120,082 ms continuously at
8/8 ready, with eight connected transports and zero quarantine/ambiguity.
The local evidence is `.ghostprov/bootstrap-repair/readiness-proof.json`.

## Implementation

- Runtime disposal retains the Yjs connection until supervisor shutdown and
  bootstrap cleanup finish.
- R10 direct bootstrap persists one private, bounded pending record per
  manifest project beside the manifest in `<fleet.json>.bootstrap`.
- Submission is recorded before calling the execution service. Interrupted
  prepared attempts can reclaim their exact owned files; submitted attempts
  require reconnection to the exact authenticated supervisor and signed
  shutdown before another launch. Unproven work remains blocked.
- Failed cleanup retains the record. Successful bootstrap/recovery removes
  it. Normal `bun dev` startup uses this path automatically.
- Recovery logs and diagnostic snapshots carry allowlisted bootstrap phase
  and code fields without raw exception details or credentials.

The production process was preserved during repair. The updated host code was
exercised separately on port 18888 and takes effect in production on the next
normal `bun dev` start. Unjournaled legacy files and foreign edits are not
automatically reclaimed merely because their content resembles GhostProv.
The journal contains sensitive execution identity and encrypted bootstrap
material; it is local recovery state, not telemetry.

## Validation

- Final isolated offline suite: **2,509 pass, 3 skipped, 0 fail**, 189 files.
- Runtime ordering, durable prepared/submitted recovery, partial cleanup,
  ownership rejection, journal bounds, and diagnostic redaction regressions.
- Main and scripts typechecks passed; `git diff --check` passed.
- Deterministic R8, R9, and R10 release checks passed. No frozen sidecar asset
  or sandbox source was changed.
- Updated-code disposable R10 validation passed **11 provider requests**,
  including tools, continuation, Anthropic, and concurrent requests. One
  signed recovery retained the same supervisor and created a fresh child.
  There was one direct bootstrap and zero LLM bootstrap calls. The disposable
  project was deleted. Evidence: `.ghostprov/live-r10/one-73FRq0/report.json`.
- A second disposable live validation deliberately failed pre-submission
  synchronization and cleanup. The following generation recovered the durable
  prepared record and became ready: **2 controllers, exactly 1 submission,
  1 recovered record, no pending record afterward**. Its project was deleted.
  Evidence: `.ghostprov/bootstrap-repair/proof/run-3dTnHt/report.json`.

The three skipped opt-in tests were the modeled 14-agent soak, 16 MiB transport,
and 64 MiB request-integrity cases. Pinned Deno checks were not rerun because
this change does not modify sandbox code or frozen bundles. The two-minute
production observation is not a long-duration soak.
