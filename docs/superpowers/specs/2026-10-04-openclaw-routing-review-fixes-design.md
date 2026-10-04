# OpenClaw Target Routing Review Fixes

## Context

The target-routing implementation persists each wake's selected OpenClaw target
and recovers legacy outbox rows during host startup. Review identified two
startup-validation gaps and a client-lifetime issue:

- Retention validation currently considers only the target on the current
  binding. Active outbox rows can still reference an older target after a
  binding changes.
- Recovery treats both database `null` and whitespace target values as legacy
  rows, although only `null` is produced by the migration.
- `TargetRoutedWakeSink` caches `HttpClient`-backed clients indefinitely, which
  can prevent `IHttpClientFactory` handler rotation from refreshing DNS.

The existing full Release test baseline is 125 passed, 3 opt-in tests skipped,
and 0 failed.

## Design

Keep recovery and active-row validation in the existing
`EnsureOpenClawTargetsAsync` store transaction. The host will pass the current
binding-to-target mapping, configured target names, each current binding's
maximum retry age, and each configured target's idempotency retention.

For every active outbox row:

1. Backfill its target only when the persisted value is `null`, using the
   current binding with the same ID. A missing binding or invalid binding target
   remains a startup error.
2. Reject empty or whitespace targets rather than interpreting them as legacy
   data.
3. Require the persisted target to be configured.
4. When the row's binding still exists, require the persisted target's
   idempotency retention to cover that binding's maximum retry age. This
   validates the row's persisted route, including targets no longer selected by
   the current binding.

Perform all checks before committing the transaction so a failure cannot
partially persist legacy backfills. Existing validation of each current
binding's selected target remains in place. Startup errors should identify the
row, binding, and target where applicable, without exposing credentials.

Replace the target-client cache in `TargetRoutedWakeSink` with a target-specific
client created through `IHttpClientFactory` for each invocation or status
operation. Dispose each operation-scoped client after the operation completes;
the factory continues to pool and rotate the underlying handlers.

## Testing

- Add startup/integration coverage proving startup rejects an active row routed
  to an older target whose retention is shorter than its current binding's
  maximum retry age.
- Add startup/integration coverage proving whitespace target values fail
  rather than being backfilled, while existing `null` backfill behavior
  continues to pass.
- Update routed-sink tests to verify per-operation client creation, target
  endpoint selection, and credential isolation across targets.
- Run the focused integration and persistence tests, then the full Release
  suite and Host Release build.

## Scope

Do not change the outbox schema, retry scheduling, Gateway idempotency policy,
request payloads, or routing behavior for valid persisted targets. Do not
modify the user-provided untracked design spec.
