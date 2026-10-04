# OpenClaw Target Routing Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the startup retention/backfill gaps for persisted OpenClaw routes and let `IHttpClientFactory` rotate handlers by creating a target client for each operation.

**Architecture:** Extend the existing `IBridgeStore.EnsureOpenClawTargetsAsync` startup contract with binding retry ages and target idempotency retentions. `SonnetBridgeStore` will backfill only migration-produced `null` values and validate each active row's persisted target inside its existing transaction. `TargetRoutedWakeSink` will create and dispose an OpenClaw client for every invocation and status replay, while the factory manages handler pooling and rotation.

**Tech Stack:** .NET 10, C#, EF Core / SonnetDB, `IHttpClientFactory`, xUnit v3, Microsoft.Testing.Platform.

## Global Constraints

- A legacy outbox target may be backfilled only when its database value is `null`.
- Whitespace targets must fail startup; no implicit target fallback is allowed.
- Every active outbox row must reference a configured target.
- For an active row whose binding still exists, that row's persisted target retention must cover that binding's `Retry.MaxAge`.
- Keep backfill and validation in the existing store transaction; do not save partial backfills when validation fails.
- Create one target-specific client for each invoke/status operation and dispose it after the operation; `IHttpClientFactory` retains responsibility for handler pooling and rotation.
- Do not change the outbox schema, retry scheduling, Gateway idempotency policy, request payloads, or valid persisted-target routing behavior.

---

### Task 1: Add Startup Regression Tests

**Files:**
- Modify: `tests/DrasiWake.IntegrationTests/OutboxTargetRecoveryTests.cs`

**Interfaces:**
- Consumes: Existing `CreateHost`, `SeedWakeAsync`, and `CreateWake` test helpers.
- Produces: Reproducible tests for an active row on an older, short-retention target and for a whitespace target that must not be backfilled.

- [ ] **Step 1: Add a failing test for an active row routed to an older target**

The sample binding's retry max age is one day. Configure `previous-gateway` with one-hour retention, seed a `RetryScheduled` row for `sample-orders` whose persisted target is `previous-gateway`, and assert startup reports the row and target:

```csharp
[Fact]
public async Task Active_row_on_previous_target_fails_when_retention_is_shorter_than_binding_retry_age()
{
    var (root, host) = CreateHost(new Dictionary<string, string?>
    {
        ["DrasiWake:OpenClaw:Targets:previous-gateway:BaseAddress"] = "http://127.0.0.1:8082",
        ["DrasiWake:OpenClaw:Targets:previous-gateway:GatewayIdempotencyRetention"] = "01:00:00"
    });
    try
    {
        var outboxId = Guid.NewGuid();
        await SeedWakeAsync(host, CreateWake(
            outboxId,
            BindingId,
            "previous-gateway",
            WakeOutboxStatus.RetryScheduled));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => host.Services.GetRequiredService<HostStartupValidator>()
                .StartAsync(TestContext.Current.CancellationToken));

        Assert.Contains(outboxId.ToString(), exception.Message, StringComparison.Ordinal);
        Assert.Contains("previous-gateway", exception.Message, StringComparison.Ordinal);
        Assert.Contains("retention", exception.Message, StringComparison.OrdinalIgnoreCase);
    }
    finally
    {
        host.Dispose();
        Directory.Delete(root, recursive: true);
    }
}
```

Extend `CreateHost` with an optional `IReadOnlyDictionary<string, string?>? additionalConfiguration = null`. After constructing the existing default configuration dictionary, copy each extra pair into it before calling `AddInMemoryCollection`; this lets the test configure an additional named target without changing existing callers.

- [ ] **Step 2: Add a failing test for whitespace target data**

Seed an active row with `"  \t"` as its persisted target. Assert startup throws an `InvalidOperationException` naming the row, then reload the row and assert the persisted target remains `"  \t"`; it must not be replaced with `sample-gateway`.

- [ ] **Step 3: Run the recovery tests and confirm both new cases fail for the reported gaps**

Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj --configuration Release --filter-class DrasiWake.IntegrationTests.OutboxTargetRecoveryTests`

Expected: the previous-target case currently starts instead of rejecting the short retention, and the whitespace-target case currently starts after silent backfill. Existing null-target backfill tests continue to pass.

---

### Task 2: Validate Persisted Targets and Retention Atomically

**Files:**
- Modify: `src/DrasiWake.Core/Abstractions/IBridgeStore.cs`
- Modify: `src/DrasiWake.Host/HostStartupValidator.cs`
- Modify: `src/DrasiWake.Persistence.SonnetDB/SonnetBridgeStore.cs`
- Modify: `tests/DrasiWake.Core.Tests/Pipeline/OutboxDispatcherTests.cs`
- Modify: `tests/DrasiWake.Core.Tests/Pipeline/RecoveryCoordinatorTests.cs`
- Modify: `tests/DrasiWake.Core.Tests/Pipeline/SnapshotReconcilerTests.cs`
- Modify: `tests/DrasiWake.Persistence.SonnetDB.Tests/AtomicAcceptanceTests.cs`
- Test: `tests/DrasiWake.IntegrationTests/OutboxTargetRecoveryTests.cs`

**Interfaces:**
- Consumes: Startup regression tests from Task 1 and current `IBridgeStore.EnsureOpenClawTargetsAsync`.
- Produces: `EnsureOpenClawTargetsAsync` accepts the current maximum retry age per binding and idempotency retention per configured target, in addition to the existing target mapping and configured-name set.

- [ ] **Step 1: Extend the store interface and update its in-memory test doubles**

Add these arguments before `CancellationToken cancellationToken` in `IBridgeStore.EnsureOpenClawTargetsAsync`:

```csharp
IReadOnlyDictionary<string, TimeSpan> maximumRetryAgeByBindingId,
IReadOnlyDictionary<string, TimeSpan> idempotencyRetentionByTarget,
```

Apply the same signature to the three `IBridgeStore` test doubles listed above. Their implementations remain no-ops returning `ValueTask.CompletedTask`.

Update the direct `SonnetBridgeStore.EnsureOpenClawTargetsAsync` call in `AtomicAcceptanceTests.Legacy_pending_row_is_backfilled_after_nullable_target_migration` to pass `[bindingId] = TimeSpan.FromDays(1)` and `["sample-gateway"] = TimeSpan.FromDays(30)` policy dictionaries before the cancellation token.

- [ ] **Step 2: Pass binding ages and target retentions from startup**

In `HostStartupValidator.StartAsync`, build ordinal dictionaries from the validated candidate and settings:

```csharp
var maximumRetryAgeByBindingId = candidate.Registry.Bindings.ToDictionary(
    binding => binding.Id,
    binding => binding.Retry.MaxAge,
    StringComparer.Ordinal);
var idempotencyRetentionByTarget = settings.OpenClawTargets.ToDictionary(
    pair => pair.Key,
    pair => pair.Value.GatewayIdempotencyRetention,
    StringComparer.Ordinal);
```

Pass both dictionaries to `store.EnsureOpenClawTargetsAsync` together with the existing `targetByBindingId`, `configuredTargetNames`, and cancellation token. Keep the current binding-target validation and its current-target retention check.

- [ ] **Step 3: Make null-only backfill and persisted-route validation in the store**

In `SonnetBridgeStore.EnsureOpenClawTargetsAsync`, preserve the transaction and active status query. Replace the `IsNullOrWhiteSpace` legacy test with a `null` check. If the value is non-null but whitespace, throw:

```csharp
throw new InvalidOperationException(
    $"Active wake outbox item '{item.Id}' for binding '{item.BindingId}' has an empty or whitespace OpenClaw target.");
```

After the existing configured-target check, use `targetByBindingId.ContainsKey(item.BindingId)` to determine whether the binding is still current. For a current binding, require policy entries in `maximumRetryAgeByBindingId` and `idempotencyRetentionByTarget`, then compare that persisted target's retention with the binding's retry age. Throw an `InvalidOperationException` naming the row, binding, and persisted target if retention is shorter. A removed binding has no retry-age policy to enforce; its active row must still have a configured persisted target.

Do not call `SaveChangesAsync` or commit until all rows have passed validation. Preserve successful legacy `null` backfill and the existing errors for legacy rows without a current binding or configured target.

- [ ] **Step 4: Run startup regressions and affected Core tests**

Run:

```powershell
dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj --configuration Release --filter-class DrasiWake.IntegrationTests.OutboxTargetRecoveryTests
dotnet test --project tests\DrasiWake.Core.Tests\DrasiWake.Core.Tests.csproj --configuration Release
```

Expected: all recovery and Core tests pass, including the two new failures from Task 1 now rejecting startup.

---

### Task 3: Assert Per-Operation Routed Client Creation

**Files:**
- Modify: `tests/DrasiWake.IntegrationTests/TargetRoutedWakeSinkTests.cs`

**Interfaces:**
- Consumes: Existing recording `IHttpClientFactory`, alpha/beta target setup, and invocation/status replay test.
- Produces: A regression assertion that the factory creates a client separately for each operation and for the correct target.

- [ ] **Step 1: Strengthen the existing route test**

After the two invokes and two status replays, replace the once-per-target `CreatedNames` assertion with explicit per-operation counts:

```csharp
Assert.Equal(2, factory.CreatedNames.Count(name => name == "alpha"));
Assert.Equal(2, factory.CreatedNames.Count(name => name == "beta"));
```

Keep the existing endpoint, authorization isolation, idempotency-key, skill, session, and input assertions. The unknown-target test must continue to assert that no client is created.

- [ ] **Step 2: Run the routed-sink test and confirm the lifecycle assertion fails**

Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj --configuration Release --filter-class DrasiWake.IntegrationTests.TargetRoutedWakeSinkTests`

Expected: the invocation/status behavior passes, but each target's factory creation count is one instead of two.

---

### Task 4: Create and Dispose a Client for Each Routed Operation

**Files:**
- Modify: `src/DrasiWake.Host/TargetRoutedWakeSink.cs`
- Test: `tests/DrasiWake.IntegrationTests/TargetRoutedWakeSinkTests.cs`

**Interfaces:**
- Consumes: Existing `IHttpClientFactory`, shared `OpenClawOptions`, and immutable target options.
- Produces: `InvokeAsync` and `GetStatusAsync` each create a fresh `OpenClawMetaInvocationClient` for the request's persisted target and dispose it after awaiting the operation.

- [ ] **Step 1: Remove target-client caching and sink disposal**

Remove `ConcurrentDictionary`, the cached lazy clients, and `TargetRoutedWakeSink.Dispose`. Keep target validation and unknown-target errors in the resolver; have it return a newly constructed adapter client:

```csharp
private OpenClawMetaInvocationClient Resolve(WakeRequest request)
{
    ArgumentNullException.ThrowIfNull(request);
    var targetName = request.OpenClawTarget;
    if (string.IsNullOrWhiteSpace(targetName) || !targets.TryGetValue(targetName, out var targetOptions))
    {
        throw new InvalidOperationException(
            $"Wake request '{request.IdempotencyKey}' references unknown OpenClaw target '{targetName}'.");
    }

    return new OpenClawMetaInvocationClient(
        httpClientFactory.CreateClient(targetName),
        options,
        targetOptions);
}
```

- [ ] **Step 2: Await each operation inside a client disposal scope**

Change both methods to `async ValueTask` and scope the resolved client to the operation:

```csharp
public async ValueTask<WakeAcceptance> InvokeAsync(WakeRequest request, CancellationToken cancellationToken)
{
    using var client = Resolve(request);
    return await client.InvokeAsync(request, cancellationToken);
}

public async ValueTask<WakeExecutionStatus?> GetStatusAsync(
    WakeRequest request,
    CancellationToken cancellationToken)
{
    using var client = Resolve(request);
    return await client.GetStatusAsync(request, cancellationToken);
}
```

Remove `using var sink` from the routed-sink tests because the sink no longer owns cached disposable clients.

- [ ] **Step 3: Run routed-sink tests**

Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj --configuration Release --filter-class DrasiWake.IntegrationTests.TargetRoutedWakeSinkTests`

Expected: both tests pass; four operations create four named clients, requests still use the persisted target endpoint and its own credentials, and unknown targets create none.

---

### Task 5: Verify the Complete Change

**Files:**
- Verify: `DrasiWake.sln`
- Verify: `src/DrasiWake.Host/DrasiWake.Host.csproj`

- [ ] **Step 1: Run the focused persistence test project**

Run: `dotnet test --project tests\DrasiWake.Persistence.SonnetDB.Tests\DrasiWake.Persistence.SonnetDB.Tests.csproj --configuration Release`

Expected: all persistence tests pass, including legacy nullable-target migration/backfill coverage.

- [ ] **Step 2: Run the full Release test suite**

Run: `dotnet test --solution DrasiWake.sln --configuration Release`

Expected: 0 failed; the three existing real-service/Aspire tests may remain opt-in skipped.

- [ ] **Step 3: Build the Host in Release**

Run: `dotnet build src\DrasiWake.Host\DrasiWake.Host.csproj --configuration Release --no-restore`

Expected: build succeeds with 0 warnings and 0 errors.
