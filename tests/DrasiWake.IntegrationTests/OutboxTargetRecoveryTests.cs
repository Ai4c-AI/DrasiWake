using DrasiWake.Host;
using DrasiWake.Core.Domain;
using DrasiWake.Persistence.SonnetDB;
using DrasiWake.Persistence.SonnetDB.Entities;
using DrasiWake.LocalEnvironment;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DrasiWake.IntegrationTests;

public sealed class OutboxTargetRecoveryTests
{
    private const string BindingId = "sample-orders";

    [Fact]
    public async Task Legacy_pending_row_is_backfilled_from_its_current_binding()
    {
        var (root, host) = CreateHost();
        try
        {
            var outboxId = Guid.NewGuid();
            await SeedWakeAsync(host, CreateWake(outboxId, BindingId, null, WakeOutboxStatus.Pending));

            await host.Services.GetRequiredService<HostStartupValidator>()
                .StartAsync(TestContext.Current.CancellationToken);

            var persisted = await LoadWakeAsync(host, outboxId);
            Assert.Equal("sample-gateway", persisted.OpenClawTarget);
        }
        finally
        {
            host.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Legacy_active_row_with_missing_binding_fails_startup()
    {
        var (root, host) = CreateHost();
        try
        {
            var outboxId = Guid.NewGuid();
            await SeedWakeAsync(host, CreateWake(outboxId, "removed-binding", null, WakeOutboxStatus.Pending));

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => host.Services.GetRequiredService<HostStartupValidator>()
                    .StartAsync(TestContext.Current.CancellationToken));

            Assert.Contains(outboxId.ToString(), exception.Message, StringComparison.Ordinal);
            Assert.Contains("removed-binding", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            host.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Active_row_for_removed_target_fails_startup()
    {
        var (root, host) = CreateHost();
        try
        {
            var outboxId = Guid.NewGuid();
            await SeedWakeAsync(host, CreateWake(
                outboxId,
                BindingId,
                "removed-gateway",
                WakeOutboxStatus.RetryScheduled));

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => host.Services.GetRequiredService<HostStartupValidator>()
                    .StartAsync(TestContext.Current.CancellationToken));

            Assert.Contains(outboxId.ToString(), exception.Message, StringComparison.Ordinal);
            Assert.Contains("removed-gateway", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            host.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Completed_row_without_target_does_not_block_startup()
    {
        var (root, host) = CreateHost();
        try
        {
            var outboxId = Guid.NewGuid();
            await SeedWakeAsync(host, CreateWake(outboxId, "removed-binding", null, WakeOutboxStatus.Completed));

            await host.Services.GetRequiredService<HostStartupValidator>()
                .StartAsync(TestContext.Current.CancellationToken);

            var persisted = await LoadWakeAsync(host, outboxId);
            Assert.Null(persisted.OpenClawTarget);
        }
        finally
        {
            host.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    private static (string Root, IHost Host) CreateHost()
    {
        var root = Path.Combine(Path.GetTempPath(), $"DrasiWake-target-recovery-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        var repositoryRoot = RepositoryRootLocator.Find(AppContext.BaseDirectory);
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["DrasiWake:Drasi:ServerUri"] = "http://127.0.0.1:8080",
                ["DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress"] = "http://127.0.0.1:8081",
                ["DrasiWake:OpenClaw:Targets:sample-gateway:GatewayIdempotencyRetention"] = "30.00:00:00",
                ["DrasiWake:Database:Path"] = Path.Combine(root, "database"),
                ["DrasiWake:Registry:Path"] = Path.Combine(
                    repositoryRoot,
                    "src",
                    "DrasiWake.Host",
                    "contracts",
                    "sample-binding.yaml")
            })
            .Build();
        return (root, DrasiWakeHostBuilder.CreateHost(configuration));
    }

    private static WakeOutbox CreateWake(
        Guid id,
        string bindingId,
        string? target,
        WakeOutboxStatus status)
    {
        var now = DateTimeOffset.UtcNow;
        return new WakeOutbox
        {
            Id = id,
            BindingId = bindingId,
            SessionId = $"session-{id:N}",
            SnapshotFingerprint = "fingerprint",
            Skill = "triage-order",
            OpenClawTarget = target,
            InputJson = "{}",
            ContractVersion = "v1",
            IdempotencyKey = $"drasiwake:{id:N}",
            CreatedAtUtc = now,
            NextAttemptAtUtc = now,
            Status = status
        };
    }

    private static async Task SeedWakeAsync(IHost host, WakeOutbox wake)
    {
        var factory = host.Services.GetRequiredService<IDbContextFactory<BridgeDbContext>>();
        await using var context = await factory.CreateDbContextAsync(TestContext.Current.CancellationToken);
        await context.Database.MigrateAsync(TestContext.Current.CancellationToken);
        context.WakeOutbox.Add(wake);
        await context.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    private static async Task<WakeOutbox> LoadWakeAsync(IHost host, Guid id)
    {
        var factory = host.Services.GetRequiredService<IDbContextFactory<BridgeDbContext>>();
        await using var context = await factory.CreateDbContextAsync(TestContext.Current.CancellationToken);
        return await context.WakeOutbox.AsNoTracking()
            .SingleAsync(wake => wake.Id == id, TestContext.Current.CancellationToken);
    }
}
