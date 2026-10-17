using System.Text.Json.Nodes;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;
using DrasiWake.Persistence.SonnetDB;
using DrasiWake.Persistence.SonnetDB.Entities;
using Microsoft.EntityFrameworkCore;

namespace DrasiWake.Persistence.SonnetDB.Tests;

public sealed class OutboxQueryTests
{
    [Fact]
    public async Task QueryOutboxAsync_filters_by_status_binding_and_session()
    {
        using var database = await TestDatabase.CreateAsync(TestContext.Current.CancellationToken);
        var rows = new[]
        {
            CreateRow("b-1", "s-1", WakeOutboxStatus.DeadLetter, -3),
            CreateRow("b-1", "s-2", WakeOutboxStatus.Pending, -2),
            CreateRow("b-2", "s-1", WakeOutboxStatus.DeadLetter, -1),
        };
        await database.InsertOutboxAsync(TestContext.Current.CancellationToken, rows);

        var deadLetters = await database.Store.QueryOutboxAsync(
            new OutboxQuery([WakeOutboxStatus.DeadLetter], null, null, 50, null, null),
            TestContext.Current.CancellationToken);
        Assert.Equal(
            new[] { rows[2].Id, rows[0].Id },
            deadLetters.Select(item => item.Id).ToArray());

        var binding = await database.Store.QueryOutboxAsync(
            new OutboxQuery(null, "b-1", null, 50, null, null),
            TestContext.Current.CancellationToken);
        Assert.Equal(
            new[] { rows[1].Id, rows[0].Id },
            binding.Select(item => item.Id).ToArray());

        var session = await database.Store.QueryOutboxAsync(
            new OutboxQuery(null, null, "s-2", 50, null, null),
            TestContext.Current.CancellationToken);
        Assert.Equal(new[] { rows[1].Id }, session.Select(item => item.Id).ToArray());
    }

    [Fact]
    public async Task QueryOutboxAsync_keyset_pagination_walks_all_rows_without_duplicates()
    {
        using var database = await TestDatabase.CreateAsync(TestContext.Current.CancellationToken);
        var rows = Enumerable.Range(0, 5)
            .Select(index => CreateRow($"b-{index}", "s", WakeOutboxStatus.Pending, -index - 1))
            .ToArray();
        await database.InsertOutboxAsync(TestContext.Current.CancellationToken, rows);

        var visited = new List<Guid>();
        DateTimeOffset? cursorCreated = null;
        Guid? cursorId = null;
        while (true)
        {
            var page = await database.Store.QueryOutboxAsync(
                new OutboxQuery(null, null, null, 2, cursorCreated, cursorId),
                TestContext.Current.CancellationToken);
            if (page.Count == 0)
                break;

            Assert.True(page.Count <= 2);
            visited.AddRange(page.Select(item => item.Id));
            var last = page[^1];
            cursorCreated = last.CreatedAtUtc;
            cursorId = last.Id;
        }

        Assert.Equal(rows.Length, visited.Count);
        Assert.Equal(visited.Count, visited.Distinct().Count());
        // Newest rows first.
        Assert.Equal(
            rows.OrderByDescending(row => row.CreatedAtUtc).Select(row => row.Id),
            visited);
    }

    private static WakeOutboxItem CreateRow(
        string bindingId,
        string sessionId,
        WakeOutboxStatus status,
        int secondsAgo)
    {
        var createdAt = DateTimeOffset.FromUnixTimeMilliseconds(
            DateTimeOffset.UtcNow.AddSeconds(secondsAgo).ToUnixTimeMilliseconds());
        return new WakeOutboxItem(
            Guid.NewGuid(),
            bindingId,
            sessionId,
            $"fp-{Guid.NewGuid():N}",
            "triage-order",
            new JsonObject { ["value"] = 1 },
            "1.0.0",
            $"idem-{Guid.NewGuid():N}",
            AttemptCount: 0,
            createdAt,
            createdAt,
            status,
            InvocationId: null,
            TraceId: null,
            "sample-gateway");
    }

    private sealed class TestDatabase(string directory, DbContextOptions<BridgeDbContext> options) : IDisposable
    {
        private readonly DbContextOptions<BridgeDbContext> options = options;

        public SonnetBridgeStore Store { get; } = new(new TestContextFactory(options));

        public static async Task<TestDatabase> CreateAsync(CancellationToken cancellationToken)
        {
            var directory = Path.Combine(AppContext.BaseDirectory, "outbox-query-testdata", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            var options = new DbContextOptionsBuilder<BridgeDbContext>()
                .UseSonnetDB($"Data Source={directory}")
                .Options;
            var database = new TestDatabase(directory, options);
            await using var context = new BridgeDbContext(options);
            await context.Database.MigrateAsync(cancellationToken);
            return database;
        }

        public async Task InsertOutboxAsync(CancellationToken cancellationToken, params WakeOutboxItem[] items)
        {
            await using var context = new BridgeDbContext(options);
            foreach (var item in items)
            {
                context.WakeOutbox.Add(new WakeOutbox
                {
                    Id = item.Id,
                    BindingId = item.BindingId,
                    SessionId = item.SessionId,
                    SnapshotFingerprint = item.SnapshotFingerprint,
                    Skill = item.Skill,
                    OpenClawTarget = item.OpenClawTarget,
                    InputJson = item.Input.ToJsonString(),
                    ContractVersion = item.ContractVersion,
                    IdempotencyKey = item.IdempotencyKey,
                    AttemptCount = item.AttemptCount,
                    CreatedAtUtc = item.CreatedAtUtc,
                    NextAttemptAtUtc = item.NextAttemptAtUtc,
                    Status = item.Status,
                    ReplayCount = item.ReplayCount,
                    Version = 1
                });
            }

            await context.SaveChangesAsync(cancellationToken);
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch (IOException)
            {
            }
        }

        private sealed class TestContextFactory(DbContextOptions<BridgeDbContext> options)
            : IDbContextFactory<BridgeDbContext>
        {
            public BridgeDbContext CreateDbContext() => new(options);

            public Task<BridgeDbContext> CreateDbContextAsync(CancellationToken cancellationToken = default) =>
                Task.FromResult(new BridgeDbContext(options));
        }
    }
}
