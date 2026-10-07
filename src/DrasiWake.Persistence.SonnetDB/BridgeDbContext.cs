using DrasiWake.Persistence.SonnetDB.Entities;
using Microsoft.EntityFrameworkCore;

namespace DrasiWake.Persistence.SonnetDB;

public sealed class BridgeDbContext(DbContextOptions<BridgeDbContext> options) : DbContext(options)
{
    public DbSet<SubscriptionState> Subscriptions => Set<SubscriptionState>();
    public DbSet<SnapshotCheckpoint> SnapshotCheckpoints => Set<SnapshotCheckpoint>();
    public DbSet<KeyMapping> KeyMappings => Set<KeyMapping>();
    public DbSet<WakeOutbox> WakeOutbox => Set<WakeOutbox>();
    public DbSet<RaftProjectionState> RaftProjectionStates => Set<RaftProjectionState>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SubscriptionState>(entity =>
        {
            entity.ToTable("Subscriptions");
            entity.HasKey(state => state.QueryKey);
            entity.Property(state => state.QueryKey).HasMaxLength(2048);
            entity.Property(state => state.ServerUri).HasMaxLength(2048);
            entity.Property(state => state.InstanceId).HasMaxLength(512);
            entity.Property(state => state.QueryId).HasMaxLength(512);
            entity.Property(state => state.LastErrorCode).HasMaxLength(256);
            entity.HasIndex(state => new { state.ServerUri, state.InstanceId, state.QueryId }).IsUnique();
        });

        modelBuilder.Entity<SnapshotCheckpoint>(entity =>
        {
            entity.ToTable("SnapshotCheckpoints");
            entity.HasKey(checkpoint => new { checkpoint.BindingId, checkpoint.SessionId });
            entity.Property(checkpoint => checkpoint.BindingId).HasMaxLength(512);
            entity.Property(checkpoint => checkpoint.SessionId).HasMaxLength(2048);
            entity.Property(checkpoint => checkpoint.Fingerprint).HasMaxLength(128);
        });

        modelBuilder.Entity<KeyMapping>(entity =>
        {
            entity.ToTable("KeyMappings");
            entity.HasKey(mapping => new { mapping.ContractScope, mapping.CanonicalIdentity });
            entity.Property(mapping => mapping.ContractScope).HasMaxLength(512);
            entity.Property(mapping => mapping.CanonicalIdentity).HasMaxLength(2048);
            entity.Property(mapping => mapping.SessionId).HasMaxLength(2048);
            entity.Property(mapping => mapping.State).HasMaxLength(32);
        });

        modelBuilder.Entity<WakeOutbox>(entity =>
        {
            entity.ToTable("WakeOutbox");
            entity.HasKey(item => item.Id);
            entity.Property(item => item.BindingId).HasMaxLength(512);
            entity.Property(item => item.SessionId).HasMaxLength(2048);
            entity.Property(item => item.SnapshotFingerprint).HasMaxLength(128);
            entity.Property(item => item.Skill).HasMaxLength(512);
            entity.Property(item => item.OpenClawTarget).HasMaxLength(512);
            entity.Property(item => item.ContractVersion).HasMaxLength(256);
            entity.Property(item => item.IdempotencyKey).HasMaxLength(512);
            entity.Property(item => item.InvocationId).HasMaxLength(512);
            entity.Property(item => item.TraceId).HasMaxLength(256);
            entity.Property(item => item.LastErrorCode).HasMaxLength(256);
            entity.Property(item => item.ClaimCommandId).HasMaxLength(64);
            entity.Property(item => item.Version).IsConcurrencyToken();
            entity.HasIndex(item => new { item.NextAttemptAtUtc, item.CreatedAtUtc, item.Id });
            entity.HasIndex(item => item.IdempotencyKey).IsUnique();
        });

        modelBuilder.Entity<RaftProjectionState>(entity =>
        {
            entity.ToTable("RaftProjectionState", table =>
            {
                table.HasCheckConstraint(
                    "CK_RaftProjectionState_LastAppliedIndex_NonNegative",
                    "\"LastAppliedIndex\" >= 0");
                table.HasCheckConstraint("CK_RaftProjectionState_Singleton", "\"Id\" = 1");
            });
            entity.HasKey(state => state.Id);
            entity.Property(state => state.Id).ValueGeneratedNever();
            entity.Property(state => state.LastAppliedIndex).IsRequired();
            entity.Property(state => state.LastAppliedCommandId).HasMaxLength(64);
            entity.Property(state => state.ConfigurationFingerprint).HasMaxLength(128);
        });
    }
}