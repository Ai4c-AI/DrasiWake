using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DrasiWake.Persistence.SonnetDB.Migrations
{
    /// <inheritdoc />
    public partial class InitialBridgeState : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "KeyMappings",
                columns: table => new
                {
                    ContractScope = table.Column<string>(type: "STRING", maxLength: 512, nullable: false),
                    CanonicalIdentity = table.Column<string>(type: "STRING", maxLength: 2048, nullable: false),
                    SessionId = table.Column<string>(type: "STRING", maxLength: 2048, nullable: false),
                    State = table.Column<string>(type: "STRING", maxLength: 32, nullable: false),
                    UpdatedAtUtc = table.Column<DateTimeOffset>(type: "DATETIME", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_KeyMappings", x => new { x.ContractScope, x.CanonicalIdentity });
                });

            migrationBuilder.CreateTable(
                name: "SnapshotCheckpoints",
                columns: table => new
                {
                    BindingId = table.Column<string>(type: "STRING", maxLength: 512, nullable: false),
                    SessionId = table.Column<string>(type: "STRING", maxLength: 2048, nullable: false),
                    Fingerprint = table.Column<string>(type: "STRING", maxLength: 128, nullable: false),
                    AcceptedAtUtc = table.Column<DateTimeOffset>(type: "DATETIME", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SnapshotCheckpoints", x => new { x.BindingId, x.SessionId });
                });

            migrationBuilder.CreateTable(
                name: "Subscriptions",
                columns: table => new
                {
                    QueryKey = table.Column<string>(type: "STRING", maxLength: 2048, nullable: false),
                    ServerUri = table.Column<string>(type: "STRING", maxLength: 2048, nullable: false),
                    InstanceId = table.Column<string>(type: "STRING", maxLength: 512, nullable: true),
                    QueryId = table.Column<string>(type: "STRING", maxLength: 512, nullable: false),
                    IsConnected = table.Column<bool>(type: "BOOL", nullable: false),
                    LastConnectedAtUtc = table.Column<DateTimeOffset>(type: "DATETIME", nullable: true),
                    LastReconciledAtUtc = table.Column<DateTimeOffset>(type: "DATETIME", nullable: true),
                    LastErrorCode = table.Column<string>(type: "STRING", maxLength: 256, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Subscriptions", x => x.QueryKey);
                });

            migrationBuilder.CreateTable(
                name: "WakeOutbox",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "STRING", nullable: false),
                    BindingId = table.Column<string>(type: "STRING", maxLength: 512, nullable: false),
                    SessionId = table.Column<string>(type: "STRING", maxLength: 2048, nullable: false),
                    SnapshotFingerprint = table.Column<string>(type: "STRING", maxLength: 128, nullable: false),
                    Skill = table.Column<string>(type: "STRING", maxLength: 512, nullable: false),
                    InputJson = table.Column<string>(type: "STRING", nullable: false),
                    ContractVersion = table.Column<string>(type: "STRING", maxLength: 256, nullable: false),
                    IdempotencyKey = table.Column<string>(type: "STRING", maxLength: 512, nullable: false),
                    AttemptCount = table.Column<int>(type: "INT", nullable: false),
                    CreatedAtUtc = table.Column<DateTimeOffset>(type: "DATETIME", nullable: false),
                    NextAttemptAtUtc = table.Column<DateTimeOffset>(type: "DATETIME", nullable: false),
                    Status = table.Column<int>(type: "INT", nullable: false),
                    InvocationId = table.Column<string>(type: "STRING", maxLength: 512, nullable: true),
                    TraceId = table.Column<string>(type: "STRING", maxLength: 256, nullable: true),
                    LastErrorCode = table.Column<string>(type: "STRING", maxLength: 256, nullable: true),
                    RetainUntilUtc = table.Column<DateTimeOffset>(type: "DATETIME", nullable: true),
                    Version = table.Column<int>(type: "INT", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_WakeOutbox", x => x.Id);
                });

            migrationBuilder.CreateIndex(
                name: "IX_Subscriptions_ServerUri_InstanceId_QueryId",
                table: "Subscriptions",
                columns: new[] { "ServerUri", "InstanceId", "QueryId" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_WakeOutbox_IdempotencyKey",
                table: "WakeOutbox",
                column: "IdempotencyKey",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_WakeOutbox_NextAttemptAtUtc_CreatedAtUtc_Id",
                table: "WakeOutbox",
                columns: new[] { "NextAttemptAtUtc", "CreatedAtUtc", "Id" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "KeyMappings");

            migrationBuilder.DropTable(
                name: "SnapshotCheckpoints");

            migrationBuilder.DropTable(
                name: "Subscriptions");

            migrationBuilder.DropTable(
                name: "WakeOutbox");
        }
    }
}
