using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DrasiWake.Persistence.SonnetDB.Migrations
{
    /// <inheritdoc />
    public partial class AddOutboxAuditColumns : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<DateTimeOffset>(
                name: "FirstDispatchedAtUtc",
                table: "WakeOutbox",
                type: "DATETIME",
                nullable: true);

            migrationBuilder.AddColumn<DateTimeOffset>(
                name: "LastReplayedAtUtc",
                table: "WakeOutbox",
                type: "DATETIME",
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "ReplayCount",
                table: "WakeOutbox",
                type: "INT",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<Guid>(
                name: "SupersededByOutboxId",
                table: "WakeOutbox",
                type: "STRING",
                maxLength: 64,
                nullable: true);

            migrationBuilder.CreateIndex(
                name: "IX_WakeOutbox_Status_CreatedAtUtc",
                table: "WakeOutbox",
                columns: new[] { "Status", "CreatedAtUtc" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_WakeOutbox_Status_CreatedAtUtc",
                table: "WakeOutbox");

            migrationBuilder.DropColumn(
                name: "FirstDispatchedAtUtc",
                table: "WakeOutbox");

            migrationBuilder.DropColumn(
                name: "LastReplayedAtUtc",
                table: "WakeOutbox");

            migrationBuilder.DropColumn(
                name: "ReplayCount",
                table: "WakeOutbox");

            migrationBuilder.DropColumn(
                name: "SupersededByOutboxId",
                table: "WakeOutbox");
        }
    }
}
