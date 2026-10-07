using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DrasiWake.Persistence.SonnetDB.Migrations
{
    /// <inheritdoc />
    public partial class AddRaftProjectionState : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "RaftProjectionState",
                columns: table => new
                {
                    Id = table.Column<int>(type: "INT", nullable: false),
                    LastAppliedIndex = table.Column<long>(type: "INT", nullable: false),
                    LastAppliedCommandId = table.Column<string>(type: "STRING", maxLength: 64, nullable: true),
                    ConfigurationFingerprint = table.Column<string>(type: "STRING", maxLength: 128, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_RaftProjectionState", x => x.Id);
                    table.CheckConstraint("CK_RaftProjectionState_LastAppliedIndex_NonNegative", "\"LastAppliedIndex\" >= 0");
                    table.CheckConstraint("CK_RaftProjectionState_Singleton", "\"Id\" = 1");
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "RaftProjectionState");
        }
    }
}
