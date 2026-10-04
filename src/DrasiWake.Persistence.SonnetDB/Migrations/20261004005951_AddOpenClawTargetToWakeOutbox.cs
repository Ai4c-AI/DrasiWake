using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DrasiWake.Persistence.SonnetDB.Migrations
{
    /// <inheritdoc />
    public partial class AddOpenClawTargetToWakeOutbox : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "OpenClawTarget",
                table: "WakeOutbox",
                type: "STRING",
                maxLength: 512,
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "OpenClawTarget",
                table: "WakeOutbox");
        }
    }
}
