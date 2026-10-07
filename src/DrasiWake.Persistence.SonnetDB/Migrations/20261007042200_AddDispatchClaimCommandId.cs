using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DrasiWake.Persistence.SonnetDB.Migrations;

public sealed partial class AddDispatchClaimCommandId : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<string>(
            name: "ClaimCommandId",
            table: "WakeOutbox",
            type: "STRING",
            maxLength: 64,
            nullable: true);
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropColumn(
            name: "ClaimCommandId",
            table: "WakeOutbox");
    }
}
