using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace EcomAPI.Migrations.Write
{
    /// <inheritdoc />
    public partial class AddOutboxTraceContext : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "TraceParent",
                table: "OutboxMessages",
                type: "TEXT",
                maxLength: 55,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "TraceState",
                table: "OutboxMessages",
                type: "TEXT",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "TraceParent",
                table: "OutboxMessages");

            migrationBuilder.DropColumn(
                name: "TraceState",
                table: "OutboxMessages");
        }
    }
}
