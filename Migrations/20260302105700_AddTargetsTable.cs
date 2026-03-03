using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Eswatini.Health.Api.Migrations
{
    /// <inheritdoc />
    public partial class AddTargetsTable : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "IndicatorTargets",
                columns: table => new
                {
                    Id = table.Column<int>(type: "int", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    Indicator = table.Column<string>(type: "nvarchar(50)", maxLength: 50, nullable: false),
                    RegionId = table.Column<int>(type: "int", nullable: false),
                    Year = table.Column<int>(type: "int", nullable: false),
                    Quarter = table.Column<int>(type: "int", nullable: true),
                    Month = table.Column<int>(type: "int", nullable: true),
                    TargetValue = table.Column<decimal>(type: "decimal(18,2)", precision: 18, scale: 2, nullable: false),
                    TargetType = table.Column<string>(type: "nvarchar(20)", maxLength: 20, nullable: false, defaultValue: "number"),
                    Notes = table.Column<string>(type: "nvarchar(500)", maxLength: 500, nullable: true),
                    AgeGroup = table.Column<string>(type: "nvarchar(20)", maxLength: 20, nullable: true),
                    Sex = table.Column<string>(type: "nvarchar(10)", maxLength: 10, nullable: true),
                    PopulationType = table.Column<string>(type: "nvarchar(50)", maxLength: 50, nullable: true),
                    CreatedAt = table.Column<DateTime>(type: "datetime2", nullable: false),
                    UpdatedAt = table.Column<DateTime>(type: "datetime2", nullable: false),
                    CreatedBy = table.Column<string>(type: "nvarchar(36)", maxLength: 36, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_IndicatorTargets", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "IndicatorValues_HIV",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    Indicator = table.Column<string>(type: "nvarchar(50)", maxLength: 50, nullable: false),
                    RegionId = table.Column<int>(type: "int", nullable: false),
                    VisitDate = table.Column<DateTime>(type: "datetime2", nullable: false),
                    AgeGroup = table.Column<string>(type: "nvarchar(20)", maxLength: 20, nullable: false),
                    Sex = table.Column<string>(type: "nvarchar(10)", maxLength: 10, nullable: false),
                    PopulationType = table.Column<string>(type: "nvarchar(50)", maxLength: 50, nullable: true),
                    Value = table.Column<int>(type: "int", nullable: false),
                    CreatedAt = table.Column<DateTime>(type: "datetime2", nullable: false),
                    UpdatedAt = table.Column<DateTime>(type: "datetime2", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_IndicatorValues_HIV", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "IndicatorValues_Prevention",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    Indicator = table.Column<string>(type: "nvarchar(50)", maxLength: 50, nullable: false),
                    RegionId = table.Column<int>(type: "int", nullable: false),
                    VisitDate = table.Column<DateTime>(type: "datetime2", nullable: false),
                    AgeGroup = table.Column<string>(type: "nvarchar(20)", maxLength: 20, nullable: false),
                    Sex = table.Column<string>(type: "nvarchar(10)", maxLength: 10, nullable: false),
                    PopulationType = table.Column<string>(type: "nvarchar(50)", maxLength: 50, nullable: true),
                    Value = table.Column<int>(type: "int", nullable: false),
                    CreatedAt = table.Column<DateTime>(type: "datetime2", nullable: false),
                    UpdatedAt = table.Column<DateTime>(type: "datetime2", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_IndicatorValues_Prevention", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "IndicatorValues_TB",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    TBType = table.Column<string>(type: "nvarchar(50)", maxLength: 50, nullable: true),
                    Indicator = table.Column<string>(type: "nvarchar(50)", maxLength: 50, nullable: false),
                    RegionId = table.Column<int>(type: "int", nullable: false),
                    VisitDate = table.Column<DateTime>(type: "datetime2", nullable: false),
                    AgeGroup = table.Column<string>(type: "nvarchar(20)", maxLength: 20, nullable: false),
                    Sex = table.Column<string>(type: "nvarchar(10)", maxLength: 10, nullable: false),
                    PopulationType = table.Column<string>(type: "nvarchar(50)", maxLength: 50, nullable: true),
                    Value = table.Column<int>(type: "int", nullable: false),
                    CreatedAt = table.Column<DateTime>(type: "datetime2", nullable: false),
                    UpdatedAt = table.Column<DateTime>(type: "datetime2", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_IndicatorValues_TB", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Users",
                columns: table => new
                {
                    Id = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Email = table.Column<string>(type: "nvarchar(255)", maxLength: 255, nullable: false),
                    Name = table.Column<string>(type: "nvarchar(255)", maxLength: 255, nullable: false),
                    Surname = table.Column<string>(type: "nvarchar(255)", maxLength: 255, nullable: false),
                    Role = table.Column<string>(type: "nvarchar(50)", maxLength: 50, nullable: false, defaultValue: "viewer"),
                    PasswordHash = table.Column<string>(type: "nvarchar(500)", maxLength: 500, nullable: false),
                    LastLoginAt = table.Column<DateTime>(type: "datetime2", nullable: true),
                    IsActive = table.Column<bool>(type: "bit", nullable: false, defaultValue: true),
                    CreatedAt = table.Column<DateTime>(type: "datetime2", nullable: false),
                    UpdatedAt = table.Column<DateTime>(type: "datetime2", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Users", x => x.Id);
                });

            migrationBuilder.CreateIndex(
                name: "IX_Targets_Lookup",
                table: "IndicatorTargets",
                columns: new[] { "Indicator", "RegionId", "Year", "Quarter", "Month" });

            migrationBuilder.CreateIndex(
                name: "IX_HIV_DateRange",
                table: "IndicatorValues_HIV",
                column: "VisitDate");

            migrationBuilder.CreateIndex(
                name: "IX_HIV_Lookup",
                table: "IndicatorValues_HIV",
                columns: new[] { "Indicator", "RegionId", "VisitDate", "AgeGroup", "Sex", "PopulationType" });

            migrationBuilder.CreateIndex(
                name: "IX_HIV_Updated",
                table: "IndicatorValues_HIV",
                column: "UpdatedAt");

            migrationBuilder.CreateIndex(
                name: "IX_Prevention_DateRange",
                table: "IndicatorValues_Prevention",
                column: "VisitDate");

            migrationBuilder.CreateIndex(
                name: "IX_Prevention_Lookup",
                table: "IndicatorValues_Prevention",
                columns: new[] { "Indicator", "RegionId", "VisitDate", "AgeGroup", "Sex", "PopulationType" });

            migrationBuilder.CreateIndex(
                name: "IX_Prevention_Updated",
                table: "IndicatorValues_Prevention",
                column: "UpdatedAt");

            migrationBuilder.CreateIndex(
                name: "IX_TB_DateRange",
                table: "IndicatorValues_TB",
                column: "VisitDate");

            migrationBuilder.CreateIndex(
                name: "IX_TB_Lookup",
                table: "IndicatorValues_TB",
                columns: new[] { "Indicator", "RegionId", "VisitDate", "AgeGroup", "Sex", "TBType", "PopulationType" });

            migrationBuilder.CreateIndex(
                name: "IX_TB_Updated",
                table: "IndicatorValues_TB",
                column: "UpdatedAt");

            migrationBuilder.CreateIndex(
                name: "IX_Users_Email",
                table: "Users",
                column: "Email",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "IndicatorTargets");

            migrationBuilder.DropTable(
                name: "IndicatorValues_HIV");

            migrationBuilder.DropTable(
                name: "IndicatorValues_Prevention");

            migrationBuilder.DropTable(
                name: "IndicatorValues_TB");

            migrationBuilder.DropTable(
                name: "Users");
        }
    }
}
