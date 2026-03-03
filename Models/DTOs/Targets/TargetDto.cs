namespace Eswatini.Health.Api.Models.DTOs.Targets;

public class TargetDto
{
    public int Id { get; set; }
    public string Indicator { get; set; } = string.Empty;
    public int? RegionId { get; set; }
    public string RegionName { get; set; } = string.Empty; // "National" if RegionId is null
    public int Year { get; set; }
    public int? Quarter { get; set; }
    public int? Month { get; set; }
    public decimal TargetValue { get; set; }
    public string TargetType { get; set; } = "number";
    public string? Notes { get; set; }
    public string Period { get; set; } = string.Empty; // "2025", "2025-Q1", "2025-03"
    public string Level { get; set; } = "National"; // "National" or "Regional"
}

public class CreateTargetRequest
{
    public string Indicator { get; set; } = string.Empty;
    public int? RegionId { get; set; }  // NULL for National
    public int Year { get; set; }
    public int? Quarter { get; set; }
    public int? Month { get; set; }
    public decimal TargetValue { get; set; }
    public string TargetType { get; set; } = "number";
    public string? Notes { get; set; }
    public string? AgeGroup { get; set; }
    public string? Sex { get; set; }
    public string? PopulationType { get; set; }
}

public class UpdateTargetRequest
{
    public decimal TargetValue { get; set; }
    public string? Notes { get; set; }
}

public class TargetSummaryDto
{
    public string Indicator { get; set; } = string.Empty;
    public int Year { get; set; }
    public decimal? NationalTarget { get; set; }
    public Dictionary<int, decimal> RegionalTargets { get; set; } = new(); // RegionId -> Target
    public decimal? Achieved { get; set; }
    public decimal? PercentageAchieved { get; set; }
}