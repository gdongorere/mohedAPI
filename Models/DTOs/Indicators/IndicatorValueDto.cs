namespace Eswatini.Health.Api.Models.DTOs.Indicators;

public class IndicatorValueDto
{
    public string Indicator { get; set; } = string.Empty;
    public int RegionId { get; set; }
    public string RegionName { get; set; } = string.Empty;
    public DateTime VisitDate { get; set; }
    public string Period { get; set; } = string.Empty; // YYYY-MM-DD, YYYY-MM, YYYY-Qn
    public string AgeGroup { get; set; } = string.Empty;
    public string Sex { get; set; } = string.Empty;
    public string? PopulationType { get; set; }
    public string? TBType { get; set; }
    public int Value { get; set; }
    public DateTime LastUpdated { get; set; }
}

public class IndicatorDataRequest
{
    public string[]? Indicators { get; set; }
    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }
    public int? RegionId { get; set; }
    public string? AgeGroup { get; set; }
    public string? Sex { get; set; }
    public string? PopulationType { get; set; }
    public string? TBType { get; set; }
    public string? PeriodType { get; set; } = "daily"; // daily, monthly, quarterly
}