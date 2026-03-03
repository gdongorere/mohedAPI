namespace Eswatini.Health.Api.Models.Targets;

public class IndicatorTarget
{
    public int Id { get; set; }
    public string Indicator { get; set; } = string.Empty;
    public int? RegionId { get; set; }  // NULL = National
    public int Year { get; set; }
    public int? Quarter { get; set; }  // 1-4, NULL means annual or monthly
    public int? Month { get; set; }     // 1-12, NULL means annual or quarterly
    public decimal TargetValue { get; set; }
    public string TargetType { get; set; } = "number"; // "number", "percentage"
    public string? Notes { get; set; }
    
    // Optional demographic breakdowns
    public string? AgeGroup { get; set; }
    public string? Sex { get; set; }
    public string? PopulationType { get; set; }
    
    // Audit fields
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
    public string CreatedBy { get; set; } = string.Empty;
}