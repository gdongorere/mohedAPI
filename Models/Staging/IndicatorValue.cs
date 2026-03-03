namespace Eswatini.Health.Api.Models.Staging;

public abstract class IndicatorValueBase
{
    public long Id { get; set; }
    public string Indicator { get; set; } = string.Empty;
    public int RegionId { get; set; }
    public DateTime VisitDate { get; set; }
    public string AgeGroup { get; set; } = string.Empty;
    public string Sex { get; set; } = string.Empty;
    public string? PopulationType { get; set; }
    public int Value { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
}

// Specific classes for each table (EF Core needs concrete classes)
public class IndicatorValueHIV : IndicatorValueBase { }
public class IndicatorValuePrevention : IndicatorValueBase { }
public class IndicatorValueTB : IndicatorValueBase 
{
    public string? TBType { get; set; }
}