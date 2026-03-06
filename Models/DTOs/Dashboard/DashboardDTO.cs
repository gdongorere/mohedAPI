namespace Eswatini.Health.Api.Models.DTOs.Dashboard;

// Main Dashboard Summary
public class DashboardSummaryDto
{
    public string Period { get; set; } = string.Empty;
    public DateTime ReportingDate { get; set; }
    public DashboardMetricsDto Summary { get; set; } = new();
    public DateTime LastUpdated { get; set; }
    public string BatchId { get; set; } = string.Empty;
}

public class DashboardMetricsDto
{
    public int TotalOnArt { get; set; }
    public int VlTested { get; set; }
    public int VlSuppressed { get; set; }
    public int VlUnsuppressed { get; set; }
    public int VlUndetectable { get; set; }
    public decimal VlSuppressionRate { get; set; }
    public decimal VlCoverageRate { get; set; }
    public decimal VlUndetectableRate { get; set; }
    public decimal UnsuppressedRate { get; set; }
}

// Detailed Dashboard
public class DetailedDashboardDto
{
    public string Period { get; set; } = string.Empty;
    public DateTime ReportingDate { get; set; }
    public SummaryMetricsDto Summary { get; set; } = new();
    public List<SexBasedBreakdownDto> BySex { get; set; } = new();
    public List<AgeGroupBreakdownDto> ByAgeGroup { get; set; } = new();
    public List<RegionalBreakdownDto> ByRegion { get; set; } = new();
    public DateTime LastUpdated { get; set; }
    public string BatchId { get; set; } = string.Empty;
}

public class SummaryMetricsDto
{
    public int TotalOnArt { get; set; }
    public int VlTested { get; set; }
    public int VlSuppressed { get; set; }
    public int VlUnsuppressed { get; set; }
    public int VlUndetectable { get; set; }
    public decimal SuppressionRate { get; set; }
    public decimal CoverageRate { get; set; }
    public decimal UnsuppressedRate { get; set; }
    public decimal UndetectableRate { get; set; }
}

// Breakdown DTOs
public class SexBasedBreakdownDto
{
    public string Sex { get; set; } = string.Empty;
    public int OnArt { get; set; }
    public int Tested { get; set; }
    public int Suppressed { get; set; }
    public int Unsuppressed { get; set; }
    public decimal SuppressionRate { get; set; }
    public decimal TestCoverage { get; set; }
}

public class AgeGroupBreakdownDto
{
    public string AgeGroup { get; set; } = string.Empty;
    public string AgeGroupCode { get; set; } = string.Empty;
    public int OnArt { get; set; }
    public int Tested { get; set; }
    public int Suppressed { get; set; }
    public int Unsuppressed { get; set; }
    public decimal SuppressionRate { get; set; }
    public decimal TestCoverage { get; set; }
}

public class RegionalBreakdownDto
{
    public string RegionCode { get; set; } = string.Empty;
    public string RegionName { get; set; } = string.Empty;
    public int OnArt { get; set; }
    public int Tested { get; set; }
    public int Suppressed { get; set; }
    public decimal SuppressionRate { get; set; }
    public decimal TestCoverage { get; set; }
}