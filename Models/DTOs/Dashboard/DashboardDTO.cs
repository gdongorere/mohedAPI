namespace Eswatini.Health.Api.Models.DTOs.Dashboard;

public class DashboardSummaryDto
{
    public DateTime AsOfDate { get; set; }
    public List<MetricDto> Metrics { get; set; } = new();
    public List<ChartDataDto> Charts { get; set; } = new();
    public DateTime LastUpdated { get; set; }
}

public class MetricDto
{
    public string Indicator { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public decimal Value { get; set; }
    public decimal? Target { get; set; }
    public decimal? PercentageOfTarget { get; set; }
    public string Unit { get; set; } = string.Empty;
    public string Trend { get; set; } = "stable"; // up, down, stable
    public decimal? PreviousValue { get; set; }
    public decimal? PercentChange { get; set; }
}

public class ChartDataDto
{
    public string Title { get; set; } = string.Empty;
    public string ChartType { get; set; } = "line"; // line, bar, pie
    public List<string> Labels { get; set; } = new();
    public List<ChartDatasetDto> Datasets { get; set; } = new();
}

public class ChartDatasetDto
{
    public string Label { get; set; } = string.Empty;
    public List<decimal> Data { get; set; } = new();
    public string BorderColor { get; set; } = string.Empty;
    public string BackgroundColor { get; set; } = string.Empty;
}

public class BreakdownDto<T>
{
    public string Dimension { get; set; } = string.Empty;
    public List<T> Items { get; set; } = new();
}

public class SexBreakdownDto
{
    public string Sex { get; set; } = string.Empty;
    public int Total { get; set; }
    public decimal Percentage { get; set; }
}

public class AgeGroupBreakdownDto
{
    public string AgeGroup { get; set; } = string.Empty;
    public int Total { get; set; }
    public decimal Percentage { get; set; }
}

public class RegionalBreakdownDto
{
    public int RegionId { get; set; }
    public string RegionName { get; set; } = string.Empty;
    public int Total { get; set; }
    public decimal Percentage { get; set; }
}