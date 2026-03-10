namespace Eswatini.Health.Api.Models.DTOs.Dashboard;

public class TBDashboardDto
{
    public DateTime AsOfDate { get; set; }
    public TPMonthlyMetricsDto Monthly { get; set; } = new();
    public TPCumulativeMetricsDto Cumulative { get; set; } = new();
    public Dictionary<string, int> BySex { get; set; } = new();
    public Dictionary<string, int> ByAgeGroup { get; set; } = new();
    public List<RegionalBreakdownDto> ByRegion { get; set; } = new();
    public DateTime LastUpdated { get; set; }
}

public class TPMonthlyMetricsDto
{
    public int Eligible { get; set; }
    public int Started { get; set; }
    public int Completed { get; set; }
    public decimal InitiationRate { get; set; }
}

public class TPCumulativeMetricsDto
{
    public int TotalStarted { get; set; }
    public int TotalCompleted { get; set; }
    public decimal OverallCompletionRate { get; set; }
}

public class TBCascadeDto
{
    public DateTime AsOfDate { get; set; }
    public int Eligible { get; set; }
    public int Started { get; set; }
    public int Completed { get; set; }
    public decimal InitiationRate { get; set; }
    public decimal CompletionRate { get; set; }
    public TPTOutcomesDto Outcomes { get; set; } = new();
}

public class TPTOutcomesDto
{
    public int Stopped { get; set; }
    public int TransferredOut { get; set; }
    public int Died { get; set; }
    public int SelfStopped { get; set; }
    public int StoppedByClinician { get; set; }
    public int LTFU { get; set; }
}