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

namespace Eswatini.Health.Api.Models.DTOs.Dashboard;

public class TBArtDashboardDto
{
    public DateTime AsOfDate { get; set; }
    public int TotalOnArt { get; set; }
    public TPTMetricsDto TPTMetrics { get; set; } = new();
    public DateTime LastUpdated { get; set; }
}

public class TPTMetricsDto
{
    public int Eligible { get; set; }
    public int EligibleOnArt { get; set; }
    public int EligibleNotOnArt { get; set; }
    
    public int Started { get; set; }
    public int StartedOnArt { get; set; }
    public int StartedNotOnArt { get; set; }
    
    public int Completed { get; set; }
    public int CompletedOnArt { get; set; }
    public int CompletedNotOnArt { get; set; }
    
    public decimal TptCoverageAmongArt { get; set; }
    public decimal TptCoverageOverall { get; set; }
}

public class TBCascadeArtDto
{
    public DateTime AsOfDate { get; set; }
    public TPTArtCascadeDto OnArt { get; set; } = new();
    public TPTArtCascadeDto NotOnArt { get; set; } = new();
}

public class TPTArtCascadeDto
{
    public int Eligible { get; set; }
    public int Started { get; set; }
    public int Completed { get; set; }
    public decimal InitiationRate { get; set; }
    public decimal CompletionRate { get; set; }
}