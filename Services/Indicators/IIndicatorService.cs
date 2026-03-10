using Eswatini.Health.Api.Models.DTOs.Indicators;
using Eswatini.Health.Api.Models.DTOs.Dashboard;

namespace Eswatini.Health.Api.Services.Indicators;

public interface IIndicatorService
{
    Task<List<IndicatorValueDto>> GetIndicatorDataAsync(IndicatorDataRequest request);
    Task<Dictionary<string, List<IndicatorValueDto>>> GetIndicatorTrendsAsync(
        string[] indicators, DateTime startDate, DateTime endDate, string periodType = "daily");
    Task<bool> HasDataForPeriodAsync(string period);
}

// HIV Specific
public interface IHIVIndicatorService : IIndicatorService
{
    // Existing methods
    Task<int> GetTotalOnArtAsync(DateTime date, int? regionId = null);
    Task<int> GetNewOnArtAsync(DateTime startDate, DateTime endDate, int? regionId = null);
    Task<(int Tested, int Suppressed)> GetViralLoadOutcomesAsync(DateTime date, int? regionId = null);
    Task<Dictionary<string, int>> GetBreakdownBySexAsync(string indicator, DateTime date, int? regionId = null);
    Task<Dictionary<string, int>> GetBreakdownByAgeGroupAsync(string indicator, DateTime date, int? regionId = null);
    
    // New methods for quarterly period dashboard
    Task<DashboardSummaryDto> GetDashboardSummaryAsync(string period);
    Task<DetailedDashboardDto> GetDetailedDashboardAsync(string period);
    Task<List<RegionalBreakdownDto>> GetBreakdownByRegionAsync(string period);
    Task<List<SexBasedBreakdownDto>> GetBreakdownBySexAsync(string period, string? regionCode = null);
    Task<List<AgeGroupBreakdownDto>> GetBreakdownByAgeGroupAsync(string period, string? regionCode = null);
    Task<decimal> GetIndicatorValueAsync(string indicatorCode, string period, string? regionCode = null, string? ageGroup = null, string? sex = null);
}

// Prevention Specific
public interface IPreventionIndicatorService : IIndicatorService
{
    Task<int> GetHIVTestsAsync(DateTime startDate, DateTime endDate, int? regionId = null);
    Task<int> GetHIVPositivesAsync(DateTime startDate, DateTime endDate, int? regionId = null);
    Task<int> GetPrEPInitiationsAsync(DateTime startDate, DateTime endDate, int? regionId = null);
    Task<int> GetPrEPSeroconversionsAsync(DateTime startDate, DateTime endDate, int? regionId = null);
    Task<Dictionary<string, int>> GetPrEPByMethodAsync(DateTime startDate, DateTime endDate, int? regionId = null);
}

// TB Specific
public interface ITBIndicatorService : IIndicatorService
{
    // TPT Cascade metrics
    Task<int> GetTPTEligibleAsync(DateTime startDate, DateTime endDate, int? regionId = null);
    Task<int> GetTPTStartedAsync(DateTime startDate, DateTime endDate, int? regionId = null);
    Task<int> GetTPTCompletedAsync(DateTime startDate, DateTime endDate, int? regionId = null);
    Task<int> GetTPTStoppedAsync(DateTime startDate, DateTime endDate, int? regionId = null);
    Task<int> GetTPTTransferredOutAsync(DateTime startDate, DateTime endDate, int? regionId = null);
    Task<int> GetTPTDiedAsync(DateTime startDate, DateTime endDate, int? regionId = null);
    Task<int> GetTPTSelfStoppedAsync(DateTime startDate, DateTime endDate, int? regionId = null);
    Task<int> GetTPTStoppedByClinicianAsync(DateTime startDate, DateTime endDate, int? regionId = null);
    Task<int> GetTPTLTFUAsync(DateTime startDate, DateTime endDate, int? regionId = null);
    Task<TBArtDashboardDto> GetTBArtDashboardAsync(DateTime? asOfDate = null, int? regionId = null);
    Task<TBCascadeArtDto> GetTPTCascadeWithArtAsync(DateTime? asOfDate = null, int? regionId = null);
    Task<int> GetTPTByPopulationTypeAsync(string indicator, DateTime startDate, DateTime endDate, string populationType, int? regionId = null);
}
