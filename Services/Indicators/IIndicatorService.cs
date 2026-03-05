using Eswatini.Health.Api.Models.DTOs.Indicators;

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
    Task<int> GetTotalOnArtAsync(DateTime date, int? regionId = null);
    Task<int> GetNewOnArtAsync(DateTime startDate, DateTime endDate, int? regionId = null);
    Task<(int Tested, int Suppressed)> GetViralLoadOutcomesAsync(DateTime date, int? regionId = null);
    Task<Dictionary<string, int>> GetBreakdownBySexAsync(string indicator, DateTime date, int? regionId = null);
    Task<Dictionary<string, int>> GetBreakdownByAgeGroupAsync(string indicator, DateTime date, int? regionId = null);
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

// TB Specific (for future)
public interface ITBIndicatorService : IIndicatorService
{
    // Will be implemented when TB data is available
}