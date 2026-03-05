using Microsoft.EntityFrameworkCore;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.DTOs.Indicators;
using Eswatini.Health.Api.Models.Staging;
using Eswatini.Health.Api.Services.Period;

namespace Eswatini.Health.Api.Services.Indicators;

public class HIVIndicatorService : IndicatorServiceBase, IHIVIndicatorService
{
    public HIVIndicatorService(
        StagingDbContext db,
        ILogger<HIVIndicatorService> logger,
        IPeriodService periodService) : base(db, logger, periodService)
    {
    }

    public async Task<List<IndicatorValueDto>> GetIndicatorDataAsync(IndicatorDataRequest request)
    {
        try
        {
            var query = _db.IndicatorValues_HIV.AsQueryable();
            
            // Apply filters
            if (request.Indicators?.Any() == true)
                query = query.Where(x => request.Indicators.Contains(x.Indicator));
            
            if (request.RegionId.HasValue)
                query = query.Where(x => x.RegionId == request.RegionId.Value);
            
            if (request.StartDate.HasValue)
                query = query.Where(x => x.VisitDate >= request.StartDate.Value);
            
            if (request.EndDate.HasValue)
                query = query.Where(x => x.VisitDate <= request.EndDate.Value);
            
            if (!string.IsNullOrEmpty(request.AgeGroup))
                query = query.Where(x => x.AgeGroup == request.AgeGroup);
            
            if (!string.IsNullOrEmpty(request.Sex))
                query = query.Where(x => x.Sex == request.Sex);
            
            if (!string.IsNullOrEmpty(request.PopulationType))
                query = query.Where(x => x.PopulationType == request.PopulationType);

            var results = await query
                .OrderBy(x => x.VisitDate)
                .Select(x => new IndicatorValueDto
                {
                    Indicator = x.Indicator,
                    RegionId = x.RegionId,
                    RegionName = GetRegionName(x.RegionId),
                    VisitDate = x.VisitDate,
                    Period = _periodService.FormatDateAsPeriod(x.VisitDate, request.PeriodType ?? "daily"),
                    AgeGroup = x.AgeGroup,
                    Sex = x.Sex,
                    PopulationType = x.PopulationType,
                    Value = x.Value,
                    LastUpdated = x.UpdatedAt
                })
                .ToListAsync();

            // Aggregate by period if needed (do this in memory after EF query)
            if (request.PeriodType != "daily" && request.PeriodType != null && results.Any())
            {
                results = results
                    .GroupBy(x => new { x.Indicator, x.RegionId, x.Period, x.AgeGroup, x.Sex, x.PopulationType })
                    .Select(g => new IndicatorValueDto
                    {
                        Indicator = g.Key.Indicator,
                        RegionId = g.Key.RegionId,
                        RegionName = GetRegionName(g.Key.RegionId),
                        VisitDate = g.Min(x => x.VisitDate),
                        Period = g.Key.Period,
                        AgeGroup = g.Key.AgeGroup,
                        Sex = g.Key.Sex,
                        PopulationType = g.Key.PopulationType,
                        Value = g.Sum(x => x.Value),
                        LastUpdated = g.Max(x => x.LastUpdated)
                    })
                    .OrderBy(x => x.VisitDate)
                    .ToList();
            }

            return results;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting HIV indicator data");
            return new List<IndicatorValueDto>();
        }
    }

    public async Task<Dictionary<string, List<IndicatorValueDto>>> GetIndicatorTrendsAsync(
        string[] indicators, DateTime startDate, DateTime endDate, string periodType = "daily")
    {
        try
        {
            var request = new IndicatorDataRequest
            {
                Indicators = indicators,
                StartDate = startDate,
                EndDate = endDate,
                PeriodType = periodType
            };

            var data = await GetIndicatorDataAsync(request);
            
            return data
                .GroupBy(x => x.Indicator)
                .ToDictionary(g => g.Key, g => g.ToList());
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting HIV indicator trends");
            return new Dictionary<string, List<IndicatorValueDto>>();
        }
    }

    public async Task<bool> HasDataForPeriodAsync(string period)
    {
        try
        {
            var dateRange = _periodService.GetDateRangeForPeriod(period);
            
            return await _db.IndicatorValues_HIV
                .AnyAsync(x => x.VisitDate >= dateRange.StartDate && x.VisitDate <= dateRange.EndDate);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking data for period {Period}", period);
            return false;
        }
    }

    public async Task<int> GetTotalOnArtAsync(DateTime date, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_HIV
                .Where(x => x.Indicator == "TX_CURR");

            // For TX_CURR, we need the latest value for each patient/dimension
            // In a real implementation, you'd use the view vw_LatestIndicatorValues_HIV
            // For now, we'll sum all values and assume they're already aggregated correctly
            
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);

            // If a specific date is provided, filter by date
            // Note: This assumes VisitDate is the reporting period
            // For TX_CURR, we typically want the most recent value
            // This is a simplified approach - in production, use a view with ROW_NUMBER()
            var total = await query.SumAsync(x => (int?)x.Value) ?? 0;
            
            _logger.LogDebug("GetTotalOnArtAsync for date {Date}, region {RegionId}: {Total}", 
                date, regionId, total);
            
            return total;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting total on ART for date {Date}, region {RegionId}", date, regionId);
            return 0;
        }
    }

    public async Task<int> GetNewOnArtAsync(DateTime startDate, DateTime endDate, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_HIV
                .Where(x => x.Indicator == "TX_NEW" 
                    && x.VisitDate >= startDate 
                    && x.VisitDate <= endDate);
            
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);

            return await query.SumAsync(x => x.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting new on ART");
            return 0;
        }
    }

    public async Task<(int Tested, int Suppressed)> GetViralLoadOutcomesAsync(DateTime date, int? regionId = null)
    {
        try
        {
            var tested = await _db.IndicatorValues_HIV
                .Where(x => x.Indicator == "TX_VL_TESTED")
                .ApplyRegionFilter(regionId)
                .SumAsync(x => (int?)x.Value) ?? 0;
                
            var suppressed = await _db.IndicatorValues_HIV
                .Where(x => x.Indicator == "TX_VL_SUPPRESSED")
                .ApplyRegionFilter(regionId)
                .SumAsync(x => (int?)x.Value) ?? 0;

            _logger.LogDebug("GetViralLoadOutcomesAsync for date {Date}, region {RegionId}: Tested={Tested}, Suppressed={Suppressed}", 
                date, regionId, tested, suppressed);

            return (tested, suppressed);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting viral load outcomes");
            return (0, 0);
        }
    }

    public async Task<Dictionary<string, int>> GetBreakdownBySexAsync(string indicator, DateTime date, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_HIV
                .Where(x => x.Indicator == indicator)
                .ApplyRegionFilter(regionId);

            var results = await query
                .GroupBy(x => x.Sex)
                .Select(g => new { Sex = g.Key, Total = g.Sum(x => x.Value) })
                .ToListAsync();

            return results.ToDictionary(x => x.Sex, x => x.Total);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting breakdown by sex for indicator {Indicator}", indicator);
            return new Dictionary<string, int>();
        }
    }

    public async Task<Dictionary<string, int>> GetBreakdownByAgeGroupAsync(string indicator, DateTime date, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_HIV
                .Where(x => x.Indicator == indicator)
                .ApplyRegionFilter(regionId);

            var results = await query
                .GroupBy(x => x.AgeGroup)
                .Select(g => new { AgeGroup = g.Key, Total = g.Sum(x => x.Value) })
                .ToListAsync();

            return results.ToDictionary(x => x.AgeGroup, x => x.Total);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting breakdown by age group for indicator {Indicator}", indicator);
            return new Dictionary<string, int>();
        }
    }
}

// Extension method for region filtering
public static class QueryExtensions
{
    public static IQueryable<IndicatorValueHIV> ApplyRegionFilter(this IQueryable<IndicatorValueHIV> query, int? regionId)
    {
        if (regionId.HasValue)
            return query.Where(x => x.RegionId == regionId.Value);
        return query;
    }
}