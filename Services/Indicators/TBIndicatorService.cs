using Microsoft.EntityFrameworkCore;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.DTOs.Indicators;
using Eswatini.Health.Api.Models.Staging;
using Eswatini.Health.Api.Services.Period;

namespace Eswatini.Health.Api.Services.Indicators;

public class TBIndicatorService : IndicatorServiceBase, ITBIndicatorService
{
    public TBIndicatorService(
        StagingDbContext db,
        ILogger<TBIndicatorService> logger,
        IPeriodService periodService) : base(db, logger, periodService)
    {
    }

    public async Task<List<IndicatorValueDto>> GetIndicatorDataAsync(IndicatorDataRequest request)
    {
        try
        {
            var query = _db.IndicatorValues_TB.AsQueryable();
            
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
            
            if (!string.IsNullOrEmpty(request.TBType))
                query = query.Where(x => x.TBType == request.TBType);

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
                    TBType = x.TBType,
                    Value = x.Value,
                    LastUpdated = x.UpdatedAt
                })
                .ToListAsync();

            // Aggregate by period if needed (do this in memory after EF query)
            if (request.PeriodType != "daily" && request.PeriodType != null && results.Any())
            {
                results = results
                    .GroupBy(x => new { x.Indicator, x.RegionId, x.Period, x.AgeGroup, x.Sex, x.PopulationType, x.TBType })
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
                        TBType = g.Key.TBType,
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
            _logger.LogError(ex, "Error getting TB indicator data");
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
            _logger.LogError(ex, "Error getting TB indicator trends");
            return new Dictionary<string, List<IndicatorValueDto>>();
        }
    }

    public async Task<bool> HasDataForPeriodAsync(string period)
    {
        try
        {
            var dateRange = _periodService.GetDateRangeForPeriod(period);
            
            return await _db.IndicatorValues_TB
                .AnyAsync(x => x.VisitDate >= dateRange.StartDate && x.VisitDate <= dateRange.EndDate);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking data for period {Period}", period);
            return false;
        }
    }

    // ========== TPT SPECIFIC METHODS ==========

    public async Task<int> GetTPTEligibleAsync(DateTime startDate, DateTime endDate, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_TB
                .Where(x => x.Indicator == "TPT_ELIGIBLE" 
                    && x.VisitDate >= startDate 
                    && x.VisitDate <= endDate);
            
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);

            return await query.SumAsync(x => x.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting TPT eligible");
            return 0;
        }
    }

    public async Task<int> GetTPTStartedAsync(DateTime startDate, DateTime endDate, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_TB
                .Where(x => x.Indicator == "TPT_STARTED" 
                    && x.VisitDate >= startDate 
                    && x.VisitDate <= endDate);
            
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);

            return await query.SumAsync(x => x.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting TPT started");
            return 0;
        }
    }

    public async Task<int> GetTPTCompletedAsync(DateTime startDate, DateTime endDate, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_TB
                .Where(x => x.Indicator == "TPT_COMPLETED" 
                    && x.VisitDate >= startDate 
                    && x.VisitDate <= endDate);
            
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);

            return await query.SumAsync(x => x.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting TPT completed");
            return 0;
        }
    }

    public async Task<int> GetTPTStoppedAsync(DateTime startDate, DateTime endDate, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_TB
                .Where(x => x.Indicator == "TPT_STOPPED" 
                    && x.VisitDate >= startDate 
                    && x.VisitDate <= endDate);
            
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);

            return await query.SumAsync(x => x.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting TPT stopped");
            return 0;
        }
    }

    public async Task<int> GetTPTTransferredOutAsync(DateTime startDate, DateTime endDate, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_TB
                .Where(x => x.Indicator == "TPT_TRANSFERRED_OUT" 
                    && x.VisitDate >= startDate 
                    && x.VisitDate <= endDate);
            
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);

            return await query.SumAsync(x => x.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting TPT transferred out");
            return 0;
        }
    }

    public async Task<int> GetTPTDiedAsync(DateTime startDate, DateTime endDate, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_TB
                .Where(x => x.Indicator == "TPT_DIED" 
                    && x.VisitDate >= startDate 
                    && x.VisitDate <= endDate);
            
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);

            return await query.SumAsync(x => x.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting TPT died");
            return 0;
        }
    }

    public async Task<int> GetTPTSelfStoppedAsync(DateTime startDate, DateTime endDate, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_TB
                .Where(x => x.Indicator == "TPT_SELF_STOPPED" 
                    && x.VisitDate >= startDate 
                    && x.VisitDate <= endDate);
            
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);

            return await query.SumAsync(x => x.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting TPT self stopped");
            return 0;
        }
    }

    public async Task<int> GetTPTStoppedByClinicianAsync(DateTime startDate, DateTime endDate, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_TB
                .Where(x => x.Indicator == "TPT_STOPPED_BY_CLINICIAN" 
                    && x.VisitDate >= startDate 
                    && x.VisitDate <= endDate);
            
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);

            return await query.SumAsync(x => x.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting TPT stopped by clinician");
            return 0;
        }
    }

    public async Task<int> GetTPTLTFUAsync(DateTime startDate, DateTime endDate, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_TB
                .Where(x => x.Indicator == "TPT_LTFU" 
                    && x.VisitDate >= startDate 
                    && x.VisitDate <= endDate);
            
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);

            return await query.SumAsync(x => x.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting TPT LTFU");
            return 0;
        }
    }
}

// Extension method for region filtering
public static class TBQueryExtensions
{
    public static IQueryable<IndicatorValueTB> ApplyRegionFilter(this IQueryable<IndicatorValueTB> query, int? regionId)
    {
        if (regionId.HasValue)
            return query.Where(x => x.RegionId == regionId.Value);
        return query;
    }
}