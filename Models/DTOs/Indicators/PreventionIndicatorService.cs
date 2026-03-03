using Microsoft.EntityFrameworkCore;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.DTOs.Indicators;
using Eswatini.Health.Api.Models.Staging;
using Eswatini.Health.Api.Services.Period;

namespace Eswatini.Health.Api.Services.Indicators;

public class PreventionIndicatorService : IndicatorServiceBase, IPreventionIndicatorService
{
    public PreventionIndicatorService(
        StagingDbContext db,
        ILogger<PreventionIndicatorService> logger,
        IPeriodService periodService) : base(db, logger, periodService)
    {
    }

    public async Task<List<IndicatorValueDto>> GetIndicatorDataAsync(IndicatorDataRequest request)
    {
        try
        {
            var query = _db.IndicatorValues_Prevention.AsQueryable();
            
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

            // Aggregate by period if needed
            if (request.PeriodType != "daily" && request.PeriodType != null)
            {
                results = results
                    .GroupBy(x => new { x.Indicator, x.RegionId, x.Period, x.AgeGroup, x.Sex, x.PopulationType })
                    .Select(g => new IndicatorValueDto
                    {
                        Indicator = g.Key.Indicator,
                        RegionId = g.Key.RegionId,
                        RegionName = GetRegionName(g.Key.RegionId),
                        VisitDate = g.First().VisitDate,
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
            _logger.LogError(ex, "Error getting prevention indicator data");
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
            _logger.LogError(ex, "Error getting prevention indicator trends");
            return new Dictionary<string, List<IndicatorValueDto>>();
        }
    }

    public async Task<bool> HasDataForPeriodAsync(string period)
    {
        try
        {
            var dateRange = _periodService.GetDateRangeForPeriod(period);
            
            return await _db.IndicatorValues_Prevention
                .AnyAsync(x => x.VisitDate >= dateRange.StartDate && x.VisitDate <= dateRange.EndDate);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking data for period {Period}", period);
            return false;
        }
    }

    public async Task<int> GetHIVTestsAsync(DateTime startDate, DateTime endDate, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_Prevention
                .Where(x => x.Indicator == "HTS_TST" 
                    && x.VisitDate >= startDate 
                    && x.VisitDate <= endDate);
            
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);

            return await query.SumAsync(x => x.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting HIV tests");
            return 0;
        }
    }

    public async Task<int> GetHIVPositivesAsync(DateTime startDate, DateTime endDate, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_Prevention
                .Where(x => x.Indicator == "HTS_POS" 
                    && x.VisitDate >= startDate 
                    && x.VisitDate <= endDate);
            
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);

            return await query.SumAsync(x => x.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting HIV positives");
            return 0;
        }
    }

    public async Task<int> GetPrEPInitiationsAsync(DateTime startDate, DateTime endDate, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_Prevention
                .Where(x => x.Indicator == "PREP_NEW" 
                    && x.VisitDate >= startDate 
                    && x.VisitDate <= endDate);
            
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);

            return await query.SumAsync(x => x.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting PrEP initiations");
            return 0;
        }
    }

    public async Task<int> GetPrEPSeroconversionsAsync(DateTime startDate, DateTime endDate, int? regionId = null)
    {
        try
        {
            var query = _db.IndicatorValues_Prevention
                .Where(x => x.Indicator == "PREP_SEROCONVERSION" 
                    && x.VisitDate >= startDate 
                    && x.VisitDate <= endDate);
            
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);

            return await query.SumAsync(x => x.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting PrEP seroconversions");
            return 0;
        }
    }

    public async Task<Dictionary<string, int>> GetPrEPByMethodAsync(DateTime startDate, DateTime endDate, int? regionId = null)
    {
        try
        {
            // This would require a different indicator per method or a dimension
            // For now, return empty dictionary - to be implemented when data structure is clear
            return await Task.FromResult(new Dictionary<string, int>());
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting PrEP by method");
            return new Dictionary<string, int>();
        }
    }

    private string GetRegionName(int regionId)
    {
        return regionId switch
        {
            1 => "Hhohho",
            2 => "Manzini",
            3 => "Shiselweni",
            4 => "Lubombo",
            _ => "Unknown"
        };
    }
}