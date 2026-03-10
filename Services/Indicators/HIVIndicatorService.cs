using Microsoft.EntityFrameworkCore;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.DTOs.Indicators;
using Eswatini.Health.Api.Models.Staging;
using Eswatini.Health.Api.Services.Period;
using Eswatini.Health.Api.Models.DTOs.Dashboard;
using Eswatini.Health.Api.Common.Helpers;

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
            // Parse the quarter period (e.g., "2025Q4" -> Q4 2025)
            var year = int.Parse(period.Substring(0, 4));
            var quarter = int.Parse(period.Substring(5, 1));
            
            // Calculate the quarter date range
            DateTime startDate, endDate;
            switch (quarter)
            {
                case 1:
                    startDate = new DateTime(year, 1, 1);
                    endDate = new DateTime(year, 3, 31);
                    break;
                case 2:
                    startDate = new DateTime(year, 4, 1);
                    endDate = new DateTime(year, 6, 30);
                    break;
                case 3:
                    startDate = new DateTime(year, 7, 1);
                    endDate = new DateTime(year, 9, 30);
                    break;
                case 4:
                    startDate = new DateTime(year, 10, 1);
                    endDate = new DateTime(year, 12, 31);
                    break;
                default:
                    return false;
            }
            
            _logger.LogDebug("Checking for data in period {Period} from {StartDate} to {EndDate}", 
                period, startDate, endDate);
            
            // Check if there's ANY data in this date range
            var hasData = await _db.IndicatorValues_HIV
                .AnyAsync(x => x.VisitDate >= startDate && x.VisitDate <= endDate);
            
            _logger.LogDebug("HasDataForPeriodAsync for {Period}: {HasData}", period, hasData);
            
            return hasData;
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
    
























    // Add these methods to the existing HIVIndicatorService class

public async Task<DashboardSummaryDto> GetDashboardSummaryAsync(string period)
{
    try
    {
        var periodDate = PeriodHelper.ConvertToReportingDate(period);
        
        // National level, all ages, both sexes
        var txCurr = await GetIndicatorValueAsync("TX_CURR", period);
        var vlTested = await GetIndicatorValueAsync("TX_VL_TESTED", period);
        var vlSuppressed = await GetIndicatorValueAsync("TX_VL_SUPPRESSED", period);
        var vlUndetectable = await GetIndicatorValueAsync("TX_VL_UNDETECTABLE", period);
        
        var suppressionRate = vlTested > 0 
            ? Math.Round((vlSuppressed / vlTested) * 100, 1) 
            : 0;
        
        var coverageRate = txCurr > 0 
            ? Math.Round((vlTested / txCurr) * 100, 1) 
            : 0;
        
        var undetectableRate = vlTested > 0 
            ? Math.Round((vlUndetectable / vlTested) * 100, 1) 
            : 0;
        
        var unsuppressedRate = 100 - suppressionRate;

        return new DashboardSummaryDto
        {
            Period = period,
            ReportingDate = periodDate,
            LastUpdated = DateTime.UtcNow,
            BatchId = "LATEST",
            Summary = new DashboardMetricsDto
            {
                TotalOnArt = (int)txCurr,
                VlTested = (int)vlTested,
                VlSuppressed = (int)vlSuppressed,
                VlUnsuppressed = (int)(vlTested - vlSuppressed),
                VlUndetectable = (int)vlUndetectable,
                VlSuppressionRate = suppressionRate,
                VlCoverageRate = coverageRate,
                VlUndetectableRate = undetectableRate,
                UnsuppressedRate = unsuppressedRate
            }
        };
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Error getting dashboard summary for period {Period}", period);
        throw;
    }
}

public async Task<DetailedDashboardDto> GetDetailedDashboardAsync(string period)
{
    try
    {
        var periodDate = PeriodHelper.ConvertToReportingDate(period);
        
        var summary = await GetDashboardSummaryAsync(period);
        var bySex = await GetBreakdownBySexAsync(period);
        var byAgeGroup = await GetBreakdownByAgeGroupAsync(period);
        var byRegion = await GetBreakdownByRegionAsync(period);

        return new DetailedDashboardDto
        {
            Period = period,
            ReportingDate = periodDate,
            LastUpdated = summary.LastUpdated,
            BatchId = summary.BatchId,
            Summary = new SummaryMetricsDto
            {
                TotalOnArt = summary.Summary.TotalOnArt,
                VlTested = summary.Summary.VlTested,
                VlSuppressed = summary.Summary.VlSuppressed,
                VlUnsuppressed = summary.Summary.VlUnsuppressed,
                VlUndetectable = summary.Summary.VlUndetectable,
                SuppressionRate = summary.Summary.VlSuppressionRate,
                CoverageRate = summary.Summary.VlCoverageRate,
                UnsuppressedRate = summary.Summary.UnsuppressedRate,
                UndetectableRate = summary.Summary.VlUndetectableRate
            },
            BySex = bySex,
            ByAgeGroup = byAgeGroup,
            ByRegion = byRegion
        };
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Error getting detailed dashboard for period {Period}", period);
        throw;
    }
}

public async Task<List<RegionalBreakdownDto>> GetBreakdownByRegionAsync(string period)
{
    try
    {
        // Parse the quarter period
        var year = int.Parse(period.Substring(0, 4));
        var quarter = int.Parse(period.Substring(5, 1));
        
        // Calculate the quarter date range
        DateTime startDate, endDate;
        switch (quarter)
        {
            case 1:
                startDate = new DateTime(year, 1, 1);
                endDate = new DateTime(year, 3, 31);
                break;
            case 2:
                startDate = new DateTime(year, 4, 1);
                endDate = new DateTime(year, 6, 30);
                break;
            case 3:
                startDate = new DateTime(year, 7, 1);
                endDate = new DateTime(year, 9, 30);
                break;
            case 4:
                startDate = new DateTime(year, 10, 1);
                endDate = new DateTime(year, 12, 31);
                break;
            default:
                return new List<RegionalBreakdownDto>();
        }
        
        var query = _db.IndicatorValues_HIV
            .Where(iv => iv.VisitDate >= startDate && iv.VisitDate <= endDate);

        var results = await query
            .Where(iv => iv.Indicator == "TX_CURR" 
                || iv.Indicator == "TX_VL_TESTED" 
                || iv.Indicator == "TX_VL_SUPPRESSED")
            .GroupBy(iv => iv.RegionId)
            .Select(g => new
            {
                RegionId = g.Key,
                OnArt = g.Where(iv => iv.Indicator == "TX_CURR").Sum(iv => (int?)iv.Value) ?? 0,
                Tested = g.Where(iv => iv.Indicator == "TX_VL_TESTED").Sum(iv => (int?)iv.Value) ?? 0,
                Suppressed = g.Where(iv => iv.Indicator == "TX_VL_SUPPRESSED").Sum(iv => (int?)iv.Value) ?? 0
            })
            .ToListAsync();

        return results.Select(r => new RegionalBreakdownDto
        {
            RegionCode = GetRegionCode(r.RegionId),
            RegionName = GetRegionName(r.RegionId),
            OnArt = r.OnArt,
            Tested = r.Tested,
            Suppressed = r.Suppressed,
            SuppressionRate = r.Tested > 0 
                ? Math.Round((decimal)r.Suppressed / r.Tested * 100, 1) 
                : 0,
            TestCoverage = r.OnArt > 0 
                ? Math.Round((decimal)r.Tested / r.OnArt * 100, 1) 
                : 0
        }).ToList();
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Error getting breakdown by region for period {Period}", period);
        return new List<RegionalBreakdownDto>();
    }
}

public async Task<List<SexBasedBreakdownDto>> GetBreakdownBySexAsync(string period, string? regionCode = null)
{
    try
    {
        // Parse the quarter period
        var year = int.Parse(period.Substring(0, 4));
        var quarter = int.Parse(period.Substring(5, 1));
        
        // Calculate the quarter date range
        DateTime startDate, endDate;
        switch (quarter)
        {
            case 1:
                startDate = new DateTime(year, 1, 1);
                endDate = new DateTime(year, 3, 31);
                break;
            case 2:
                startDate = new DateTime(year, 4, 1);
                endDate = new DateTime(year, 6, 30);
                break;
            case 3:
                startDate = new DateTime(year, 7, 1);
                endDate = new DateTime(year, 9, 30);
                break;
            case 4:
                startDate = new DateTime(year, 10, 1);
                endDate = new DateTime(year, 12, 31);
                break;
            default:
                _logger.LogWarning("Invalid quarter: {Quarter} for period {Period}", quarter, period);
                return new List<SexBasedBreakdownDto>();
        }
        
        _logger.LogDebug("GetBreakdownBySexAsync for period {Period} from {StartDate} to {EndDate}", 
            period, startDate, endDate);
        
        var query = _db.IndicatorValues_HIV
            .Where(iv => iv.VisitDate >= startDate 
                      && iv.VisitDate <= endDate
                      && iv.Sex != null);

        if (!string.IsNullOrEmpty(regionCode))
        {
            var regionId = GetRegionId(regionCode);
            query = query.Where(iv => iv.RegionId == regionId);
            _logger.LogDebug("Filtering by region: {RegionCode} -> RegionId: {RegionId}", regionCode, regionId);
        }

        var results = await query
            .Where(iv => iv.Indicator == "TX_CURR" 
                || iv.Indicator == "TX_VL_TESTED" 
                || iv.Indicator == "TX_VL_SUPPRESSED")
            .GroupBy(iv => iv.Sex)
            .Select(g => new
            {
                Sex = g.Key,
                OnArt = g.Where(iv => iv.Indicator == "TX_CURR").Sum(iv => (int?)iv.Value) ?? 0,
                Tested = g.Where(iv => iv.Indicator == "TX_VL_TESTED").Sum(iv => (int?)iv.Value) ?? 0,
                Suppressed = g.Where(iv => iv.Indicator == "TX_VL_SUPPRESSED").Sum(iv => (int?)iv.Value) ?? 0
            })
            .ToListAsync();

        _logger.LogDebug("Found {Count} sex breakdown results for period {Period}", results.Count, period);

        return results.Select(r => new SexBasedBreakdownDto
        {
            Sex = r.Sex == "M" ? "Male" : r.Sex == "F" ? "Female" : "Unknown",
            OnArt = r.OnArt,
            Tested = r.Tested,
            Suppressed = r.Suppressed,
            Unsuppressed = r.Tested - r.Suppressed,
            SuppressionRate = r.Tested > 0 
                ? Math.Round((decimal)r.Suppressed / r.Tested * 100, 1) 
                : 0,
            TestCoverage = r.OnArt > 0 
                ? Math.Round((decimal)r.Tested / r.OnArt * 100, 1) 
                : 0
        }).ToList();
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Error getting breakdown by sex for period {Period}", period);
        return new List<SexBasedBreakdownDto>();
    }
}

public async Task<List<AgeGroupBreakdownDto>> GetBreakdownByAgeGroupAsync(string period, string? regionCode = null)
{
    try
    {
        // Parse the quarter period
        var year = int.Parse(period.Substring(0, 4));
        var quarter = int.Parse(period.Substring(5, 1));
        
        // Calculate the quarter date range
        DateTime startDate, endDate;
        switch (quarter)
        {
            case 1:
                startDate = new DateTime(year, 1, 1);
                endDate = new DateTime(year, 3, 31);
                break;
            case 2:
                startDate = new DateTime(year, 4, 1);
                endDate = new DateTime(year, 6, 30);
                break;
            case 3:
                startDate = new DateTime(year, 7, 1);
                endDate = new DateTime(year, 9, 30);
                break;
            case 4:
                startDate = new DateTime(year, 10, 1);
                endDate = new DateTime(year, 12, 31);
                break;
            default:
                _logger.LogWarning("Invalid quarter: {Quarter} for period {Period}", quarter, period);
                return new List<AgeGroupBreakdownDto>();
        }
        
        _logger.LogDebug("GetBreakdownByAgeGroupAsync for period {Period} from {StartDate} to {EndDate}", 
            period, startDate, endDate);
        
        var query = _db.IndicatorValues_HIV
            .Where(iv => iv.VisitDate >= startDate 
                      && iv.VisitDate <= endDate
                      && iv.AgeGroup != null);

        if (!string.IsNullOrEmpty(regionCode))
        {
            var regionId = GetRegionId(regionCode);
            query = query.Where(iv => iv.RegionId == regionId);
            _logger.LogDebug("Filtering by region: {RegionCode} -> RegionId: {RegionId}", regionCode, regionId);
        }

        var results = await query
            .Where(iv => iv.Indicator == "TX_CURR" 
                || iv.Indicator == "TX_VL_TESTED" 
                || iv.Indicator == "TX_VL_SUPPRESSED")
            .GroupBy(iv => iv.AgeGroup)
            .Select(g => new
            {
                AgeGroup = g.Key,
                OnArt = g.Where(iv => iv.Indicator == "TX_CURR").Sum(iv => (int?)iv.Value) ?? 0,
                Tested = g.Where(iv => iv.Indicator == "TX_VL_TESTED").Sum(iv => (int?)iv.Value) ?? 0,
                Suppressed = g.Where(iv => iv.Indicator == "TX_VL_SUPPRESSED").Sum(iv => (int?)iv.Value) ?? 0
            })
            .ToListAsync();

        _logger.LogDebug("Found {Count} age group breakdown results for period {Period}", results.Count, period);

        // Define standard age group order
        var ageGroupOrder = new Dictionary<string, int>
        {
            ["< 1"] = 1,
            ["1 - 4"] = 2,
            ["5 - 9"] = 3,
            ["10 - 14"] = 4,
            ["15 - 19"] = 5,
            ["20 - 24"] = 6,
            ["25 - 29"] = 7,
            ["30 - 34"] = 8,
            ["35 - 39"] = 9,
            ["40 - 44"] = 10,
            ["45 - 49"] = 11,
            ["50 - 54"] = 12,
            ["55 - 59"] = 13,
            [">= 60"] = 14,
            ["Unknown"] = 99
        };

        return results.Select(r => new AgeGroupBreakdownDto
        {
            AgeGroup = r.AgeGroup ?? "Unknown",
            AgeGroupCode = r.AgeGroup ?? "Unknown",
            OnArt = r.OnArt,
            Tested = r.Tested,
            Suppressed = r.Suppressed,
            Unsuppressed = r.Tested - r.Suppressed,
            SuppressionRate = r.Tested > 0 
                ? Math.Round((decimal)r.Suppressed / r.Tested * 100, 1) 
                : 0,
            TestCoverage = r.OnArt > 0 
                ? Math.Round((decimal)r.Tested / r.OnArt * 100, 1) 
                : 0
        })
        .OrderBy(a => ageGroupOrder.ContainsKey(a.AgeGroup) ? ageGroupOrder[a.AgeGroup] : 99)
        .ToList();
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Error getting breakdown by age group for period {Period}", period);
        return new List<AgeGroupBreakdownDto>();
    }
}

public async Task<decimal> GetIndicatorValueAsync(string indicatorCode, string period, string? regionCode = null, string? ageGroup = null, string? sex = null)
{
    try
    {
        // Parse the quarter period
        var year = int.Parse(period.Substring(0, 4));
        var quarter = int.Parse(period.Substring(5, 1));
        
        // Calculate the quarter date range
        DateTime startDate, endDate;
        switch (quarter)
        {
            case 1:
                startDate = new DateTime(year, 1, 1);
                endDate = new DateTime(year, 3, 31);
                break;
            case 2:
                startDate = new DateTime(year, 4, 1);
                endDate = new DateTime(year, 6, 30);
                break;
            case 3:
                startDate = new DateTime(year, 7, 1);
                endDate = new DateTime(year, 9, 30);
                break;
            case 4:
                startDate = new DateTime(year, 10, 1);
                endDate = new DateTime(year, 12, 31);
                break;
            default:
                return 0;
        }
        
        var query = _db.IndicatorValues_HIV
            .Where(iv => iv.Indicator == indicatorCode
                && iv.VisitDate >= startDate
                && iv.VisitDate <= endDate);

        if (!string.IsNullOrEmpty(regionCode))
        {
            var regionId = GetRegionId(regionCode);
            query = query.Where(iv => iv.RegionId == regionId);
        }
        
        if (!string.IsNullOrEmpty(ageGroup))
            query = query.Where(iv => iv.AgeGroup == ageGroup);
        
        if (!string.IsNullOrEmpty(sex))
            query = query.Where(iv => iv.Sex == sex);

        // Sum all values in the quarter
        var value = await query
            .SumAsync(iv => (int?)iv.Value) ?? 0;

        _logger.LogDebug("GetIndicatorValueAsync for {Indicator} in {Period}: {Value}", 
            indicatorCode, period, value);

        return value;
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Error getting {Indicator} for period {Period}", indicatorCode, period);
        return 0;
    }
}

// Helper methods for region conversion
private static new string GetRegionCode(int regionId)
{
    return regionId switch
    {
        1 => "HH",
        2 => "MN",
        3 => "LB",
        4 => "SH",
        _ => "UN"
    };
}

private static new int GetRegionId(string regionCode)
{
    return regionCode.ToUpper() switch
    {
        "HH" => 1,
        "MN" => 2,
        "LB" => 3,
        "SH" => 4,
        _ => 0
    };
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