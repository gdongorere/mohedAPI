using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Common.Helpers;
using Microsoft.EntityFrameworkCore;

namespace Eswatini.Health.Api.Services.Period;

public interface IPeriodService
{
    string FormatDateAsPeriod(DateTime date, string periodType = "daily");
    DateTime ParsePeriodToDate(string period);
    (DateTime StartDate, DateTime EndDate) GetDateRangeForPeriod(string period);
    List<string> GetAvailablePeriods(List<DateTime> dates, string periodType = "daily");
    string GetCurrentPeriod();
    
    // New methods for quarterly period handling
    Task<string> GetLatestAvailablePeriodAsync();
    Task<string> GetDefaultPeriodAsync();
    Task<List<string>> GetAllAvailablePeriodsAsync();
    bool IsValidPeriod(string period);
    string ConvertToPeriodString(DateTime date);
    DateTime ConvertToReportingDate(string period);
}

public class PeriodService : IPeriodService
{
    private readonly StagingDbContext _db;
    private readonly IConfiguration _configuration;
    private readonly ILogger<PeriodService> _logger;

    public PeriodService(
        StagingDbContext db,
        IConfiguration configuration,
        ILogger<PeriodService> logger)
    {
        _db = db;
        _configuration = configuration;
        _logger = logger;
    }

    public string FormatDateAsPeriod(DateTime date, string periodType = "daily")
    {
        return periodType.ToLower() switch
        {
            "daily" => date.ToString("yyyy-MM-dd"),
            "monthly" => date.ToString("yyyy-MM"),
            "quarterly" => $"{(date.Year)}-Q{((date.Month - 1) / 3) + 1}",
            "yearly" => date.ToString("yyyy"),
            _ => date.ToString("yyyy-MM-dd")
        };
    }

    public DateTime ParsePeriodToDate(string period)
    {
        if (DateTime.TryParse(period, out var date))
            return date;

        // Handle YYYY-MM
        if (period.Length == 7 && period.Contains("-"))
        {
            var parts = period.Split('-');
            if (parts.Length == 2 && int.TryParse(parts[0], out var year) && int.TryParse(parts[1], out var month))
                return new DateTime(year, month, 1);
        }

        // Handle YYYY-Qn
        if (period.Length == 6 && period.Contains("-Q"))
        {
            var parts = period.Split("-Q");
            if (parts.Length == 2 && int.TryParse(parts[0], out var year) && int.TryParse(parts[1], out var quarter))
            {
                var month = (quarter - 1) * 3 + 1;
                return new DateTime(year, month, 1);
            }
        }

        // Handle YYYY
        if (period.Length == 4 && int.TryParse(period, out var yearOnly))
            return new DateTime(yearOnly, 1, 1);

        return DateTime.UtcNow;
    }

    public (DateTime StartDate, DateTime EndDate) GetDateRangeForPeriod(string period)
    {
        var date = ParsePeriodToDate(period);
        
        if (period.Length == 4) // Year
            return (new DateTime(date.Year, 1, 1), new DateTime(date.Year, 12, 31));
        
        if (period.Length == 7 && period.Contains("-") && !period.Contains("Q")) // Month
            return (new DateTime(date.Year, date.Month, 1), new DateTime(date.Year, date.Month, DateTime.DaysInMonth(date.Year, date.Month)));
        
        if (period.Contains("-Q")) // Quarter
        {
            var quarter = int.Parse(period.Substring(period.Length - 1));
            var startMonth = (quarter - 1) * 3 + 1;
            var endMonth = startMonth + 2;
            return (
                new DateTime(date.Year, startMonth, 1),
                new DateTime(date.Year, endMonth, DateTime.DaysInMonth(date.Year, endMonth))
            );
        }
        
        // Day
        return (date.Date, date.Date);
    }

    public List<string> GetAvailablePeriods(List<DateTime> dates, string periodType = "daily")
    {
        return dates
            .Select(d => FormatDateAsPeriod(d, periodType))
            .Distinct()
            .OrderByDescending(p => p)
            .ToList();
    }

    public string GetCurrentPeriod()
    {
        return FormatDateAsPeriod(DateTime.UtcNow, "quarterly");
    }

    // New methods
    public async Task<string> GetLatestAvailablePeriodAsync()
    {
        try
        {
            var latestDate = await _db.IndicatorValues_HIV
                .Select(iv => iv.VisitDate)
                .Distinct()
                .OrderByDescending(d => d)
                .FirstOrDefaultAsync();

            if (latestDate != default)
            {
                return ConvertToPeriodString(latestDate);
            }

            _logger.LogInformation("No available periods found in database");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get latest available period from database");
        }

        return await GetDefaultPeriodAsync();
    }

    public async Task<string> GetDefaultPeriodAsync()
    {
        try
        {
            var configPeriod = _configuration["ApiSettings:DefaultPeriod"];
            if (!string.IsNullOrEmpty(configPeriod))
            {
                _logger.LogInformation("Using configured default period: {Period}", configPeriod);
                return await Task.FromResult(configPeriod);
            }

            // Calculate current quarter
            var now = DateTime.UtcNow;
            var quarter = (now.Month - 1) / 3 + 1;
            var currentPeriod = $"{now.Year}Q{quarter}";
            
            _logger.LogInformation("Calculated current quarter as default period: {Period}", currentPeriod);
            return await Task.FromResult(currentPeriod);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get default period, falling back to hardcoded value");
            return await Task.FromResult("2025Q3");
        }
    }

    public async Task<List<string>> GetAllAvailablePeriodsAsync()
    {
        try
        {
            var periods = await _db.IndicatorValues_HIV
                .Select(iv => iv.VisitDate)
                .Distinct()
                .OrderByDescending(d => d)
                .Select(d => ConvertToPeriodString(d))
                .ToListAsync();

            return periods;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting all available periods");
            return new List<string>();
        }
    }

    public bool IsValidPeriod(string period)
    {
        return PeriodHelper.IsValidPeriod(period);
    }

    public string ConvertToPeriodString(DateTime date)
    {
        return PeriodHelper.ConvertToPeriodString(date);
    }

    public DateTime ConvertToReportingDate(string period)
    {
        return PeriodHelper.ConvertToReportingDate(period);
    }
}