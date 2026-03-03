namespace Eswatini.Health.Api.Services.Period;

public interface IPeriodService
{
    string FormatDateAsPeriod(DateTime date, string periodType = "daily");
    DateTime ParsePeriodToDate(string period);
    (DateTime StartDate, DateTime EndDate) GetDateRangeForPeriod(string period);
    List<string> GetAvailablePeriods(List<DateTime> dates, string periodType = "daily");
    string GetCurrentPeriod();
}

public class PeriodService : IPeriodService
{
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
}