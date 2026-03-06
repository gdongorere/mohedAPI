namespace Eswatini.Health.Api.Common.Helpers;

public static class PeriodHelper
{
    public static string GetPreviousPeriod(string currentPeriod)
    {
        if (currentPeriod.Length != 6 || !currentPeriod.StartsWith("20"))
            return "2025Q2";

        var year = int.Parse(currentPeriod.Substring(0, 4));
        var quarter = int.Parse(currentPeriod.Substring(5, 1));

        if (quarter == 1)
            return $"{(year - 1)}Q4";
        else
            return $"{year}Q{quarter - 1}";
    }

    public static bool IsValidPeriod(string period)
    {
        if (string.IsNullOrEmpty(period) || period.Length != 6)
            return false;

        if (!period.StartsWith("20"))
            return false;

        if (!period.Contains('Q'))
            return false;

        var quarter = period.Substring(5, 1);
        return quarter is "1" or "2" or "3" or "4";
    }

    public static DateTime ConvertToReportingDate(string period)
    {
        if (!IsValidPeriod(period))
            return DateTime.UtcNow;

        var year = int.Parse(period.Substring(0, 4));
        var quarter = int.Parse(period.Substring(5, 1));

        return quarter switch
        {
            1 => new DateTime(year, 1, 1),
            2 => new DateTime(year, 4, 1),
            3 => new DateTime(year, 7, 1),
            4 => new DateTime(year, 10, 1),
            _ => new DateTime(year, 1, 1)
        };
    }

    public static string ConvertToPeriodString(DateTime date)
    {
        var quarter = (date.Month - 1) / 3 + 1;
        return $"{date.Year}Q{quarter}";
    }

    public static (DateTime StartDate, DateTime EndDate) GetQuarterDateRange(string period)
    {
        var date = ConvertToReportingDate(period);
        return (date, date.AddMonths(3).AddDays(-1));
    }
}