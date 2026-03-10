using Eswatini.Health.Api.Models.ETL;

namespace Eswatini.Health.Api.Services.ETL;

public interface IETLService
{
    Task<ETLResult> RunETLForSourceAsync(string source, string triggeredBy = "system");
    Task<ETLJobStatusDto> GetJobStatusAsync(string jobName);
    Task<List<ETLJobHistoryDto>> GetETLHistoryAsync(string? jobName = null, int limit = 100);
    Task<Dictionary<string, LastRunInfoDto>> GetLastRunTimesAsync();
}

// Individual ETL job interfaces
public interface IHTSETLService
{
    Task<ETLResult> RunAsync(string triggeredBy = "system");
    Task<int> GetRecordCountForPeriodAsync(DateTime startDate, DateTime endDate);
}

public interface IPrEPETLService
{
    Task<ETLResult> RunAsync(string triggeredBy = "system");
    Task<int> GetRecordCountForPeriodAsync(DateTime startDate, DateTime endDate);
}

public interface IARTETLService
{
    Task<ETLResult> RunAsync(string triggeredBy = "system");
    Task<int> GetRecordCountForPeriodAsync(DateTime startDate, DateTime endDate);
}

public interface ITBETLService
{
    Task<ETLResult> RunAsync(string triggeredBy = "system");
    Task<int> GetRecordCountForPeriodAsync(DateTime startDate, DateTime endDate);
}