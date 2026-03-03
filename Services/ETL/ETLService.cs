using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.ETL;
using Microsoft.EntityFrameworkCore;

namespace Eswatini.Health.Api.Services.ETL;

public class ETLService : IETLService
{
    private readonly StagingDbContext _db;
    private readonly ILogger<ETLService> _logger;
    private readonly IHTSETLService _htsETL;
    private readonly IPrEPETLService _prepETL;
    private readonly IARTETLService _artETL;

    // ETL History tracking (in-memory for now, can be moved to database later)
    private static readonly List<ETLJobHistoryDto> _jobHistory = new();

    public ETLService(
        StagingDbContext db,
        ILogger<ETLService> logger,
        IHTSETLService htsETL,
        IPrEPETLService prepETL,
        IARTETLService artETL)
    {
        _db = db;
        _logger = logger;
        _htsETL = htsETL;
        _prepETL = prepETL;
        _artETL = artETL;
    }

    public async Task<ETLResult> RunETLForSourceAsync(string source, string triggeredBy = "system")
    {
        ETLResult result = source.ToLower() switch
        {
            "hts" => await _htsETL.RunAsync(triggeredBy),
            "prep" => await _prepETL.RunAsync(triggeredBy),
            "art" => await _artETL.RunAsync(triggeredBy),
            _ => throw new ArgumentException($"Unknown source: {source}")
        };

        // Record history
        _jobHistory.Add(new ETLJobHistoryDto
        {
            Id = _jobHistory.Count + 1,
            JobName = result.JobName,
            BatchId = result.BatchId,
            StartTime = result.StartTime,
            EndTime = result.EndTime,
            Status = result.Success ? "completed" : "failed",
            RecordsRead = result.RecordsRead,
            RecordsInserted = result.RecordsInserted,
            ErrorMessage = result.ErrorMessage,
            TriggeredBy = triggeredBy
        });

        // Keep only last 1000 records
        if (_jobHistory.Count > 1000)
            _jobHistory.RemoveAt(0);

        return result;
    }

    public async Task<ETLJobStatusDto> GetJobStatusAsync(string jobName)
    {
        var lastRun = _jobHistory
            .Where(j => j.JobName.Contains(jobName, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(j => j.StartTime)
            .FirstOrDefault();

        if (lastRun == null)
        {
            return new ETLJobStatusDto
            {
                JobName = jobName,
                Status = "never_run"
            };
        }

        return new ETLJobStatusDto
        {
            JobName = lastRun.JobName,
            Status = lastRun.Status,
            LastRunTime = lastRun.EndTime ?? lastRun.StartTime,
            LastBatchId = lastRun.BatchId,
            RecordsProcessed = lastRun.RecordsInserted,
            ErrorMessage = lastRun.ErrorMessage
        };
    }

    public async Task<List<ETLJobHistoryDto>> GetETLHistoryAsync(string? jobName = null, int limit = 100)
    {
        var query = _jobHistory.AsEnumerable();

        if (!string.IsNullOrEmpty(jobName))
            query = query.Where(j => j.JobName.Contains(jobName, StringComparison.OrdinalIgnoreCase));

        return query
            .OrderByDescending(j => j.StartTime)
            .Take(limit)
            .ToList();
    }

    public async Task<Dictionary<string, LastRunInfoDto>> GetLastRunTimesAsync()
    {
        var jobs = new[] { "HTS", "PrEP", "ART" };
        var result = new Dictionary<string, LastRunInfoDto>();

        foreach (var job in jobs)
        {
            var lastRun = _jobHistory
                .Where(j => j.JobName.Contains(job, StringComparison.OrdinalIgnoreCase))
                .OrderByDescending(j => j.StartTime)
                .FirstOrDefault();

            result[job] = new LastRunInfoDto
            {
                SourceTable = job switch
                {
                    "HTS" => "tmpHTSTestedDetail",
                    "PrEP" => "LineListingsPrep, aPrepDetail",
                    "ART" => "TBD",
                    _ => "Unknown"
                },
                TargetTable = job switch
                {
                    "HTS" => "IndicatorValues_Prevention",
                    "PrEP" => "IndicatorValues_Prevention",
                    "ART" => "IndicatorValues_HIV",
                    _ => "Unknown"
                },
                LastRunTime = lastRun?.EndTime ?? lastRun?.StartTime,
                LastBatchId = int.TryParse(lastRun?.BatchId, out var batchId) ? batchId.ToString() : "0",
                Status = lastRun?.Status ?? "never_run",
                RecordCount = lastRun?.RecordsInserted ?? 0
            };
        }

        return result;
    }
}