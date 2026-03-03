namespace Eswatini.Health.Api.Models.ETL;

public class ETLResult
{
    public bool Success { get; set; }
    public string BatchId { get; set; } = string.Empty;
    public string JobName { get; set; } = string.Empty;
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public int RecordsRead { get; set; }
    public int RecordsInserted { get; set; }
    public int RecordsUpdated { get; set; }
    public string? ErrorMessage { get; set; }
    public long DurationMs => EndTime.HasValue ? (long)(EndTime.Value - StartTime).TotalMilliseconds : 0;
}

public class ETLJobStatusDto
{
    public string JobName { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public DateTime? LastRunTime { get; set; }
    public string? LastBatchId { get; set; }
    public int? RecordsProcessed { get; set; }
    public string? ErrorMessage { get; set; }
}

public class ETLJobHistoryDto
{
    public long Id { get; set; }
    public string JobName { get; set; } = string.Empty;
    public string BatchId { get; set; } = string.Empty;
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public string Status { get; set; } = string.Empty;
    public int RecordsRead { get; set; }
    public int RecordsInserted { get; set; }
    public int RecordsUpdated { get; set; }
    public string? ErrorMessage { get; set; }
    public string? TriggeredBy { get; set; }
}

public class LastRunInfoDto
{
    public string SourceTable { get; set; } = string.Empty;
    public string TargetTable { get; set; } = string.Empty;
    public DateTime? LastRunTime { get; set; }
    public string LastBatchId { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public int RecordCount { get; set; }
}