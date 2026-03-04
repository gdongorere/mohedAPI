using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.Staging;
using Microsoft.EntityFrameworkCore;
using System.Text;

namespace Eswatini.Health.Api.Services.ETL;

public static class ETLHelper
{
    /// <summary>
    /// Creates a unique key for deduplication based on all relevant columns
    /// </summary>
    public static string CreateUniqueKey(string indicator, int regionId, DateTime visitDate, string ageGroup, string sex, string? populationType = null, string? tbType = null)
    {
        return $"{indicator}|{regionId}|{visitDate:yyyy-MM-dd}|{ageGroup}|{sex}|{populationType ?? "NULL"}|{tbType ?? "NULL"}";
    }

    /// <summary>
    /// Batch inserts with deduplication check - returns detailed stats
    /// </summary>
    public static async Task<(int Inserted, int Duplicates, int Updated)> BatchInsertWithDeduplicationAsync<T>(
        List<T> records,
        StagingDbContext db,
        ILogger logger,
        string batchId,
        Dictionary<string, (DateTime UpdatedAt, int Id)> existingRecords) where T : IndicatorValueBase
    {
        if (!records.Any()) return (0, 0, 0);

        var newRecords = new List<T>();
        var duplicateCount = 0;

        foreach (var record in records)
        {
            var key = CreateUniqueKey(
                record.Indicator,
                record.RegionId,
                record.VisitDate,
                record.AgeGroup,
                record.Sex,
                record.PopulationType,
                record is IndicatorValueTB tb ? tb.TBType : null
            );

            if (existingRecords.ContainsKey(key))
            {
                duplicateCount++;
            }
            else
            {
                newRecords.Add(record);
                // We'll add to dictionary after insert when we have the ID
            }
        }

        var inserted = 0;

        if (newRecords.Any())
        {
            await db.AddRangeAsync(newRecords);
            inserted = await db.SaveChangesAsync();
            
            // Add newly inserted records to existing records dictionary
            foreach (var record in newRecords)
            {
                var key = CreateUniqueKey(
                    record.Indicator,
                    record.RegionId,
                    record.VisitDate,
                    record.AgeGroup,
                    record.Sex,
                    record.PopulationType,
                    record is IndicatorValueTB tb ? tb.TBType : null
                );
                
                // Note: We don't have the ID yet without re-querying
                // For deduplication in the same batch, just tracking existence is enough
                if (!existingRecords.ContainsKey(key))
                {
                    existingRecords[key] = (record.UpdatedAt, 0);
                }
            }
        }

        return (inserted, duplicateCount, 0);
    }

    /// <summary>
    /// Load existing records for deduplication
    /// </summary>
    public static async Task<Dictionary<string, (DateTime UpdatedAt, int Id)>> LoadExistingRecordsAsync<T>(
        StagingDbContext db,
        DateTime? since = null) where T : IndicatorValueBase
    {
        var query = db.Set<T>().AsQueryable();
        
        if (since.HasValue)
        {
            query = query.Where(x => x.UpdatedAt >= since.Value);
        }

        var results = await query
            .Select(x => new
            {
                Key = CreateUniqueKey(
                    x.Indicator,
                    x.RegionId,
                    x.VisitDate,
                    x.AgeGroup,
                    x.Sex,
                    x.PopulationType,
                    (x as IndicatorValueTB).TBType
                ),
                x.UpdatedAt,
                x.Id
            })
            .ToListAsync();

        return results.ToDictionary(x => x.Key, x => (x.UpdatedAt, (int)x.Id));
    }

    /// <summary>
    /// Log ETL summary in a clean format
    /// </summary>
    public static void LogETLSummary(ILogger logger, string jobName, int recordsRead, int recordsInserted, int duplicates, long elapsedMs)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"╔══════════════════════════════════════════════════════════╗");
        sb.AppendLine($"║                    {jobName,-10} SUMMARY                    ║");
        sb.AppendLine($"╠══════════════════════════════════════════════════════════╣");
        sb.AppendLine($"║  Records Read:      {recordsRead,10}                              ║");
        sb.AppendLine($"║  Records Inserted:  {recordsInserted,10}                              ║");
        sb.AppendLine($"║  Duplicates Found:  {duplicates,10}                              ║");
        sb.AppendLine($"║  Time Elapsed:      {elapsedMs,10}ms                              ║");
        sb.AppendLine($"╚══════════════════════════════════════════════════════════╝");
        
        logger.LogInformation(sb.ToString());
    }
}