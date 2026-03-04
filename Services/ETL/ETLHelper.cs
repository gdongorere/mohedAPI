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
        // Normalize values to avoid key collisions
        var normalizedAgeGroup = ageGroup?.Trim() ?? "Unknown";
        var normalizedSex = sex?.Trim() ?? "Other";
        var normalizedPopulationType = string.IsNullOrEmpty(populationType) ? "NULL" : populationType.Trim();
        var normalizedTbType = string.IsNullOrEmpty(tbType) ? "NULL" : tbType.Trim();
        
        return $"{indicator}|{regionId}|{visitDate:yyyy-MM-dd}|{normalizedAgeGroup}|{normalizedSex}|{normalizedPopulationType}|{normalizedTbType}";
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
        var updateCount = 0;

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

            if (existingRecords.TryGetValue(key, out var existing))
            {
                // If this record is newer, we might want to update
                if (record.UpdatedAt > existing.UpdatedAt)
                {
                    // For now, just count it - we can implement update logic later
                    updateCount++;
                }
                else
                {
                    duplicateCount++;
                }
            }
            else
            {
                newRecords.Add(record);
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

        if (duplicateCount > 0 || updateCount > 0)
        {
            logger.LogDebug("Batch {BatchId}: {Inserted} inserted, {Duplicates} duplicates, {Updates} newer records found", 
                batchId, inserted, duplicateCount, updateCount);
        }

        return (inserted, duplicateCount, updateCount);
    }

    /// <summary>
    /// Load existing records for deduplication - FIXED to handle duplicates
    /// </summary>
    public static async Task<Dictionary<string, (DateTime UpdatedAt, int Id)>> LoadExistingRecordsAsync<T>(
        StagingDbContext db,
        ILogger logger,
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
                    (x as IndicatorValueTB) != null ? (x as IndicatorValueTB).TBType : null
                ),
                x.UpdatedAt,
                x.Id
            })
            .ToListAsync();

        // Handle duplicates by keeping the most recent one
        var dict = new Dictionary<string, (DateTime UpdatedAt, int Id)>(StringComparer.OrdinalIgnoreCase);
        var duplicates = 0;

        foreach (var item in results)
        {
            if (dict.TryGetValue(item.Key, out var existing))
            {
                duplicates++;
                // Keep the most recent record
                if (item.UpdatedAt > existing.UpdatedAt)
                {
                    dict[item.Key] = (item.UpdatedAt, (int)item.Id);
                    logger.LogDebug("Replaced duplicate key {Key} with newer record (old: {OldDate}, new: {NewDate})", 
                        item.Key, existing.UpdatedAt, item.UpdatedAt);
                }
            }
            else
            {
                dict.Add(item.Key, (item.UpdatedAt, (int)item.Id));
            }
        }

        if (duplicates > 0)
        {
            logger.LogWarning("Found {Duplicates} duplicate keys in database for {TableName}, kept most recent versions", 
                duplicates, typeof(T).Name);
        }

        return dict;
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
        sb.AppendLine($"║  Records Read:      {recordsRead,10:N0}                              ║");
        sb.AppendLine($"║  Records Inserted:  {recordsInserted,10:N0}                              ║");
        sb.AppendLine($"║  Duplicates Found:  {duplicates,10:N0}                              ║");
        sb.AppendLine($"║  Time Elapsed:      {elapsedMs,10:N0}ms                              ║");
        sb.AppendLine($"╚══════════════════════════════════════════════════════════╝");
        
        logger.LogInformation(sb.ToString());
    }
}