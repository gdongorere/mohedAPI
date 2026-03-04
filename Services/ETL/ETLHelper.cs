using System.Text;
using Microsoft.EntityFrameworkCore;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.Staging;

namespace Eswatini.Health.Api.Services.ETL;

public static class ETLHelper
{
    /// <summary>
    /// Creates a unique key for deduplication based on all relevant columns
    /// </summary>
    public static string CreateUniqueKey(string indicator, int regionId, DateTime visitDate, string ageGroup, string sex, string? populationType = null, string? tbType = null)
    {
        var normalizedAgeGroup = ageGroup?.Trim() ?? "Unknown";
        var normalizedSex = sex?.Trim() ?? "Other";
        var normalizedPopulationType = string.IsNullOrEmpty(populationType) ? "NULL" : populationType.Trim();
        var normalizedTbType = string.IsNullOrEmpty(tbType) ? "NULL" : tbType.Trim();
        
        return $"{indicator}|{regionId}|{visitDate:yyyy-MM-dd}|{normalizedAgeGroup}|{normalizedSex}|{normalizedPopulationType}|{normalizedTbType}";
    }

    /// <summary>
    /// Batch inserts/updates with deduplication - ALWAYS processes ALL data
    /// </summary>
    public static async Task<(int Inserted, int Updated, int Skipped)> ProcessRecordsAsync<T>(
        List<T> records,
        StagingDbContext db,
        ILogger logger,
        string batchId,
        Dictionary<string, (DateTime UpdatedAt, int Id)> existingRecords) where T : IndicatorValueBase
    {
        if (!records.Any()) return (0, 0, 0);

        var newRecords = new List<T>();
        var recordsToUpdate = new List<(T Record, int Id)>();
        var skippedRecords = 0;

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
                // Compare all relevant fields to see if data changed
                if (HasRecordChanged(record, existing.Id, db))
                {
                    recordsToUpdate.Add((record, existing.Id));
                    // Update the dictionary with new timestamp
                    existingRecords[key] = (record.UpdatedAt, existing.Id);
                }
                else
                {
                    skippedRecords++;
                }
            }
            else
            {
                newRecords.Add(record);
            }
        }

        var inserted = 0;
        var updated = 0;

        // Insert new records
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
                
                if (!existingRecords.ContainsKey(key))
                {
                    existingRecords[key] = (record.UpdatedAt, 0);
                }
            }
        }

        // Update existing records
        foreach (var (record, id) in recordsToUpdate)
        {
            var existing = await db.Set<T>().FindAsync(id);
            if (existing != null)
            {
                // Update all fields that could change
                existing.Value = record.Value;
                existing.UpdatedAt = record.UpdatedAt;
                existing.PopulationType = record.PopulationType;
                
                // For TB records
                if (existing is IndicatorValueTB existingTB && record is IndicatorValueTB newTB)
                {
                    existingTB.TBType = newTB.TBType;
                }
                
                updated++;
            }
        }
        
        if (updated > 0)
        {
            await db.SaveChangesAsync();
        }

        logger.LogInformation("Batch {BatchId}: {Inserted} inserted, {Updated} updated, {Skipped} unchanged", 
            batchId, inserted, updated, skippedRecords);

        return (inserted, updated, skippedRecords);
    }

    /// <summary>
    /// Check if a record has changed compared to what's in the database
    /// </summary>
    private static bool HasRecordChanged<T>(T newRecord, int existingId, StagingDbContext db) where T : IndicatorValueBase
    {
        var existing = db.Set<T>().Local.FirstOrDefault(e => e.Id == existingId);
        if (existing == null)
        {
            // If not in local cache, we'll assume it might have changed
            // In a production system, you might want to query for it
            return true;
        }

        // Compare fields that matter
        if (existing.Value != newRecord.Value) return true;
        if (existing.PopulationType != newRecord.PopulationType) return true;
        
        // For TB records
        if (existing is IndicatorValueTB existingTB && newRecord is IndicatorValueTB newTB)
        {
            if (existingTB.TBType != newTB.TBType) return true;
        }

        return false;
    }

    /// <summary>
    /// Load ALL existing records for deduplication (no date filter!)
    /// </summary>
    public static async Task<Dictionary<string, (DateTime UpdatedAt, int Id)>> LoadAllExistingRecordsAsync<T>(
        StagingDbContext db,
        ILogger logger) where T : IndicatorValueBase
    {
        logger.LogInformation("Loading ALL existing records from {TableName} for deduplication...", typeof(T).Name);
        
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        var results = await db.Set<T>()
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
                }
            }
            else
            {
                dict.Add(item.Key, (item.UpdatedAt, (int)item.Id));
            }
        }

        stopwatch.Stop();
        
        logger.LogInformation("Loaded {Count} unique records from {TableName} in {Elapsed}ms (found {Duplicates} duplicates)", 
            dict.Count, typeof(T).Name, stopwatch.ElapsedMilliseconds, duplicates);

        return dict;
    }

    /// <summary>
    /// Log ETL summary with update information
    /// </summary>
    public static void LogETLSummary(ILogger logger, string jobName, int recordsRead, int inserted, int updated, int skipped, long elapsedMs)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"╔══════════════════════════════════════════════════════════╗");
        sb.AppendLine($"║                    {jobName,-10} SUMMARY                    ║");
        sb.AppendLine($"╠══════════════════════════════════════════════════════════╣");
        sb.AppendLine($"║  Records Read:      {recordsRead,10:N0}                              ║");
        sb.AppendLine($"║  Records Inserted:  {inserted,10:N0}                              ║");
        sb.AppendLine($"║  Records Updated:   {updated,10:N0}                              ║");
        sb.AppendLine($"║  Records Unchanged: {skipped,10:N0}                              ║");
        sb.AppendLine($"║  Time Elapsed:      {elapsedMs,10:N0}ms                              ║");
        sb.AppendLine($"╚══════════════════════════════════════════════════════════╝");
        
        logger.LogInformation(sb.ToString());
    }
}