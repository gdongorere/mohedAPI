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
    /// Creates a unique key from an IndicatorValueBase record
    /// </summary>
    public static string CreateUniqueKey(IndicatorValueBase record)
    {
        return CreateUniqueKey(
            record.Indicator,
            record.RegionId,
            record.VisitDate,
            record.AgeGroup,
            record.Sex,
            record.PopulationType,
            record is IndicatorValueTB tb ? tb.TBType : null
        );
    }

    /// <summary>
    /// Batch inserts/updates with OPTIMIZED batch updates
    /// </summary>
    public static async Task<(int Inserted, int Updated, int Skipped)> ProcessRecordsAsync<T>(
        List<T> records,
        StagingDbContext db,
        ILogger logger,
        string batchId,
        Dictionary<string, (DateTime UpdatedAt, int Id)> existingRecords) where T : IndicatorValueBase
    {
        if (!records.Any()) return (0, 0, 0);

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var newRecords = new List<T>();
        var updatesDict = new Dictionary<int, T>(); // Id -> Updated record
        var skippedRecords = 0;

        foreach (var record in records)
        {
            var key = CreateUniqueKey(record);

            if (existingRecords.TryGetValue(key, out var existing))
            {
                // If this record is newer, UPDATE it
                if (record.UpdatedAt > existing.UpdatedAt)
                {
                    updatesDict[existing.Id] = record;
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

        // FAST PATH 1: Batch insert new records
        if (newRecords.Any())
        {
            await db.AddRangeAsync(newRecords);
            inserted = await db.SaveChangesAsync();
            
            // Add newly inserted records to existing records dictionary
            foreach (var record in newRecords)
            {
                var key = CreateUniqueKey(record);
                if (!existingRecords.ContainsKey(key))
                {
                    existingRecords[key] = (record.UpdatedAt, 0);
                }
            }
        }

        // FAST PATH 2: Batch update using single SQL command
        if (updatesDict.Any())
        {
            updated = await BatchUpdateRecordsAsync(db, updatesDict, logger);
        }

        stopwatch.Stop();
        
        if (inserted > 0 || updated > 0)
        {
            logger.LogDebug("Batch {BatchId}: {Inserted} inserted, {Updated} updated, {Skipped} unchanged in {Elapsed}ms", 
                batchId, inserted, updated, skippedRecords, stopwatch.ElapsedMilliseconds);
        }

        return (inserted, updated, skippedRecords);
    }

    /// <summary>
    /// ULTRA-FAST batch update using single SQL command
    /// </summary>
    private static async Task<int> BatchUpdateRecordsAsync<T>(
        StagingDbContext db,
        Dictionary<int, T> updatesDict,
        ILogger logger) where T : IndicatorValueBase
    {
        if (!updatesDict.Any()) return 0;

        var tableName = GetTableName<T>();
        var ids = updatesDict.Keys.ToList();
        
        logger.LogDebug("Batch updating {Count} records in {TableName} using single SQL command", 
            updatesDict.Count, tableName);

        // Build CASE statements for each field
        var valueCases = new StringBuilder();
        var updatedAtCases = new StringBuilder();
        var populationTypeCases = new StringBuilder();
        
        foreach (var kvp in updatesDict)
        {
            var record = kvp.Value;
            valueCases.AppendLine($"        WHEN {kvp.Key} THEN {record.Value}");
            updatedAtCases.AppendLine($"        WHEN {kvp.Key} THEN '{record.UpdatedAt:yyyy-MM-dd HH:mm:ss}'");
            
            var popType = record.PopulationType?.Replace("'", "''") ?? "NULL";
            populationTypeCases.AppendLine($"        WHEN {kvp.Key} THEN '{popType}'");
        }

        var sql = $@"
            UPDATE [{tableName}] SET
                [Value] = CASE [Id]
                    {valueCases}
                    ELSE [Value]
                END,
                [UpdatedAt] = CASE [Id]
                    {updatedAtCases}
                    ELSE [UpdatedAt]
                END,
                [PopulationType] = CASE [Id]
                    {populationTypeCases}
                    ELSE [PopulationType]
                END
            WHERE [Id] IN ({string.Join(",", ids)})";

        return await db.Database.ExecuteSqlRawAsync(sql);
    }

    /// <summary>
    /// Get table name for entity type
    /// </summary>
    private static string GetTableName<T>() where T : IndicatorValueBase
    {
        return typeof(T) switch
        {
            var t when t == typeof(IndicatorValuePrevention) => "IndicatorValues_Prevention",
            var t when t == typeof(IndicatorValueHIV) => "IndicatorValues_HIV",
            var t when t == typeof(IndicatorValueTB) => "IndicatorValues_TB",
            _ => throw new ArgumentException($"Unknown type: {typeof(T).Name}")
        };
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
                Key = CreateUniqueKey(x),
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
        
        logger.LogInformation("Loaded {Count:N0} unique records from {TableName} in {Elapsed}ms (found {Duplicates:N0} duplicates)", 
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