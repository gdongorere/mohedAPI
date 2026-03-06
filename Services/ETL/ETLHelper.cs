using System.Text;
using Microsoft.EntityFrameworkCore;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.Staging;

namespace Eswatini.Health.Api.Services.ETL;

public static class ETLHelper
{
    /// <summary>
    /// Creates a unique key for aggregation based on all dimension columns
    /// </summary>
    public static string CreateAggregationKey(string indicator, int regionId, DateTime visitDate, string ageGroup, string sex, string? populationType = null, string? tbType = null)
    {
        var normalizedAgeGroup = ageGroup?.Trim() ?? "Unknown";
        var normalizedSex = sex?.Trim() ?? "Other";
        var normalizedPopulationType = string.IsNullOrEmpty(populationType) ? "NULL" : populationType.Trim();
        var normalizedTbType = string.IsNullOrEmpty(tbType) ? "NULL" : tbType.Trim();
        
        return $"{indicator}|{regionId}|{visitDate:yyyy-MM-dd}|{normalizedAgeGroup}|{normalizedSex}|{normalizedPopulationType}|{normalizedTbType}";
    }

    /// <summary>
    /// Aggregates records by dimensions and returns dictionary of counts
    /// </summary>
    public static Dictionary<string, int> AggregateRecords<T>(
        List<T> records) where T : IndicatorValueBase
    {
        var aggregated = new Dictionary<string, int>();

        foreach (var record in records)
        {
            var key = CreateAggregationKey(
                record.Indicator,
                record.RegionId,
                record.VisitDate,
                record.AgeGroup,
                record.Sex,
                record.PopulationType,
                record is IndicatorValueTB tb ? tb.TBType : null
            );

            if (aggregated.ContainsKey(key))
                aggregated[key] += record.Value;
            else
                aggregated[key] = record.Value;
        }

        return aggregated;
    }

    /// <summary>
    /// IDEMPOTENT upsert - REPLACES data with current source values
    /// Running multiple times produces the SAME result
    /// NOTE: This method assumes the caller has already started a transaction
    /// </summary>
    public static async Task<(int Inserted, int Updated, int Skipped, int Deleted)> UpsertAggregatedRecordsAsync<T>(
        Dictionary<string, int> newAggregatedRecords,
        StagingDbContext db,
        ILogger logger,
        string batchId,
        Dictionary<string, (DateTime UpdatedAt, int Id, int Value)> existingRecords) where T : IndicatorValueBase, new()
    {
        if (!newAggregatedRecords.Any()) return (0, 0, 0, 0);

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var recordsToInsert = new List<T>();
        var recordsToUpdate = new List<(int Id, int NewValue)>();
        var recordsToDelete = new List<int>();
        var skippedRecords = 0;

        // First, identify records that exist in staging but NOT in source (should be deleted)
        foreach (var existingKvp in existingRecords)
        {
            if (!newAggregatedRecords.ContainsKey(existingKvp.Key))
            {
                recordsToDelete.Add(existingKvp.Value.Id);
                logger.LogDebug("Marking for deletion: Key={Key}, Id={Id}, OldValue={Value}", 
                    existingKvp.Key, existingKvp.Value.Id, existingKvp.Value.Value);
            }
        }

        // Then process new/updated records
        foreach (var kvp in newAggregatedRecords)
        {
            var key = kvp.Key;
            var newValue = kvp.Value;

            if (existingRecords.TryGetValue(key, out var existing))
            {
                // Record exists - update if value changed
                if (newValue != existing.Value)
                {
                    recordsToUpdate.Add((existing.Id, newValue));
                    logger.LogDebug("Marking for update: Key={Key}, Id={Id}, OldValue={Value}, NewValue={NewValue}", 
                        key, existing.Id, existing.Value, newValue);
                }
                else
                {
                    skippedRecords++;
                }
            }
            else
            {
                // New record - insert
                var parts = key.Split('|');
                
                var record = new T();
                record.Indicator = parts[0];
                record.RegionId = int.Parse(parts[1]);
                record.VisitDate = DateTime.Parse(parts[2]);
                record.AgeGroup = parts[3];
                record.Sex = parts[4];
                record.PopulationType = parts[5] == "NULL" ? null : parts[5];
                record.Value = newValue;
                record.CreatedAt = DateTime.UtcNow;
                record.UpdatedAt = DateTime.UtcNow;

                if (record is IndicatorValueTB tbRecord && parts.Length > 6)
                {
                    tbRecord.TBType = parts[6] == "NULL" ? null : parts[6];
                }

                recordsToInsert.Add(record);
                logger.LogDebug("Marking for insert: Key={Key}, Value={NewValue}", key, newValue);
            }
        }

        var inserted = 0;
        var updated = 0;
        var deleted = 0;

        // Execute operations - NO transaction here because caller already has one
        try
        {
            // 1. Delete records that no longer exist in source
            if (recordsToDelete.Any())
            {
                var tableName = GetTableName<T>();
                
                // Delete in chunks to avoid SQL parameter limits
                var chunkSize = 1000;
                var chunks = recordsToDelete.Chunk(chunkSize);
                
                foreach (var chunk in chunks)
                {
                    var idsToDelete = string.Join(",", chunk);
                    var deleteSql = $"DELETE FROM [{tableName}] WHERE Id IN ({idsToDelete})";
                    var chunkDeleted = await db.Database.ExecuteSqlRawAsync(deleteSql);
                    deleted += chunkDeleted;
                }
                
                logger.LogInformation("Batch {BatchId}: Deleted {Count} records that no longer exist in source", 
                    batchId, deleted);
            }

            // 2. Insert new records
            if (recordsToInsert.Any())
            {
                await db.AddRangeAsync(recordsToInsert);
                inserted = await db.SaveChangesAsync();
                logger.LogInformation("Batch {BatchId}: Inserted {Count} new records", batchId, inserted);
            }

            // 3. Update existing records with new values
            if (recordsToUpdate.Any())
            {
                updated = await BatchUpdateValuesAsync(db, recordsToUpdate, logger, GetTableName<T>());
                logger.LogInformation("Batch {BatchId}: Updated {Count} records with new values", batchId, updated);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error during upsert operations for batch {BatchId}", batchId);
            throw; // Let the caller handle transaction rollback
        }

        stopwatch.Stop();
        
        logger.LogInformation("Batch {BatchId}: {Inserted} inserted, {Updated} updated, {Deleted} deleted, {Skipped} unchanged in {Elapsed}ms", 
            batchId, inserted, updated, deleted, skippedRecords, stopwatch.ElapsedMilliseconds);

        return (inserted, updated, skippedRecords, deleted);
    }

    /// <summary>
    /// Get table name for entity type
    /// </summary>
    private static string GetTableName<T>() where T : IndicatorValueBase
    {
        if (typeof(T) == typeof(IndicatorValuePrevention))
            return "IndicatorValues_Prevention";
        if (typeof(T) == typeof(IndicatorValueHIV))
            return "IndicatorValues_HIV";
        if (typeof(T) == typeof(IndicatorValueTB))
            return "IndicatorValues_TB";
        
        throw new ArgumentException($"Unknown type: {typeof(T).Name}");
    }

    /// <summary>
    /// Batch update values (REPLACE, not add)
    /// </summary>
    private static async Task<int> BatchUpdateValuesAsync(
        StagingDbContext db,
        List<(int Id, int NewValue)> updates,
        ILogger logger,
        string tableName)
    {
        if (!updates.Any()) return 0;

        var totalUpdated = 0;
        
        // Process in chunks of 1000 to avoid SQL limits
        var chunkSize = 1000;
        var chunks = updates.Chunk(chunkSize);
        var chunkNumber = 0;

        foreach (var chunk in chunks)
        {
            chunkNumber++;
            var chunkDict = chunk.ToDictionary(x => x.Id, x => x.NewValue);
            
            logger.LogDebug("Processing update chunk {ChunkNumber} with {Count} records", 
                chunkNumber, chunkDict.Count);

            var ids = chunkDict.Keys.ToList();
            var valueCases = new StringBuilder();
            
            foreach (var kvp in chunkDict)
            {
                valueCases.AppendLine($"        WHEN {kvp.Key} THEN {kvp.Value}");
            }

            var sql = $@"
                UPDATE [{tableName}] SET
                    [Value] = CASE [Id]
                        {valueCases}
                        ELSE [Value]
                    END,
                    [UpdatedAt] = GETUTCDATE()
                WHERE [Id] IN ({string.Join(",", ids)})";

            try
            {
                var updated = await db.Database.ExecuteSqlRawAsync(sql);
                totalUpdated += updated;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error updating chunk {ChunkNumber}, falling back to individual updates", chunkNumber);
                
                // Fallback to individual updates
                foreach (var kvp in chunkDict)
                {
                    try
                    {
                        var individualSql = $@"
                            UPDATE [{tableName}] SET
                                [Value] = {kvp.Value},
                                [UpdatedAt] = GETUTCDATE()
                            WHERE [Id] = {kvp.Key}";
                        
                        await db.Database.ExecuteSqlRawAsync(individualSql);
                        totalUpdated++;
                    }
                    catch (Exception indivEx)
                    {
                        logger.LogError(indivEx, "Failed to update individual record Id: {Id}", kvp.Key);
                    }
                }
            }
        }

        return totalUpdated;
    }

    /// <summary>
/// Load ALL existing records for aggregation (no date filter!)
/// </summary>
public static async Task<Dictionary<string, (DateTime UpdatedAt, int Id, int Value)>> LoadAllExistingRecordsAsync<T>(
    StagingDbContext db,
    ILogger logger) where T : IndicatorValueBase
{
    logger.LogInformation("Loading ALL existing records from {TableName} for aggregation...", typeof(T).Name);
    
    var stopwatch = System.Diagnostics.Stopwatch.StartNew();
    
    var dict = new Dictionary<string, (DateTime UpdatedAt, int Id, int Value)>(StringComparer.OrdinalIgnoreCase);
    var duplicates = 0;

    // Handle each table type separately to ensure correct key generation
    if (typeof(T) == typeof(IndicatorValueHIV))
    {
        var results = await db.IndicatorValues_HIV
            .Select(x => new
            {
                Key = CreateAggregationKey(
                    x.Indicator,
                    x.RegionId,
                    x.VisitDate,
                    x.AgeGroup,
                    x.Sex,
                    null,  // HIV has no PopulationType in key
                    null
                ),
                x.UpdatedAt,
                x.Id,
                x.Value
            })
            .ToListAsync();

        foreach (var item in results)
        {
            if (dict.TryGetValue(item.Key, out var existing))
            {
                duplicates++;
                if (item.UpdatedAt > existing.UpdatedAt)
                {
                    dict[item.Key] = (item.UpdatedAt, (int)item.Id, item.Value);
                }
            }
            else
            {
                dict.Add(item.Key, (item.UpdatedAt, (int)item.Id, item.Value));
            }
        }
    }
    else if (typeof(T) == typeof(IndicatorValuePrevention))
    {
        var results = await db.IndicatorValues_Prevention
            .Select(x => new
            {
                Key = CreateAggregationKey(
                    x.Indicator,
                    x.RegionId,
                    x.VisitDate,
                    x.AgeGroup,
                    x.Sex,
                    x.PopulationType,
                    null
                ),
                x.UpdatedAt,
                x.Id,
                x.Value
            })
            .ToListAsync();

        foreach (var item in results)
        {
            if (dict.TryGetValue(item.Key, out var existing))
            {
                duplicates++;
                if (item.UpdatedAt > existing.UpdatedAt)
                {
                    dict[item.Key] = (item.UpdatedAt, (int)item.Id, item.Value);
                }
            }
            else
            {
                dict.Add(item.Key, (item.UpdatedAt, (int)item.Id, item.Value));
            }
        }
    }
    else if (typeof(T) == typeof(IndicatorValueTB))
    {
        var results = await db.IndicatorValues_TB
            .Select(x => new
            {
                Key = CreateAggregationKey(
                    x.Indicator,
                    x.RegionId,
                    x.VisitDate,
                    x.AgeGroup,
                    x.Sex,
                    x.PopulationType,
                    x.TBType
                ),
                x.UpdatedAt,
                x.Id,
                x.Value
            })
            .ToListAsync();

        foreach (var item in results)
        {
            if (dict.TryGetValue(item.Key, out var existing))
            {
                duplicates++;
                if (item.UpdatedAt > existing.UpdatedAt)
                {
                    dict[item.Key] = (item.UpdatedAt, (int)item.Id, item.Value);
                }
            }
            else
            {
                dict.Add(item.Key, (item.UpdatedAt, (int)item.Id, item.Value));
            }
        }
    }

    stopwatch.Stop();
    
    logger.LogInformation("Loaded {Count:N0} unique aggregated records from {TableName} in {Elapsed}ms (found {Duplicates:N0} duplicates)", 
        dict.Count, typeof(T).Name, stopwatch.ElapsedMilliseconds, duplicates);

    return dict;
}

    /// <summary>
    /// Log ETL summary
    /// </summary>
    public static void LogETLSummary(ILogger logger, string jobName, int recordsRead, int inserted, int updated, int skipped, long elapsedMs)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"╔══════════════════════════════════════════════════════════╗");
        sb.AppendLine($"║                    {jobName,-10} SUMMARY                    ║");
        sb.AppendLine($"╠══════════════════════════════════════════════════════════╣");
        sb.AppendLine($"║  Raw Records Read:  {recordsRead,10:N0}                              ║");
        sb.AppendLine($"║  Aggregated Insert: {inserted,10:N0}                              ║");
        sb.AppendLine($"║  Aggregated Update: {updated,10:N0}                              ║");
        sb.AppendLine($"║  Unchanged Groups:  {skipped,10:N0}                              ║");
        sb.AppendLine($"║  Time Elapsed:      {elapsedMs,10:N0}ms                              ║");
        sb.AppendLine($"╚══════════════════════════════════════════════════════════╝");
        
        logger.LogInformation(sb.ToString());
    }
}

/// <summary>
/// Extension methods for IEnumerable
/// </summary>
public static class EnumerableExtensions
{
    public static IEnumerable<IEnumerable<T>> Chunk<T>(this IEnumerable<T> source, int chunkSize)
    {
        if (source == null) throw new ArgumentNullException(nameof(source));
        if (chunkSize <= 0) throw new ArgumentException("Chunk size must be positive", nameof(chunkSize));

        return source
            .Select((x, i) => new { Index = i, Value = x })
            .GroupBy(x => x.Index / chunkSize)
            .Select(g => g.Select(x => x.Value));
    }
}