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
    /// Batch upsert of aggregated counts
    /// </summary>
    public static async Task<(int Inserted, int Updated, int Skipped)> UpsertAggregatedRecordsAsync<T>(
        Dictionary<string, int> aggregatedRecords,
        StagingDbContext db,
        ILogger logger,
        string batchId,
        Dictionary<string, (DateTime UpdatedAt, int Id, int Value)> existingRecords) where T : IndicatorValueBase, new()
    {
        if (!aggregatedRecords.Any()) return (0, 0, 0);

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var recordsToInsert = new List<T>();
        var updatesDict = new Dictionary<int, int>(); // Id -> New Value
        var skippedRecords = 0;

        foreach (var kvp in aggregatedRecords)
        {
            var key = kvp.Key;
            var newValue = kvp.Value;

            if (existingRecords.TryGetValue(key, out var existing))
            {
                // If value changed, update it
                if (newValue != existing.Value)
                {
                    updatesDict[existing.Id] = newValue;
                    // Update the dictionary with new value and timestamp
                    existingRecords[key] = (DateTime.UtcNow, existing.Id, newValue);
                }
                else
                {
                    skippedRecords++;
                }
            }
            else
            {
                // Parse the key to create a new record
                var parts = key.Split('|');
                var record = new T
                {
                    Indicator = parts[0],
                    RegionId = int.Parse(parts[1]),
                    VisitDate = DateTime.Parse(parts[2]),
                    AgeGroup = parts[3],
                    Sex = parts[4],
                    PopulationType = parts[5] == "NULL" ? null : parts[5],
                    Value = newValue,
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow
                };

                // For TB records
                if (record is IndicatorValueTB tbRecord && parts.Length > 6)
                {
                    tbRecord.TBType = parts[6] == "NULL" ? null : parts[6];
                }

                recordsToInsert.Add(record);
            }
        }

        var inserted = 0;
        var updated = 0;

        // Insert new records
        if (recordsToInsert.Any())
        {
            await db.AddRangeAsync(recordsToInsert);
            inserted = await db.SaveChangesAsync();
            
            logger.LogDebug("Batch {BatchId}: Inserted {Count} new aggregated records", 
                batchId, inserted);
        }

        // Update existing records
        if (updatesDict.Any())
        {
            updated = await BatchUpdateValuesAsync(db, updatesDict, logger, typeof(T).Name);
            logger.LogDebug("Batch {BatchId}: Updated {Count} records with new values", 
                batchId, updated);
        }

        stopwatch.Stop();
        
        logger.LogInformation("Batch {BatchId}: {Inserted} inserted, {Updated} updated, {Skipped} unchanged in {Elapsed}ms", 
            batchId, inserted, updated, skippedRecords, stopwatch.ElapsedMilliseconds);

        return (inserted, updated, skippedRecords);
    }

    /// <summary>
    /// Batch update just the Value column
    /// </summary>
    private static async Task<int> BatchUpdateValuesAsync(
        StagingDbContext db,
        Dictionary<int, int> updatesDict,
        ILogger logger,
        string tableName)
    {
        if (!updatesDict.Any()) return 0;

        var totalUpdated = 0;
        
        // Process in chunks of 1000 to avoid SQL limits
        var chunkSize = 1000;
        var chunks = updatesDict.Chunk(chunkSize);
        var chunkNumber = 0;

        foreach (var chunk in chunks)
        {
            chunkNumber++;
            var chunkDict = chunk.ToDictionary(x => x.Key, x => x.Value);
            
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
        
        var results = await db.Set<T>()
            .Select(x => new
            {
                Key = CreateAggregationKey(
                    x.Indicator,
                    x.RegionId,
                    x.VisitDate,
                    x.AgeGroup,
                    x.Sex,
                    x.PopulationType,
                    (x as IndicatorValueTB).TBType 
                ),
                x.UpdatedAt,
                x.Id,
                x.Value
            })
            .ToListAsync();

        // Handle duplicates by keeping the most recent one
        var dict = new Dictionary<string, (DateTime UpdatedAt, int Id, int Value)>(StringComparer.OrdinalIgnoreCase);
        var duplicates = 0;

        foreach (var item in results)
        {
            if (dict.TryGetValue(item.Key, out var existing))
            {
                duplicates++;
                // Keep the most recent record
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