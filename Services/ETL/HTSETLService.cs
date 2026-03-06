using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.ETL;
using Eswatini.Health.Api.Models.Staging;

namespace Eswatini.Health.Api.Services.ETL;

public class HTSETLService : IHTSETLService
{
    private readonly StagingDbContext _db;
    private readonly IConfiguration _configuration;
    private readonly ILogger<HTSETLService> _logger;
    private readonly IFacilityRegionService _facilityRegionService;
    private readonly string _sourceConnectionString;
    private readonly int _batchSize;

    // ============================================================
    // CONFIGURATION SECTION - Change these values as needed
    // ============================================================
    
    /// <summary>
    /// Whether to always include HTS_TST even if positive+negative doesn't match tested
    /// If TRUE: Always adds HTS_TST record when tested > 0
    /// If FALSE: Only adds HTS_TST when positive+negative == tested
    /// </summary>
    private const bool AlwaysIncludeHtsTest = true;
    
    /// <summary>
    /// Whether to log HTS discrepancies (when positive+negative != tested)
    /// </summary>
    private const bool LogHtsDiscrepancies = true;
    
    /// <summary>
    /// Date filter for HTS data (set to null to process all data)
    /// </summary>
    private static readonly DateTime? MinProcessDate = null;
    
    /// <summary>
    /// Column name mappings
    /// </summary>
    private static class SourceColumns
    {
        public const string FacilityCode = "FacilityCode";
        public const string VisitDate = "VisitDate";
        public const string AgeGroup = "AgeGroup";
        public const string SexName = "SexName";
        public const string PopulationGroup = "PopulationGroup";
        public const string Tested = "HTS_TestedForHIV";
        public const string Negative = "HTS_TestedNegative";
        public const string Positive = "HTS_TestedPositive";
        public const string Linked = "HTS_TestedPositiveInitiatedOnART";
    }
    
    /// <summary>
    /// Indicator mappings
    /// </summary>
    private static class Indicators
    {
        public const string Test = "HTS_TST";
        public const string Negative = "HTS_NEG";
        public const string Positive = "HTS_POS";
        public const string Linkage = "LINKAGE_ART";
    }
    
    /// <summary>
    /// Sex mappings
    /// </summary>
    private static readonly Dictionary<string, string> SexMappings = new(StringComparer.OrdinalIgnoreCase)
    {
        ["MALE"] = "M",
        ["FEMALE"] = "F",
        ["M"] = "M",
        ["F"] = "F",
        ["Other"] = "Other",
        ["UNKNOWN"] = "Other"
    };
    
    /// <summary>
    /// Default values for missing data
    /// </summary>
    private static class Defaults
    {
        public const string AgeGroup = "Unknown";
        public const string Sex = "Other";
        public const string PopulationType = null;
    }
    
    // ============================================================
    // END OF CONFIGURATION SECTION
    // ============================================================

    public HTSETLService(
        StagingDbContext db,
        IConfiguration configuration,
        ILogger<HTSETLService> logger,
        IFacilityRegionService facilityRegionService)
    {
        _db = db;
        _configuration = configuration;
        _logger = logger;
        _facilityRegionService = facilityRegionService;
        _sourceConnectionString = configuration.GetConnectionString("SourceConnection") 
            ?? throw new InvalidOperationException("SourceConnection not configured");
        _batchSize = configuration.GetValue<int>("ETL:BatchSize", 10000);
    }

    public async Task<ETLResult> RunAsync(string triggeredBy = "system")
{
    var result = new ETLResult
    {
        JobName = "HTS ETL",
        StartTime = DateTime.UtcNow
    };

    var batchId = $"HTS_{DateTime.UtcNow:yyyyMMdd_HHmmss}_UTC";
    var totalStopwatch = System.Diagnostics.Stopwatch.StartNew();

    try
    {
        _logger.LogInformation("🚀 Starting HTS ETL with batch {BatchId}", batchId);
        
        // Process all source data into memory FIRST
        _logger.LogInformation("Step 1: Reading and aggregating source data...");
        var (recordsRead, finalRecords) = await ProcessAndAggregateSourceDataAsync();
        
        _logger.LogInformation("Step 2: Successfully processed {RecordsRead} raw records into {FinalCount} aggregated records", 
            recordsRead, finalRecords.Count);
        
        // Clear existing HTS data and insert new data
        // Note: No transaction here - ETLService already handles the transaction
        _logger.LogInformation("Step 3: Replacing staging data...");
        
        await _db.Database.ExecuteSqlRawAsync(
            "DELETE FROM IndicatorValues_Prevention WHERE Indicator IN ('HTS_TST', 'HTS_POS', 'HTS_NEG', 'LINKAGE_ART')");
        
        await _db.IndicatorValues_Prevention.AddRangeAsync(finalRecords);
        var inserted = await _db.SaveChangesAsync();
        
        result.Success = true;
        result.BatchId = batchId;
        result.RecordsRead = recordsRead;
        result.RecordsInserted = inserted;
        result.RecordsUpdated = 0;
        result.EndTime = DateTime.UtcNow;
        
        _logger.LogInformation("Step 4: Successfully inserted {Inserted} records into staging", inserted);

        totalStopwatch.Stop();
        
        _logger.LogInformation("✅ HTS ETL completed in {ElapsedMs}ms", totalStopwatch.ElapsedMilliseconds);
    }
    catch (Exception ex)
    {
        result.Success = false;
        result.ErrorMessage = ex.Message;
        result.EndTime = DateTime.UtcNow;
        _logger.LogError(ex, "❌ HTS ETL failed: {Message}", ex.Message);
        throw;
    }

    return result;
}

    private async Task<(int RecordsRead, List<IndicatorValuePrevention> FinalRecords)> ProcessAndAggregateSourceDataAsync()
    {
        var recordsRead = 0;
        var allRawRecords = new List<IndicatorValuePrevention>();
        var finalRecords = new List<IndicatorValuePrevention>();

        var facilityRegions = await _facilityRegionService.GetFacilityRegionsAsync();
        _logger.LogInformation("Found {Count} facility-region mappings", facilityRegions.Count);

        var unmappedFacilities = new HashSet<string>();
        var discrepancyCount = 0;
        var totalDiscrepancy = 0;

        // Build query with optional date filter
        var query = $@"
            SELECT 
                [{SourceColumns.FacilityCode}],
                [{SourceColumns.VisitDate}],
                [{SourceColumns.AgeGroup}],
                [{SourceColumns.SexName}],
                [{SourceColumns.PopulationGroup}],
                ISNULL([{SourceColumns.Tested}], 0) as {SourceColumns.Tested},
                ISNULL([{SourceColumns.Negative}], 0) as {SourceColumns.Negative},
                ISNULL([{SourceColumns.Positive}], 0) as {SourceColumns.Positive},
                ISNULL([{SourceColumns.Linked}], 0) as {SourceColumns.Linked}
            FROM [All_Dataset].[dbo].[tmpHTSTestedDetail]
            WHERE [{SourceColumns.VisitDate}] IS NOT NULL"
            + (MinProcessDate.HasValue ? $" AND [{SourceColumns.VisitDate}] >= '{MinProcessDate.Value:yyyy-MM-dd}'" : "")
            + $" ORDER BY [{SourceColumns.VisitDate}]";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        
        using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            recordsRead++;
            
            if (recordsRead % 10000 == 0)
                _logger.LogInformation("Processed {Count:N0} raw HTS records", recordsRead);

            var facilityCode = reader.IsDBNull(0) ? null : reader.GetString(0).Trim();
            if (string.IsNullOrEmpty(facilityCode))
                continue;

            var visitDate = reader.IsDBNull(1) ? (DateTime?)null : reader.GetDateTime(1);
            if (!visitDate.HasValue)
                continue;

            var ageGroup = reader.IsDBNull(2) ? Defaults.AgeGroup : reader.GetString(2).Trim();
            var sexName = reader.IsDBNull(3) ? Defaults.Sex : reader.GetString(3).Trim();
            var populationGroup = reader.IsDBNull(4) ? Defaults.PopulationType : reader.GetString(4).Trim();
            
            if (!facilityRegions.TryGetValue(facilityCode, out var regionId))
            {
                unmappedFacilities.Add(facilityCode);
                continue;
            }

            var sex = SexMappings.TryGetValue(sexName, out var mappedSex) ? mappedSex : Defaults.Sex;
            var now = DateTime.UtcNow;

            var testedValue = reader.GetInt32(5);
            var negativeValue = reader.GetInt32(6);
            var positiveValue = reader.GetInt32(7);
            var linkedValue = reader.GetInt32(8);

            // Log discrepancy if configured
            if (LogHtsDiscrepancies && testedValue > 0 && (positiveValue + negativeValue) != testedValue)
            {
                discrepancyCount++;
                totalDiscrepancy += testedValue - (positiveValue + negativeValue);
            }

            // Add HTS_TST
            if (testedValue > 0 && (AlwaysIncludeHtsTest || (positiveValue + negativeValue) == testedValue))
            {
                allRawRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = Indicators.Test,
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationGroup,
                    Value = testedValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // Add HTS_NEG
            if (negativeValue > 0)
            {
                allRawRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = Indicators.Negative,
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationGroup,
                    Value = negativeValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // Add HTS_POS
            if (positiveValue > 0)
            {
                allRawRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = Indicators.Positive,
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationGroup,
                    Value = positiveValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // Add LINKAGE_ART
            if (linkedValue > 0)
            {
                allRawRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = Indicators.Linkage,
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationGroup,
                    Value = linkedValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // Periodically aggregate to manage memory
            if (allRawRecords.Count >= _batchSize * 10)
            {
                var aggregated = ETLHelper.AggregateRecords(allRawRecords);
                var batchRecords = ConvertAggregatedToEntities(aggregated);
                finalRecords.AddRange(batchRecords);
                allRawRecords.Clear();
            }
        }

        // Log unmapped facilities
        if (unmappedFacilities.Any())
        {
            _logger.LogWarning("Found {Count} unmapped facilities: {Facilities}", 
                unmappedFacilities.Count, string.Join(", ", unmappedFacilities.Take(20)));
        }

        // Log discrepancy summary
        if (discrepancyCount > 0)
        {
            _logger.LogWarning("Found {Count} HTS discrepancies totaling {TotalDiff} tests", 
                discrepancyCount, totalDiscrepancy);
        }

        // Process remaining records
        if (allRawRecords.Any())
        {
            var aggregated = ETLHelper.AggregateRecords(allRawRecords);
            var batchRecords = ConvertAggregatedToEntities(aggregated);
            finalRecords.AddRange(batchRecords);
        }

        return (recordsRead, finalRecords);
    }

    private List<IndicatorValuePrevention> ConvertAggregatedToEntities(Dictionary<string, int> aggregated)
    {
        var entities = new List<IndicatorValuePrevention>();
        
        foreach (var kvp in aggregated)
        {
            var parts = kvp.Key.Split('|');
            entities.Add(new IndicatorValuePrevention
            {
                Indicator = parts[0],
                RegionId = int.Parse(parts[1]),
                VisitDate = DateTime.Parse(parts[2]),
                AgeGroup = parts[3],
                Sex = parts[4],
                PopulationType = parts[5] == "NULL" ? null : parts[5],
                Value = kvp.Value,
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTime.UtcNow
            });
        }
        
        return entities;
    }

    public async Task<int> GetRecordCountForPeriodAsync(DateTime startDate, DateTime endDate)
    {
        var query = @"
            SELECT COUNT(*) 
            FROM [All_Dataset].[dbo].[tmpHTSTestedDetail]
            WHERE VisitDate >= @StartDate AND VisitDate < @EndDate";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        command.Parameters.AddWithValue("@StartDate", startDate);
        command.Parameters.AddWithValue("@EndDate", endDate);
        
        await connection.OpenAsync();
        var result = await command.ExecuteScalarAsync();
        return result != null ? Convert.ToInt32(result) : 0;
    }
}