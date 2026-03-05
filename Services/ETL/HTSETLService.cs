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
    /// Whether to clear existing data before running ETL
    /// </summary>
    private const bool ClearExistingDataOnRun = true;
    
    /// <summary>
    /// Date filter for HTS data (set to null to process all data)
    /// Format: new DateTime(2024, 1, 1) or null for no filter
    /// </summary>
    private static readonly DateTime? MinProcessDate = null; // Example: new DateTime(2024, 1, 1);
    
    /// <summary>
    /// Column name mappings - Change if source column names change
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
    /// Indicator mappings - Change if staging indicator names need to change
    /// </summary>
    private static class Indicators
    {
        public const string Test = "HTS_TST";
        public const string Negative = "HTS_NEG";
        public const string Positive = "HTS_POS";
        public const string Linkage = "LINKAGE_ART";
    }
    
    /// <summary>
    /// Sex mappings - Maps source sex names to staging values
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
            
            // Clear existing data if configured
            if (ClearExistingDataOnRun && triggeredBy == "system")
            {
                await ClearExistingDataAsync();
            }
            
            // Load existing records for aggregation
            var existingRecords = await ETLHelper.LoadAllExistingRecordsAsync<IndicatorValuePrevention>(_db, _logger);
            
            var (recordsRead, inserted, updated, skipped) = await ProcessHTSTestingAsync(batchId, existingRecords);

            result.Success = true;
            result.BatchId = batchId;
            result.RecordsRead = recordsRead;
            result.RecordsInserted = inserted;
            result.RecordsUpdated = updated;
            result.EndTime = DateTime.UtcNow;

            totalStopwatch.Stop();
            
            ETLHelper.LogETLSummary(_logger, "HTS ETL", recordsRead, inserted, updated, skipped, totalStopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.ErrorMessage = ex.Message;
            result.EndTime = DateTime.UtcNow;
            _logger.LogError(ex, "❌ HTS ETL failed: {Message}", ex.Message);
        }

        return result;
    }

    private async Task ClearExistingDataAsync()
    {
        try
        {
            var count = await _db.IndicatorValues_Prevention
                .Where(x => x.Indicator == Indicators.Test || 
                           x.Indicator == Indicators.Positive || 
                           x.Indicator == Indicators.Negative || 
                           x.Indicator == Indicators.Linkage)
                .CountAsync();
            
            if (count > 0)
            {
                _logger.LogInformation("Clearing {Count} existing HTS records for fresh ETL", count);
                await _db.Database.ExecuteSqlRawAsync(
                    $"DELETE FROM IndicatorValues_Prevention WHERE Indicator IN ('{Indicators.Test}', '{Indicators.Positive}', '{Indicators.Negative}', '{Indicators.Linkage}')");
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error clearing existing data, continuing with ETL");
        }
    }

    private async Task<(int RecordsRead, int Inserted, int Updated, int Skipped)> ProcessHTSTestingAsync(
        string batchId, 
        Dictionary<string, (DateTime UpdatedAt, int Id, int Value)> existingRecords)
    {
        var recordsRead = 0;
        var allRawRecords = new List<IndicatorValuePrevention>();
        var inserted = 0;
        var updated = 0;
        var skipped = 0;
        
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        var facilityRegions = await _facilityRegionService.GetFacilityRegionsAsync();
        _logger.LogInformation("Found {Count} facility-region mappings", facilityRegions.Count);

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
        _logger.LogInformation("Connected to source database");
        _logger.LogInformation("Executing query: {Query}", query);
        
        using var reader = await command.ExecuteReaderAsync();
        
        var unmappedFacilities = new HashSet<string>();
        var discrepancyCount = 0;
        var totalDiscrepancy = 0;

        while (await reader.ReadAsync())
        {
            recordsRead++;
            
            if (recordsRead % 10000 == 0)
                _logger.LogInformation("Processed {Count:N0} raw HTS records", recordsRead);

            // Read values with null handling
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

            // Map sex to staging value
            var sex = SexMappings.TryGetValue(sexName, out var mappedSex) ? mappedSex : Defaults.Sex;

            var now = DateTime.UtcNow;

            // Get all values
            var testedValue = reader.GetInt32(5);
            var negativeValue = reader.GetInt32(6);
            var positiveValue = reader.GetInt32(7);
            var linkedValue = reader.GetInt32(8);

            // Log discrepancy if configured
            if (LogHtsDiscrepancies && testedValue > 0 && (positiveValue + negativeValue) != testedValue)
            {
                discrepancyCount++;
                totalDiscrepancy += testedValue - (positiveValue + negativeValue);
                _logger.LogDebug("HTS discrepancy #{Count} at {FacilityCode}/{VisitDate}: Tested={Tested}, Pos={Pos}, Neg={Neg}, Diff={Diff}",
                    discrepancyCount, facilityCode, visitDate.Value.Date, testedValue, positiveValue, negativeValue, 
                    testedValue - (positiveValue + negativeValue));
            }

            // Add HTS_TST based on configuration
            if (testedValue > 0)
            {
                bool shouldAddTest = AlwaysIncludeHtsTest || (positiveValue + negativeValue) == testedValue;
                
                if (shouldAddTest)
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
            }

            // Add HTS_NEG if negative > 0
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

            // Add HTS_POS if positive > 0
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

            // Add LINKAGE_ART if linked > 0
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

            // Aggregate when we reach batch size
            if (allRawRecords.Count >= _batchSize)
            {
                var aggregated = ETLHelper.AggregateRecords(allRawRecords);
                var (ins, upd, skp) = await ETLHelper.UpsertAggregatedRecordsAsync<IndicatorValuePrevention>(
                    aggregated, _db, _logger, batchId, existingRecords);
                
                inserted += ins;
                updated += upd;
                skipped += skp;
                allRawRecords.Clear();
            }
        }

        // Log summary of unmapped facilities
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
            var (ins, upd, skp) = await ETLHelper.UpsertAggregatedRecordsAsync<IndicatorValuePrevention>(
                aggregated, _db, _logger, batchId, existingRecords);
            
            inserted += ins;
            updated += upd;
            skipped += skp;
        }

        stopwatch.Stop();
        ETLHelper.LogETLSummary(_logger, "HTS Detail", recordsRead, inserted, updated, skipped, stopwatch.ElapsedMilliseconds);

        return (recordsRead, inserted, updated, skipped);
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