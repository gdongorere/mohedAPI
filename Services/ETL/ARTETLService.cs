using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.ETL;
using Eswatini.Health.Api.Models.Staging;

namespace Eswatini.Health.Api.Services.ETL;

public class ARTETLService : IARTETLService
{
    private readonly StagingDbContext _db;
    private readonly IConfiguration _configuration;
    private readonly ILogger<ARTETLService> _logger;
    private readonly IFacilityRegionService _facilityRegionService;
    private readonly string _sourceConnectionString;
    private readonly int _batchSize;

    public ARTETLService(
        StagingDbContext db,
        IConfiguration configuration,
        ILogger<ARTETLService> logger,
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
            JobName = "ART ETL",
            StartTime = DateTime.UtcNow
        };

        var batchId = $"ART_{DateTime.UtcNow:yyyyMMdd_HHmmss}_UTC";
        var totalStopwatch = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            _logger.LogInformation("🚀 Starting ART ETL with batch {BatchId}", batchId);
            
            // Clear existing data if this is a fresh run
            if (triggeredBy == "system" && await ShouldClearExistingData())
            {
                _logger.LogInformation("Clearing existing ART data for fresh ETL");
                await _db.Database.ExecuteSqlRawAsync("DELETE FROM IndicatorValues_HIV");
            }
            
            // Load existing records for aggregation
            var existingRecords = await ETLHelper.LoadAllExistingRecordsAsync<IndicatorValueHIV>(_db, _logger);

            var (recordsRead, inserted, updated, skipped) = await ProcessARTOutcomesAsync(batchId, existingRecords);

            result.Success = true;
            result.BatchId = batchId;
            result.RecordsRead = recordsRead;
            result.RecordsInserted = inserted;
            result.RecordsUpdated = updated;
            result.EndTime = DateTime.UtcNow;

            totalStopwatch.Stop();
            
            ETLHelper.LogETLSummary(_logger, "ART ETL", recordsRead, inserted, updated, skipped, totalStopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.ErrorMessage = ex.Message;
            result.EndTime = DateTime.UtcNow;
            _logger.LogError(ex, "❌ ART ETL failed: {Message}", ex.Message);
        }

        return result;
    }

    private async Task<bool> ShouldClearExistingData()
    {
        var count = await _db.IndicatorValues_HIV.CountAsync();
        return count > 0;
    }

    private async Task<(int RecordsRead, int Inserted, int Updated, int Skipped)> ProcessARTOutcomesAsync(
        string batchId, 
        Dictionary<string, (DateTime UpdatedAt, int Id, int Value)> existingRecords)
    {
        var recordsRead = 0;
        var allRawRecords = new List<IndicatorValueHIV>();
        var inserted = 0;
        var updated = 0;
        var skipped = 0;

        var facilityRegions = await _facilityRegionService.GetFacilityRegionsAsync();
        _logger.LogInformation("Found {Count} facility-region mappings", facilityRegions.Count);

        var unmappedFacilities = new HashSet<string>();

        var query = @"
            SELECT 
                FacilityCode,
                ReportingPeriod,
                AgeGroup,
                SexName,
                ISNULL(TX_CURR, 0) as TX_CURR,
                ISNULL(TX_VLTested, 0) as TX_VLTested,
                ISNULL(TX_VLSuppressed, 0) as TX_VLSuppressed,
                ISNULL(TX_VLUndetectable, 0) as TX_VLUndetectable
            FROM [All_Dataset].[dbo].[tmpARTTXOutcomes]
            WHERE ReportingPeriod IS NOT NULL
            ORDER BY ReportingPeriod DESC";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        _logger.LogInformation("Connected to source database");
        
        using var reader = await command.ExecuteReaderAsync();
        _logger.LogInformation("Executed query, starting to read records");

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        while (await reader.ReadAsync())
        {
            recordsRead++;
            
            if (recordsRead % 10000 == 0)
                _logger.LogInformation("Processed {Count:N0} raw ART records", recordsRead);

            var facilityCode = reader.IsDBNull(0) ? null : reader.GetString(0);
            if (string.IsNullOrEmpty(facilityCode))
                continue;

            var reportingPeriod = reader.IsDBNull(1) ? (DateTime?)null : reader.GetDateTime(1);
            if (!reportingPeriod.HasValue)
                continue;

            var ageGroup = reader.IsDBNull(2) ? "Unknown" : reader.GetString(2);
            var sexName = reader.IsDBNull(3) ? "Other" : reader.GetString(3);
            
            if (!facilityRegions.TryGetValue(facilityCode, out var regionId))
            {
                unmappedFacilities.Add(facilityCode);
                continue;
            }

            var sex = sexName.ToUpper() switch
            {
                "MALE" => "M",
                "FEMALE" => "F",
                _ => "Other"
            };

            var now = DateTime.UtcNow;

            // TX_CURR - Use actual value from column
            var currValue = reader.GetInt32(4);
            if (currValue > 0)
            {
                allRawRecords.Add(new IndicatorValueHIV
                {
                    Indicator = "TX_CURR",
                    RegionId = regionId,
                    VisitDate = reportingPeriod.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = null,
                    Value = currValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // TX_VL_TESTED
            var testedValue = reader.GetInt32(5);
            if (testedValue > 0)
            {
                allRawRecords.Add(new IndicatorValueHIV
                {
                    Indicator = "TX_VL_TESTED",
                    RegionId = regionId,
                    VisitDate = reportingPeriod.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = null,
                    Value = testedValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // TX_VL_SUPPRESSED
            var suppressedValue = reader.GetInt32(6);
            if (suppressedValue > 0)
            {
                allRawRecords.Add(new IndicatorValueHIV
                {
                    Indicator = "TX_VL_SUPPRESSED",
                    RegionId = regionId,
                    VisitDate = reportingPeriod.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = null,
                    Value = suppressedValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // TX_VL_UNDETECTABLE
            var undetectableValue = reader.GetInt32(7);
            if (undetectableValue > 0)
            {
                allRawRecords.Add(new IndicatorValueHIV
                {
                    Indicator = "TX_VL_UNDETECTABLE",
                    RegionId = regionId,
                    VisitDate = reportingPeriod.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = null,
                    Value = undetectableValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // Aggregate when we reach batch size
            if (allRawRecords.Count >= _batchSize)
            {
                var aggregated = ETLHelper.AggregateRecords(allRawRecords);
                var (ins, upd, skp) = await ETLHelper.UpsertAggregatedRecordsAsync<IndicatorValueHIV>(
                    aggregated, _db, _logger, batchId, existingRecords);
                
                inserted += ins;
                updated += upd;
                skipped += skp;
                allRawRecords.Clear();
            }
        }

        // Log unmapped facilities
        if (unmappedFacilities.Any())
        {
            _logger.LogWarning("Found {Count} unmapped facilities in ART: {Facilities}", 
                unmappedFacilities.Count, string.Join(", ", unmappedFacilities.Take(20)));
        }

        // Process remaining records
        if (allRawRecords.Any())
        {
            var aggregated = ETLHelper.AggregateRecords(allRawRecords);
            var (ins, upd, skp) = await ETLHelper.UpsertAggregatedRecordsAsync<IndicatorValueHIV>(
                aggregated, _db, _logger, batchId, existingRecords);
            
            inserted += ins;
            updated += upd;
            skipped += skp;
        }

        stopwatch.Stop();
        ETLHelper.LogETLSummary(_logger, "ART Detail", recordsRead, inserted, updated, skipped, stopwatch.ElapsedMilliseconds);

        return (recordsRead, inserted, updated, skipped);
    }

    public async Task<int> GetRecordCountForPeriodAsync(DateTime startDate, DateTime endDate)
    {
        var query = @"
            SELECT COUNT(*) 
            FROM [All_Dataset].[dbo].[tmpARTTXOutcomes]
            WHERE ReportingPeriod >= @StartDate AND ReportingPeriod < @EndDate";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        command.Parameters.AddWithValue("@StartDate", startDate);
        command.Parameters.AddWithValue("@EndDate", endDate);
        
        await connection.OpenAsync();
        var result = await command.ExecuteScalarAsync();
        return result != null ? Convert.ToInt32(result) : 0;
    }
}