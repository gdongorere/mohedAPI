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
            
            // Clear existing data if this is a fresh run
            if (triggeredBy == "system" && await ShouldClearExistingData())
            {
                _logger.LogInformation("Clearing existing HTS data for fresh ETL");
                await _db.Database.ExecuteSqlRawAsync("DELETE FROM IndicatorValues_Prevention WHERE Indicator LIKE 'HTS_%' OR Indicator = 'LINKAGE_ART'");
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

    private async Task<bool> ShouldClearExistingData()
    {
        // Check if we have any data - if not, no need to clear
        var count = await _db.IndicatorValues_Prevention
            .Where(x => x.Indicator.StartsWith("HTS_") || x.Indicator == "LINKAGE_ART")
            .CountAsync();
        
        return count > 0;
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

        var query = @"
            SELECT 
                FacilityCode,
                VisitDate,
                AgeGroup,
                SexName,
                PopulationGroup,
                ISNULL(HTS_TestedForHIV, 0) as HTS_TestedForHIV,
                ISNULL(HTS_TestedNegative, 0) as HTS_TestedNegative,
                ISNULL(HTS_TestedPositive, 0) as HTS_TestedPositive,
                ISNULL(HTS_TestedPositiveInitiatedOnART, 0) as HTS_TestedPositiveInitiatedOnART
            FROM [All_Dataset].[dbo].[tmpHTSTestedDetail]
            WHERE VisitDate IS NOT NULL
            ORDER BY VisitDate";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        _logger.LogInformation("Connected to source database");
        
        using var reader = await command.ExecuteReaderAsync();
        _logger.LogInformation("Executed query, starting to read records");

        var unmappedFacilities = new HashSet<string>();

        while (await reader.ReadAsync())
        {
            recordsRead++;
            
            if (recordsRead % 10000 == 0)
                _logger.LogInformation("Processed {Count:N0} raw HTS records", recordsRead);

            var facilityCode = reader.IsDBNull(0) ? null : reader.GetString(0);
            if (string.IsNullOrEmpty(facilityCode))
                continue;

            var visitDate = reader.IsDBNull(1) ? (DateTime?)null : reader.GetDateTime(1);
            if (!visitDate.HasValue)
                continue;

            var ageGroup = reader.IsDBNull(2) ? "Unknown" : reader.GetString(2);
            var sexName = reader.IsDBNull(3) ? "Other" : reader.GetString(3);
            var populationGroup = reader.IsDBNull(4) ? null : reader.GetString(4);
            
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

            // HTS Tested - use the actual count
            var testedValue = reader.GetInt32(5);
            if (testedValue > 0)
            {
                allRawRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "HTS_TST",
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

            // HTS Negative
            var negativeValue = reader.GetInt32(6);
            if (negativeValue > 0)
            {
                allRawRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "HTS_NEG",
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

            // HTS Positive
            if (!reader.IsDBNull(7))
            {
                var positiveValue = reader.GetInt32(7);
                if (positiveValue > 0)
                {
                    allRawRecords.Add(new IndicatorValuePrevention
                    {
                        Indicator = "HTS_POS",
                        RegionId = regionId,
                        VisitDate = visitDate.Value.Date,
                        AgeGroup = ageGroup,
                        Sex = sex,
                        PopulationType = populationGroup,
                        Value = positiveValue,  // Use actual value
                        CreatedAt = now,
                        UpdatedAt = now
                    });
                }
            }

            // Linkage to ART
            var linkedValue = reader.GetInt32(8);
            if (linkedValue > 0)
            {
                allRawRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "LINKAGE_ART",
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

        // Log unmapped facilities
        if (unmappedFacilities.Any())
        {
            _logger.LogWarning("Found {Count} unmapped facilities: {Facilities}", 
                unmappedFacilities.Count, string.Join(", ", unmappedFacilities.Take(20)));
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