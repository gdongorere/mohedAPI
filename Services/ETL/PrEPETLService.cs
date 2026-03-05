using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.ETL;
using Eswatini.Health.Api.Models.Staging;

namespace Eswatini.Health.Api.Services.ETL;

public class PrEPETLService : IPrEPETLService
{
    private readonly StagingDbContext _db;
    private readonly IConfiguration _configuration;
    private readonly ILogger<PrEPETLService> _logger;
    private readonly IFacilityRegionService _facilityRegionService;
    private readonly string _sourceConnectionString;
    private readonly int _batchSize;

    public PrEPETLService(
        StagingDbContext db,
        IConfiguration configuration,
        ILogger<PrEPETLService> logger,
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
            JobName = "PrEP ETL",
            StartTime = DateTime.UtcNow
        };

        var batchId = $"PREP_{DateTime.UtcNow:yyyyMMdd_HHmmss}_UTC";
        var totalStopwatch = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            _logger.LogInformation("🚀 Starting PrEP ETL with batch {BatchId}", batchId);
            
            // Clear existing data if this is a fresh run
            if (triggeredBy == "system" && await ShouldClearExistingData())
            {
                _logger.LogInformation("Clearing existing PrEP data for fresh ETL");
                await _db.Database.ExecuteSqlRawAsync("DELETE FROM IndicatorValues_Prevention WHERE Indicator LIKE 'PREP_%'");
            }
            
            // Load existing records for aggregation
            var existingRecords = await ETLHelper.LoadAllExistingRecordsAsync<IndicatorValuePrevention>(_db, _logger);

            // Process LineListingsPrep (primary source for initiations)
            var (prepRecordsRead, prepInserted, prepUpdated, prepSkipped) = await ProcessLineListingsPrepAsync(batchId, existingRecords);
            
            // Process aPrepDetail (secondary source for seroconversions only)
            var (detailRecordsRead, detailInserted, detailUpdated, detailSkipped) = await ProcessAPrepDetailAsync(batchId, existingRecords);

            result.Success = true;
            result.BatchId = batchId;
            result.RecordsRead = prepRecordsRead + detailRecordsRead;
            result.RecordsInserted = prepInserted + detailInserted;
            result.RecordsUpdated = prepUpdated + detailUpdated;
            result.EndTime = DateTime.UtcNow;

            totalStopwatch.Stop();
            
            _logger.LogInformation("");
            _logger.LogInformation("═══════════════════════════════════════════════════════════");
            _logger.LogInformation("📊 PrEP ETL FINAL SUMMARY");
            _logger.LogInformation("═══════════════════════════════════════════════════════════");
            _logger.LogInformation($"  Raw Records Read:      {result.RecordsRead,15:N0}");
            _logger.LogInformation($"  Aggregated Inserted:   {result.RecordsInserted,15:N0}");
            _logger.LogInformation($"  Aggregated Updated:    {result.RecordsUpdated,15:N0}");
            _logger.LogInformation($"  Unchanged Groups:      {result.RecordsRead - result.RecordsInserted - result.RecordsUpdated,15:N0}");
            _logger.LogInformation($"  Batch ID:              {batchId}");
            _logger.LogInformation($"  Time Elapsed:          {totalStopwatch.ElapsedMilliseconds,15:N0}ms");
            _logger.LogInformation("═══════════════════════════════════════════════════════════");
            _logger.LogInformation("");
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.ErrorMessage = ex.Message;
            result.EndTime = DateTime.UtcNow;
            _logger.LogError(ex, "❌ PrEP ETL failed: {Message}", ex.Message);
        }

        return result;
    }

    private async Task<bool> ShouldClearExistingData()
    {
        var count = await _db.IndicatorValues_Prevention
            .Where(x => x.Indicator.StartsWith("PREP_"))
            .CountAsync();
        return count > 0;
    }

    private async Task<(int RecordsRead, int Inserted, int Updated, int Skipped)> ProcessLineListingsPrepAsync(
        string batchId, 
        Dictionary<string, (DateTime UpdatedAt, int Id, int Value)> existingRecords)
    {
        var recordsRead = 0;
        var allRawRecords = new List<IndicatorValuePrevention>();
        var inserted = 0;
        var updated = 0;
        var skipped = 0;

        var facilityRegions = await _facilityRegionService.GetFacilityRegionsAsync();
        var unmappedFacilities = new HashSet<string>();

        var query = @"
            SELECT 
                FacilityCode,
                VisitDate,
                AgeGroup,
                SexName,
                PopulationType,
                ISNULL(PrEP_Initiation, 0) as PrEP_Initiation,
                ISNULL(PrEP_TestedForHIV, 0) as PrEP_TestedForHIV,
                ISNULL(PrEP_TestedNegative, 0) as PrEP_TestedNegative,
                ISNULL(PrEP_TestedPositive, 0) as PrEP_TestedPositive,
                ISNULL(PrEP_InitiatedOnART, 0) as PrEP_InitiatedOnART
            FROM [All_Dataset].[dbo].[LineListingsPrep]
            WHERE VisitDate IS NOT NULL
            ORDER BY VisitDate";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        using var reader = await command.ExecuteReaderAsync();

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        while (await reader.ReadAsync())
        {
            recordsRead++;
            
            if (recordsRead % 1000 == 0)
                _logger.LogInformation("Processed {Count:N0} raw LineListingsPrep records", recordsRead);

            var facilityCode = reader.IsDBNull(0) ? null : reader.GetString(0);
            if (string.IsNullOrEmpty(facilityCode))
                continue;

            var visitDate = reader.IsDBNull(1) ? (DateTime?)null : reader.GetDateTime(1);
            if (!visitDate.HasValue)
                continue;

            var ageGroup = reader.IsDBNull(2) ? "Unknown" : reader.GetString(2);
            var sexName = reader.IsDBNull(3) ? "Other" : reader.GetString(3);
            var populationType = reader.IsDBNull(4) ? null : reader.GetString(4);
            
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

            // PrEP Initiations (PREP_NEW)
            var initiationValue = reader.GetInt32(5);
            if (initiationValue > 0)
            {
                allRawRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_NEW",
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = initiationValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // PrEP Tested for HIV (PREP_TESTED)
            var testedValue = reader.GetInt32(6);
            if (testedValue > 0)
            {
                allRawRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_TESTED",
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = testedValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // PrEP Tested Negative (PREP_NEG)
            var negativeValue = reader.GetInt32(7);
            if (negativeValue > 0)
            {
                allRawRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_NEG",
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = negativeValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // PrEP Tested Positive (PREP_POS) - Seroconversion
            var positiveValue = reader.GetInt32(8);
            if (positiveValue > 0)
            {
                allRawRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_POS",
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = positiveValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });

                /*
                // it's causing double-counting
                // Add as seroconversion (only from LineListingsPrep)
                allRawRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_SEROCONVERSION",
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = positiveValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });*/
            }

            // PrEP Initiated on ART (PREP_LINKAGE_ART)
            var linkedValue = reader.GetInt32(9);
            if (linkedValue > 0)
            {
                allRawRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_LINKAGE_ART",
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
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
            _logger.LogWarning("Found {Count} unmapped facilities in LineListingsPrep: {Facilities}", 
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
        ETLHelper.LogETLSummary(_logger, "LineListingsPrep", recordsRead, inserted, updated, skipped, stopwatch.ElapsedMilliseconds);

        return (recordsRead, inserted, updated, skipped);
    }

    private async Task<(int RecordsRead, int Inserted, int Updated, int Skipped)> ProcessAPrepDetailAsync(
        string batchId, 
        Dictionary<string, (DateTime UpdatedAt, int Id, int Value)> existingRecords)
    {
        var recordsRead = 0;
        var allRawRecords = new List<IndicatorValuePrevention>();
        var inserted = 0;
        var updated = 0;
        var skipped = 0;

        var facilityRegions = await _facilityRegionService.GetFacilityRegionsAsync();
        var unmappedFacilities = new HashSet<string>();

        var query = @"
            SELECT 
                FacilityCode,
                VisitDate,
                AgeGroup,
                Sex,
                PopulationType,
                Seroconverted,
                InitiatedOnART
            FROM [All_Dataset].[dbo].[aPrepDetail]
            WHERE VisitDate IS NOT NULL
            ORDER BY VisitDate";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        using var reader = await command.ExecuteReaderAsync();

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        while (await reader.ReadAsync())
        {
            recordsRead++;
            
            if (recordsRead % 10000 == 0)
                _logger.LogInformation("Processed {Count:N0} raw aPrepDetail records", recordsRead);

            var facilityCode = reader.IsDBNull(0) ? null : reader.GetString(0);
            if (string.IsNullOrEmpty(facilityCode))
                continue;

            var visitDate = reader.IsDBNull(1) ? (DateTime?)null : reader.GetDateTime(1);
            if (!visitDate.HasValue)
                continue;

            var ageGroup = reader.IsDBNull(2) ? "Unknown" : reader.GetString(2);
            
            int? sexValue = null;
            if (!reader.IsDBNull(3))
            {
                sexValue = reader.GetByte(3);
            }
            
            var populationType = reader.IsDBNull(4) ? null : reader.GetString(4);
            
            if (!facilityRegions.TryGetValue(facilityCode, out var regionId))
            {
                unmappedFacilities.Add(facilityCode);
                continue;
            }

            var sex = sexValue switch
            {
                1 => "M",
                2 => "F",
                _ => "Other"
            };

            var now = DateTime.UtcNow;

            // Handle Seroconverted column - ONLY add seroconversions, not other indicators
            // Handle Seroconverted column - ONLY add from aPrepDetail (36 records)
if (!reader.IsDBNull(5))
{
    var seroconverted = reader.GetString(5).Trim().ToLower();
    if (seroconverted == "1" || seroconverted == "true" || seroconverted == "yes")
    {
        allRawRecords.Add(new IndicatorValuePrevention
        {
            Indicator = "PREP_SEROCONVERSION",  // Only add this indicator
            RegionId = regionId,
            VisitDate = visitDate.Value.Date,
            AgeGroup = ageGroup,
            Sex = sex,
            PopulationType = populationType,
            Value = 1,
            CreatedAt = now,
            UpdatedAt = now
        });
    }
}

            // Handle InitiatedOnART column (PREP_LINKAGE_ART)
            if (!reader.IsDBNull(6))
            {
                var initiatedOnArt = reader.GetString(6).Trim().ToLower();
                if (initiatedOnArt == "1" || initiatedOnArt == "true" || initiatedOnArt == "yes")
                {
                    allRawRecords.Add(new IndicatorValuePrevention
                    {
                        Indicator = "PREP_LINKAGE_ART",
                        RegionId = regionId,
                        VisitDate = visitDate.Value.Date,
                        AgeGroup = ageGroup,
                        Sex = sex,
                        PopulationType = populationType,
                        Value = 1,
                        CreatedAt = now,
                        UpdatedAt = now
                    });
                }
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
            _logger.LogWarning("Found {Count} unmapped facilities in aPrepDetail: {Facilities}", 
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
        ETLHelper.LogETLSummary(_logger, "aPrepDetail", recordsRead, inserted, updated, skipped, stopwatch.ElapsedMilliseconds);

        return (recordsRead, inserted, updated, skipped);
    }

    public async Task<int> GetRecordCountForPeriodAsync(DateTime startDate, DateTime endDate)
    {
        var query = @"
            SELECT COUNT(*) 
            FROM [All_Dataset].[dbo].[LineListingsPrep]
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