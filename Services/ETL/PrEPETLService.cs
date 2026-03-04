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
    private readonly string _sourceConnectionString;
    private readonly int _batchSize;

    public PrEPETLService(
        StagingDbContext db,
        IConfiguration configuration,
        ILogger<PrEPETLService> logger)
    {
        _db = db;
        _configuration = configuration;
        _logger = logger;
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
            
            // Load ALL existing records (no date filter!)
            var existingRecords = await ETLHelper.LoadAllExistingRecordsAsync<IndicatorValuePrevention>(_db, _logger);

            // Process LineListingsPrep (primary source)
            var (prepRecordsRead, prepInserted, prepUpdated, prepSkipped) = await ProcessLineListingsPrepAsync(batchId, existingRecords);
            
            // Process aPrepDetail (secondary source)
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
            _logger.LogInformation($"  Total Records Read:    {result.RecordsRead,15:N0}");
            _logger.LogInformation($"  Total Records Inserted: {result.RecordsInserted,15:N0}");
            _logger.LogInformation($"  Total Records Updated:  {result.RecordsUpdated,15:N0}");
            _logger.LogInformation($"  Total Unchanged:        {result.RecordsRead - result.RecordsInserted - result.RecordsUpdated,15:N0}");
            _logger.LogInformation($"  Batch ID:               {batchId}");
            _logger.LogInformation($"  Time Elapsed:           {totalStopwatch.ElapsedMilliseconds,15:N0}ms");
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

    private async Task<(int RecordsRead, int Inserted, int Updated, int Skipped)> ProcessLineListingsPrepAsync(
        string batchId, 
        Dictionary<string, (DateTime UpdatedAt, int Id)> existingRecords)
    {
        var recordsRead = 0;
        var inserted = 0;
        var updated = 0;
        var skipped = 0;

        var facilityRegions = await GetFacilityRegionsAsync();

        var query = @"
            SELECT 
                FacilityCode,
                VisitDate,
                AgeGroup,
                SexName,
                PopulationType,
                PrEP_Initiation,
                PrEP_TestedForHIV,
                PrEP_TestedNegative,
                PrEP_TestedPositive,
                PrEP_InitiatedOnART,
                CurrentPrepMethod
            FROM [All_Dataset].[dbo].[LineListingsPrep]
            WHERE VisitDate IS NOT NULL
            ORDER BY VisitDate";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        using var reader = await command.ExecuteReaderAsync();

        var allRecords = new List<IndicatorValuePrevention>();
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        while (await reader.ReadAsync())
        {
            recordsRead++;
            
            if (recordsRead % 1000 == 0)
                _logger.LogInformation("Processed {Count:N0} LineListingsPrep records", recordsRead);

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
                continue;

            var sex = sexName.ToUpper() switch
            {
                "MALE" => "M",
                "FEMALE" => "F",
                _ => "Other"
            };

            var now = DateTime.UtcNow;

            // PrEP Initiations
            if (!reader.IsDBNull(5) && reader.GetInt32(5) == 1)
            {
                allRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_NEW",
                    RegionId = regionId,
                    VisitDate = visitDate.Value,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = 1,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // PrEP Tested for HIV
            if (!reader.IsDBNull(6) && reader.GetInt32(6) == 1)
            {
                allRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_TESTED",
                    RegionId = regionId,
                    VisitDate = visitDate.Value,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = 1,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // PrEP Tested Negative
            if (!reader.IsDBNull(7) && reader.GetInt32(7) == 1)
            {
                allRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_NEG",
                    RegionId = regionId,
                    VisitDate = visitDate.Value,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = 1,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // PrEP Tested Positive (Seroconversion)
            if (!reader.IsDBNull(8) && reader.GetInt32(8) == 1)
            {
                allRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_POS",
                    RegionId = regionId,
                    VisitDate = visitDate.Value,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = 1,
                    CreatedAt = now,
                    UpdatedAt = now
                });

                allRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_SEROCONVERSION",
                    RegionId = regionId,
                    VisitDate = visitDate.Value,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = 1,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // PrEP Initiated on ART (Linkage)
            if (!reader.IsDBNull(9) && reader.GetInt32(9) == 1)
            {
                allRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_LINKAGE_ART",
                    RegionId = regionId,
                    VisitDate = visitDate.Value,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = 1,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            if (allRecords.Count >= _batchSize)
            {
                var (ins, upd, skp) = await ETLHelper.ProcessRecordsAsync(
                    allRecords, _db, _logger, batchId, existingRecords);
                inserted += ins;
                updated += upd;
                skipped += skp;
                allRecords.Clear();
            }
        }

        if (allRecords.Any())
        {
            var (ins, upd, skp) = await ETLHelper.ProcessRecordsAsync(
                allRecords, _db, _logger, batchId, existingRecords);
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
        Dictionary<string, (DateTime UpdatedAt, int Id)> existingRecords)
    {
        var recordsRead = 0;
        var inserted = 0;
        var updated = 0;
        var skipped = 0;

        var facilityRegions = await GetFacilityRegionsAsync();

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

        var allRecords = new List<IndicatorValuePrevention>();
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        while (await reader.ReadAsync())
        {
            recordsRead++;
            
            if (recordsRead % 10000 == 0)
                _logger.LogInformation("Processed {Count:N0} aPrepDetail records", recordsRead);

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
                continue;

            var sex = sexValue switch
            {
                1 => "M",
                2 => "F",
                _ => "Other"
            };

            var now = DateTime.UtcNow;

            // Handle Seroconverted column
            if (!reader.IsDBNull(5))
            {
                var seroconverted = reader.GetString(5).Trim().ToLower();
                if (seroconverted == "1" || seroconverted == "true" || seroconverted == "yes")
                {
                    allRecords.Add(new IndicatorValuePrevention
                    {
                        Indicator = "PREP_SEROCONVERSION",
                        RegionId = regionId,
                        VisitDate = visitDate.Value,
                        AgeGroup = ageGroup,
                        Sex = sex,
                        PopulationType = populationType,
                        Value = 1,
                        CreatedAt = now,
                        UpdatedAt = now
                    });
                }
            }

            // Handle InitiatedOnART column
            if (!reader.IsDBNull(6))
            {
                var initiatedOnArt = reader.GetString(6).Trim().ToLower();
                if (initiatedOnArt == "1" || initiatedOnArt == "true" || initiatedOnArt == "yes")
                {
                    allRecords.Add(new IndicatorValuePrevention
                    {
                        Indicator = "PREP_LINKAGE_ART",
                        RegionId = regionId,
                        VisitDate = visitDate.Value,
                        AgeGroup = ageGroup,
                        Sex = sex,
                        PopulationType = populationType,
                        Value = 1,
                        CreatedAt = now,
                        UpdatedAt = now
                    });
                }
            }

            if (allRecords.Count >= _batchSize)
            {
                var (ins, upd, skp) = await ETLHelper.ProcessRecordsAsync(
                    allRecords, _db, _logger, batchId, existingRecords);
                inserted += ins;
                updated += upd;
                skipped += skp;
                allRecords.Clear();
            }
        }

        if (allRecords.Any())
        {
            var (ins, upd, skp) = await ETLHelper.ProcessRecordsAsync(
                allRecords, _db, _logger, batchId, existingRecords);
            inserted += ins;
            updated += upd;
            skipped += skp;
        }

        stopwatch.Stop();
        ETLHelper.LogETLSummary(_logger, "aPrepDetail", recordsRead, inserted, updated, skipped, stopwatch.ElapsedMilliseconds);

        return (recordsRead, inserted, updated, skipped);
    }

    private async Task<Dictionary<string, int>> GetFacilityRegionsAsync()
    {
        var result = new Dictionary<string, int>();
        
        var query = @"
            SELECT DISTINCT FacilityCode, Region 
            FROM [All_Dataset].[dbo].[aPrepDetail] 
            WHERE FacilityCode IS NOT NULL AND Region IS NOT NULL";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        using var reader = await command.ExecuteReaderAsync();
        
        var regionMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            { "Hhohho", 1 },
            { "Manzini", 2 },
            { "Shiselweni", 3 },
            { "Lubombo", 4 }
        };
        
        while (await reader.ReadAsync())
        {
            var facilityCode = reader.GetString(0);
            var regionName = reader.GetString(1);
            
            if (regionMap.TryGetValue(regionName, out var regionId))
            {
                result[facilityCode] = regionId;
            }
        }

        _logger.LogInformation("Found {Count} facility-region mappings", result.Count);
        return result;
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