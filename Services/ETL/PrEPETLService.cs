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
            
            // Load existing records for deduplication (last 90 days to catch updates)
            var existingRecords = await ETLHelper.LoadExistingRecordsAsync<IndicatorValuePrevention>(_db, DateTime.UtcNow.AddDays(-90));
            _logger.LogInformation("📊 Loaded {Count:N0} existing records for deduplication", existingRecords.Count);

            // Process LineListingsPrep (primary source)
            var (prepRecordsRead, prepRecordsInserted) = await ProcessLineListingsPrepAsync(batchId, existingRecords);
            
            // Process aPrepDetail (secondary source)
            var (detailRecordsRead, detailRecordsInserted) = await ProcessAPrepDetailAsync(batchId, existingRecords);

            result.Success = true;
            result.BatchId = batchId;
            result.RecordsRead = prepRecordsRead + detailRecordsRead;
            result.RecordsInserted = prepRecordsInserted + detailRecordsInserted;
            result.EndTime = DateTime.UtcNow;

            totalStopwatch.Stop();
            
            _logger.LogInformation("");
            _logger.LogInformation("═══════════════════════════════════════════════════════════");
            _logger.LogInformation("📊 PrEP ETL FINAL SUMMARY");
            _logger.LogInformation("═══════════════════════════════════════════════════════════");
            _logger.LogInformation("  Total Records Read:    {TotalRead,10:N0}", result.RecordsRead);
            _logger.LogInformation("  Total Records Inserted: {TotalInserted,10:N0}", result.RecordsInserted);
            _logger.LogInformation("  Batch ID:               {BatchId}", batchId);
            _logger.LogInformation("  Time Elapsed:           {Elapsed,10:N0}ms", totalStopwatch.ElapsedMilliseconds);
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

    private async Task<(int RecordsRead, int RecordsInserted)> ProcessLineListingsPrepAsync(
        string batchId, 
        Dictionary<string, (DateTime UpdatedAt, int Id)> existingRecords)
    {
        var recordsRead = 0;
        var recordsInserted = 0;
        var duplicates = 0;

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
                var (inserted, dup, _) = await ETLHelper.BatchInsertWithDeduplicationAsync(
                    allRecords, _db, _logger, batchId, existingRecords);
                recordsInserted += inserted;
                duplicates += dup;
                allRecords.Clear();
            }
        }

        if (allRecords.Any())
        {
            var (inserted, dup, _) = await ETLHelper.BatchInsertWithDeduplicationAsync(
                allRecords, _db, _logger, batchId, existingRecords);
            recordsInserted += inserted;
            duplicates += dup;
        }

        stopwatch.Stop();
        ETLHelper.LogETLSummary(_logger, "LineListingsPrep", recordsRead, recordsInserted, duplicates, stopwatch.ElapsedMilliseconds);

        return (recordsRead, recordsInserted);
    }

    private async Task<(int RecordsRead, int RecordsInserted)> ProcessAPrepDetailAsync(
        string batchId, 
        Dictionary<string, (DateTime UpdatedAt, int Id)> existingRecords)
    {
        var recordsRead = 0;
        var recordsInserted = 0;
        var duplicates = 0;

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

            // Handle NULL values safely
            var facilityCode = reader.IsDBNull(0) ? null : reader.GetString(0);
            if (string.IsNullOrEmpty(facilityCode))
                continue;

            var visitDate = reader.IsDBNull(1) ? (DateTime?)null : reader.GetDateTime(1);
            if (!visitDate.HasValue)
                continue;

            var ageGroup = reader.IsDBNull(2) ? "Unknown" : reader.GetString(2);
            
            // Handle Sex column correctly - it's TINYINT in database
            int? sexValue = null;
            if (!reader.IsDBNull(3))
            {
                // SQL Server TINYINT maps to byte in C#
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

            // Handle Seroconverted column correctly - it's nvarchar
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

            // Handle InitiatedOnART column correctly - it's nvarchar
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
                var (inserted, dup, _) = await ETLHelper.BatchInsertWithDeduplicationAsync(
                    allRecords, _db, _logger, batchId, existingRecords);
                recordsInserted += inserted;
                duplicates += dup;
                allRecords.Clear();
            }
        }

        if (allRecords.Any())
        {
            var (inserted, dup, _) = await ETLHelper.BatchInsertWithDeduplicationAsync(
                allRecords, _db, _logger, batchId, existingRecords);
            recordsInserted += inserted;
            duplicates += dup;
        }

        stopwatch.Stop();
        ETLHelper.LogETLSummary(_logger, "aPrepDetail", recordsRead, recordsInserted, duplicates, stopwatch.ElapsedMilliseconds);

        return (recordsRead, recordsInserted);
    }

    private async Task<int> BatchInsertPreventionAsync(List<IndicatorValuePrevention> records, string batchId)
    {
        await _db.IndicatorValues_Prevention.AddRangeAsync(records);
        return await _db.SaveChangesAsync();
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