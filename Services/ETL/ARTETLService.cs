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
    private readonly string _sourceConnectionString;
    private readonly int _batchSize;

    public ARTETLService(
        StagingDbContext db,
        IConfiguration configuration,
        ILogger<ARTETLService> logger)
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
            JobName = "ART ETL",
            StartTime = DateTime.UtcNow
        };

        var batchId = $"ART_{DateTime.UtcNow:yyyyMMdd_HHmmss}_UTC";
        var totalStopwatch = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            _logger.LogInformation("🚀 Starting ART ETL with batch {BatchId}", batchId);
            
            // Load existing records for deduplication (last 90 days to catch updates)
            var existingRecords = await ETLHelper.LoadExistingRecordsAsync<IndicatorValueHIV>(_db, _logger, DateTime.UtcNow.AddDays(-90));
            _logger.LogInformation("📊 Loaded {Count:N0} existing records for deduplication", existingRecords.Count);

            var (recordsRead, recordsInserted) = await ProcessARTOutcomesAsync(batchId, existingRecords);

            result.Success = true;
            result.BatchId = batchId;
            result.RecordsRead = recordsRead;
            result.RecordsInserted = recordsInserted;
            result.EndTime = DateTime.UtcNow;

            totalStopwatch.Stop();
            
            ETLHelper.LogETLSummary(_logger, "ART ETL", recordsRead, recordsInserted, 0, totalStopwatch.ElapsedMilliseconds);
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

    private async Task<(int RecordsRead, int RecordsInserted)> ProcessARTOutcomesAsync(
        string batchId, 
        Dictionary<string, (DateTime UpdatedAt, int Id)> existingRecords)
    {
        var recordsRead = 0;
        var recordsInserted = 0;
        var duplicates = 0;

        var facilityRegions = await GetFacilityRegionsAsync();
        _logger.LogInformation("Found {Count} facility-region mappings", facilityRegions.Count);

        var query = @"
            SELECT 
                FacilityCode,
                ReportingPeriod,
                AgeGroup,
                SexName,
                TX_CURR,
                TX_VLTested,
                TX_VLSuppressed,
                TX_VLUndetectable
            FROM [All_Dataset].[dbo].[tmpARTTXOutcomes]
            WHERE ReportingPeriod IS NOT NULL
            ORDER BY ReportingPeriod DESC";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        _logger.LogInformation("Connected to source database");
        
        using var reader = await command.ExecuteReaderAsync();
        _logger.LogInformation("Executed query, starting to read records");

        var allRecords = new List<IndicatorValueHIV>();
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        while (await reader.ReadAsync())
        {
            recordsRead++;
            
            if (recordsRead % 10000 == 0)
                _logger.LogInformation("Processed {Count:N0} ART records", recordsRead);

            // Handle NULL values safely
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
                _logger.LogDebug("No region mapping for facility {FacilityCode}", facilityCode);
                continue;
            }

            var sex = sexName.ToUpper() switch
            {
                "MALE" => "M",
                "FEMALE" => "F",
                _ => "Other"
            };

            var now = DateTime.UtcNow;

            // TX_CURR
            if (!reader.IsDBNull(4) && reader.GetInt32(4) == 1)
            {
                allRecords.Add(new IndicatorValueHIV
                {
                    Indicator = "TX_CURR",
                    RegionId = regionId,
                    VisitDate = reportingPeriod.Value,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = null,
                    Value = 1,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // TX_VLTested
            if (!reader.IsDBNull(5) && reader.GetInt32(5) == 1)
            {
                allRecords.Add(new IndicatorValueHIV
                {
                    Indicator = "TX_VL_TESTED",
                    RegionId = regionId,
                    VisitDate = reportingPeriod.Value,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = null,
                    Value = 1,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // TX_VLSuppressed
            if (!reader.IsDBNull(6) && reader.GetInt32(6) == 1)
            {
                allRecords.Add(new IndicatorValueHIV
                {
                    Indicator = "TX_VL_SUPPRESSED",
                    RegionId = regionId,
                    VisitDate = reportingPeriod.Value,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = null,
                    Value = 1,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // TX_VLUndetectable
            if (!reader.IsDBNull(7) && reader.GetInt32(7) == 1)
            {
                allRecords.Add(new IndicatorValueHIV
                {
                    Indicator = "TX_VL_UNDETECTABLE",
                    RegionId = regionId,
                    VisitDate = reportingPeriod.Value,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = null,
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
        ETLHelper.LogETLSummary(_logger, "ART Detail", recordsRead, recordsInserted, duplicates, stopwatch.ElapsedMilliseconds);

        return (recordsRead, recordsInserted);
    }

    private async Task<int> BatchInsertHIVAsync(List<IndicatorValueHIV> records, string batchId)
    {
        await _db.IndicatorValues_HIV.AddRangeAsync(records);
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