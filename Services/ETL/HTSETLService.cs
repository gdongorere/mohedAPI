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
    private readonly string _sourceConnectionString;
    private readonly int _batchSize;

    public HTSETLService(
        StagingDbContext db,
        IConfiguration configuration,
        ILogger<HTSETLService> logger)
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
            JobName = "HTS ETL",
            StartTime = DateTime.UtcNow
        };

        var batchId = $"HTS_{DateTime.UtcNow:yyyyMMdd_HHmmss}_UTC";
        var totalStopwatch = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            _logger.LogInformation("🚀 Starting HTS ETL with batch {BatchId}", batchId);
            
            // Load existing records for deduplication (last 90 days to catch updates)
            var existingRecords = await ETLHelper.LoadExistingRecordsAsync<IndicatorValuePrevention>(_db, _logger, DateTime.UtcNow.AddDays(-90));
            _logger.LogInformation("📊 Loaded {Count:N0} existing records for deduplication", existingRecords.Count);

            var (recordsRead, recordsInserted) = await ProcessHTSTestingAsync(batchId, existingRecords);

            result.Success = true;
            result.BatchId = batchId;
            result.RecordsRead = recordsRead;
            result.RecordsInserted = recordsInserted;
            result.EndTime = DateTime.UtcNow;

            totalStopwatch.Stop();
            
            ETLHelper.LogETLSummary(_logger, "HTS ETL", recordsRead, recordsInserted, 0, totalStopwatch.ElapsedMilliseconds);
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

    private async Task<(int RecordsRead, int RecordsInserted)> ProcessHTSTestingAsync(
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
                VisitDate,
                AgeGroup,
                SexName,
                PopulationGroup,
                HTS_TestedForHIV,
                HTS_TestedNegative,
                HTS_TestedPositive,
                HTS_TestedPositiveInitiatedOnART
            FROM [All_Dataset].[dbo].[tmpHTSTestedDetail]
            WHERE VisitDate IS NOT NULL
            ORDER BY VisitDate";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        _logger.LogInformation("Connected to source database");
        
        using var reader = await command.ExecuteReaderAsync();
        _logger.LogInformation("Executed query, starting to read records");

        var allRecords = new List<IndicatorValuePrevention>();
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        while (await reader.ReadAsync())
        {
            recordsRead++;
            
            if (recordsRead % 10000 == 0)
                _logger.LogInformation("Processed {Count:N0} HTS records", recordsRead);

            // Handle NULL values safely
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
                continue;

            var sex = sexName.ToUpper() switch
            {
                "MALE" => "M",
                "FEMALE" => "F",
                _ => "Other"
            };

            var now = DateTime.UtcNow;

            // HTS Tested
            if (!reader.IsDBNull(5) && reader.GetInt32(5) == 1)
            {
                allRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "HTS_TST",
                    RegionId = regionId,
                    VisitDate = visitDate.Value,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationGroup,
                    Value = 1,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // HTS Negative
            if (!reader.IsDBNull(6) && reader.GetInt32(6) == 1)
            {
                allRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "HTS_NEG",
                    RegionId = regionId,
                    VisitDate = visitDate.Value,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationGroup,
                    Value = 1,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // HTS Positive
            if (!reader.IsDBNull(7) && reader.GetInt32(7) == 1)
            {
                allRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "HTS_POS",
                    RegionId = regionId,
                    VisitDate = visitDate.Value,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationGroup,
                    Value = 1,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // Linkage to ART
            if (!reader.IsDBNull(8) && reader.GetInt32(8) == 1)
            {
                allRecords.Add(new IndicatorValuePrevention
                {
                    Indicator = "LINKAGE_ART",
                    RegionId = regionId,
                    VisitDate = visitDate.Value,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationGroup,
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
        ETLHelper.LogETLSummary(_logger, "HTS Detail", recordsRead, recordsInserted, duplicates, stopwatch.ElapsedMilliseconds);

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