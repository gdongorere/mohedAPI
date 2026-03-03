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

        try
        {
            _logger.LogInformation("Starting HTS ETL with batch {BatchId}", batchId);

            // Process HTS Testing data
            var (recordsRead, recordsInserted) = await ProcessHTSTestingAsync(batchId);

            result.Success = true;
            result.BatchId = batchId;
            result.RecordsRead = recordsRead;
            result.RecordsInserted = recordsInserted;
            result.EndTime = DateTime.UtcNow;

            _logger.LogInformation("HTS ETL completed: {RecordsInserted} records inserted", recordsInserted);
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.ErrorMessage = ex.Message;
            result.EndTime = DateTime.UtcNow;
            _logger.LogError(ex, "HTS ETL failed");
        }

        return result;
    }

    private async Task<(int RecordsRead, int RecordsInserted)> ProcessHTSTestingAsync(string batchId)
    {
        var recordsRead = 0;
        var recordsInserted = 0;

        // Get facility-region mapping
        var facilityRegions = await GetFacilityRegionsAsync();

        // Query source data
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
            WHERE VisitDate >= DATEADD(day, -7, GETDATE()) -- Last 7 days, adjust as needed
            ORDER BY VisitDate";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        using var reader = await command.ExecuteReaderAsync();

        var htsTests = new List<IndicatorValuePrevention>();
        var htsPositives = new List<IndicatorValuePrevention>();
        var htsNegatives = new List<IndicatorValuePrevention>();
        var linkages = new List<IndicatorValuePrevention>();

        while (await reader.ReadAsync())
        {
            recordsRead++;
            
            var facilityCode = reader.GetString(0);
            var visitDate = reader.GetDateTime(1);
            var ageGroup = reader.IsDBNull(2) ? "Unknown" : reader.GetString(2);
            var sexName = reader.IsDBNull(3) ? "Other" : reader.GetString(3);
            var populationGroup = reader.IsDBNull(4) ? null : reader.GetString(4);
            
            // Get region from facility mapping
            if (!facilityRegions.TryGetValue(facilityCode, out var regionId))
                continue; // Skip if facility not mapped

            var sex = sexName.ToUpper() switch
            {
                "MALE" => "M",
                "FEMALE" => "F",
                _ => "Other"
            };

            // HTS Tested
            if (!reader.IsDBNull(5) && reader.GetInt32(5) == 1)
            {
                htsTests.Add(new IndicatorValuePrevention
                {
                    Indicator = "HTS_TST",
                    RegionId = regionId,
                    VisitDate = visitDate,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationGroup,
                    Value = 1,
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow
                });
            }

            // HTS Negative
            if (!reader.IsDBNull(6) && reader.GetInt32(6) == 1)
            {
                htsNegatives.Add(new IndicatorValuePrevention
                {
                    Indicator = "HTS_NEG",
                    RegionId = regionId,
                    VisitDate = visitDate,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationGroup,
                    Value = 1,
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow
                });
            }

            // HTS Positive
            if (!reader.IsDBNull(7) && reader.GetInt32(7) == 1)
            {
                htsPositives.Add(new IndicatorValuePrevention
                {
                    Indicator = "HTS_POS",
                    RegionId = regionId,
                    VisitDate = visitDate,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationGroup,
                    Value = 1,
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow
                });
            }

            // Linkage to ART
            if (!reader.IsDBNull(8) && reader.GetInt32(8) == 1)
            {
                linkages.Add(new IndicatorValuePrevention
                {
                    Indicator = "LINKAGE_ART",
                    RegionId = regionId,
                    VisitDate = visitDate,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationGroup,
                    Value = 1,
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow
                });
            }

            // Batch insert when we reach batch size
            if (htsTests.Count >= _batchSize)
            {
                recordsInserted += await BatchInsertPreventionAsync(htsTests, batchId);
                htsTests.Clear();
            }
            if (htsPositives.Count >= _batchSize)
            {
                recordsInserted += await BatchInsertPreventionAsync(htsPositives, batchId);
                htsPositives.Clear();
            }
            if (htsNegatives.Count >= _batchSize)
            {
                recordsInserted += await BatchInsertPreventionAsync(htsNegatives, batchId);
                htsNegatives.Clear();
            }
            if (linkages.Count >= _batchSize)
            {
                recordsInserted += await BatchInsertPreventionAsync(linkages, batchId);
                linkages.Clear();
            }
        }

        // Insert remaining records
        if (htsTests.Any())
            recordsInserted += await BatchInsertPreventionAsync(htsTests, batchId);
        if (htsPositives.Any())
            recordsInserted += await BatchInsertPreventionAsync(htsPositives, batchId);
        if (htsNegatives.Any())
            recordsInserted += await BatchInsertPreventionAsync(htsNegatives, batchId);
        if (linkages.Any())
            recordsInserted += await BatchInsertPreventionAsync(linkages, batchId);

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
            SELECT FacilityCode, Region 
            FROM [cmis_dev].dbo.Facility 
            WHERE Region IS NOT NULL";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        using var reader = await command.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            var facilityCode = reader.GetString(0);
            var regionValue = reader.GetByte(1);
            
            // Region mapping: 1=Hhohho, 2=Manzini, 3=Shiselweni, 4=Lubombo
            if (regionValue >= 1 && regionValue <= 4)
            {
                result[facilityCode] = regionValue;
            }
        }

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
        return result == null ? 0 : (int)result;
    }
}