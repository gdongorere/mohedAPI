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

        try
        {
            _logger.LogInformation("Starting PrEP ETL with batch {BatchId}", batchId);

            // Process LineListingsPrep (primary source)
            var (prepRecordsRead, prepRecordsInserted) = await ProcessLineListingsPrepAsync(batchId);
            
            // Process aPrepDetail (secondary source, avoiding duplicates)
            var (detailRecordsRead, detailRecordsInserted) = await ProcessAPrepDetailAsync(batchId);

            result.Success = true;
            result.BatchId = batchId;
            result.RecordsRead = prepRecordsRead + detailRecordsRead;
            result.RecordsInserted = prepRecordsInserted + detailRecordsInserted;
            result.EndTime = DateTime.UtcNow;

            _logger.LogInformation("PrEP ETL completed: {RecordsInserted} records inserted", result.RecordsInserted);
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.ErrorMessage = ex.Message;
            result.EndTime = DateTime.UtcNow;
            _logger.LogError(ex, "PrEP ETL failed");
        }

        return result;
    }

    private async Task<(int RecordsRead, int RecordsInserted)> ProcessLineListingsPrepAsync(string batchId)
    {
        var recordsRead = 0;
        var recordsInserted = 0;

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
            WHERE VisitDate >= DATEADD(day, -7, GETDATE())
            ORDER BY VisitDate";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        using var reader = await command.ExecuteReaderAsync();

        var initiations = new List<IndicatorValuePrevention>();
        var tested = new List<IndicatorValuePrevention>();
        var positives = new List<IndicatorValuePrevention>();
        var negatives = new List<IndicatorValuePrevention>();
        var seroconversions = new List<IndicatorValuePrevention>();

        while (await reader.ReadAsync())
        {
            recordsRead++;
            
            var facilityCode = reader.GetString(0);
            var visitDate = reader.GetDateTime(1);
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

            // PrEP Initiations
            if (!reader.IsDBNull(5) && reader.GetInt32(5) == 1)
            {
                initiations.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_NEW",
                    RegionId = regionId,
                    VisitDate = visitDate,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = 1,
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow
                });
            }

            // PrEP Tested for HIV
            if (!reader.IsDBNull(6) && reader.GetInt32(6) == 1)
            {
                tested.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_TESTED",
                    RegionId = regionId,
                    VisitDate = visitDate,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = 1,
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow
                });
            }

            // PrEP Tested Negative
            if (!reader.IsDBNull(7) && reader.GetInt32(7) == 1)
            {
                negatives.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_NEG",
                    RegionId = regionId,
                    VisitDate = visitDate,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = 1,
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow
                });
            }

            // PrEP Tested Positive (Seroconversion)
            if (!reader.IsDBNull(8) && reader.GetInt32(8) == 1)
            {
                positives.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_POS",
                    RegionId = regionId,
                    VisitDate = visitDate,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = 1,
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow
                });

                // Also count as seroconversion
                seroconversions.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_SEROCONVERSION",
                    RegionId = regionId,
                    VisitDate = visitDate,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = 1,
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow
                });
            }

            // Batch inserts
            recordsInserted += await BatchInsertIfNeededAsync(initiations, tested, negatives, positives, seroconversions, batchId);
        }

        // Insert remaining
        recordsInserted += await BatchInsertAllAsync(initiations, tested, negatives, positives, seroconversions, batchId);

        return (recordsRead, recordsInserted);
    }

    private async Task<(int RecordsRead, int RecordsInserted)> ProcessAPrepDetailAsync(string batchId)
    {
        var recordsRead = 0;
        var recordsInserted = 0;

        var facilityRegions = await GetFacilityRegionsAsync();

        var query = @"
            SELECT 
                FacilityCode,
                VisitDate,
                AgeGroup,
                Sex,
                PopulationType,
                CurrentPrepMethod,
                Seroconverted,
                InitiatedOnART
            FROM [All_Dataset].[dbo].[aPrepDetail]
            WHERE VisitDate >= DATEADD(day, -7, GETDATE())
            ORDER BY VisitDate";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        using var reader = await command.ExecuteReaderAsync();

        var records = new List<IndicatorValuePrevention>();

        while (await reader.ReadAsync())
        {
            recordsRead++;
            
            var facilityCode = reader.GetString(0);
            var visitDate = reader.GetDateTime(1);
            var ageGroup = reader.IsDBNull(2) ? "Unknown" : reader.GetString(2);
            var sexValue = reader.IsDBNull(3) ? 0 : reader.GetInt32(3);
            var populationType = reader.IsDBNull(4) ? null : reader.GetString(4);
            
            if (!facilityRegions.TryGetValue(facilityCode, out var regionId))
                continue;

            var sex = sexValue switch
            {
                1 => "M",
                2 => "F",
                _ => "Other"
            };

            // Check for seroconversion
            if (!reader.IsDBNull(6) && reader.GetBoolean(6))
            {
                records.Add(new IndicatorValuePrevention
                {
                    Indicator = "PREP_SEROCONVERSION",
                    RegionId = regionId,
                    VisitDate = visitDate,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = populationType,
                    Value = 1,
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow
                });
            }

            if (records.Count >= _batchSize)
            {
                recordsInserted += await BatchInsertPreventionAsync(records, batchId);
                records.Clear();
            }
        }

        if (records.Any())
            recordsInserted += await BatchInsertPreventionAsync(records, batchId);

        return (recordsRead, recordsInserted);
    }

    private async Task<int> BatchInsertIfNeededAsync(
        List<IndicatorValuePrevention> initiations,
        List<IndicatorValuePrevention> tested,
        List<IndicatorValuePrevention> negatives,
        List<IndicatorValuePrevention> positives,
        List<IndicatorValuePrevention> seroconversions,
        string batchId)
    {
        var inserted = 0;
        
        if (initiations.Count >= _batchSize)
        {
            inserted += await BatchInsertPreventionAsync(initiations, batchId);
            initiations.Clear();
        }
        if (tested.Count >= _batchSize)
        {
            inserted += await BatchInsertPreventionAsync(tested, batchId);
            tested.Clear();
        }
        if (negatives.Count >= _batchSize)
        {
            inserted += await BatchInsertPreventionAsync(negatives, batchId);
            negatives.Clear();
        }
        if (positives.Count >= _batchSize)
        {
            inserted += await BatchInsertPreventionAsync(positives, batchId);
            positives.Clear();
        }
        if (seroconversions.Count >= _batchSize)
        {
            inserted += await BatchInsertPreventionAsync(seroconversions, batchId);
            seroconversions.Clear();
        }

        return inserted;
    }

    private async Task<int> BatchInsertAllAsync(
        List<IndicatorValuePrevention> initiations,
        List<IndicatorValuePrevention> tested,
        List<IndicatorValuePrevention> negatives,
        List<IndicatorValuePrevention> positives,
        List<IndicatorValuePrevention> seroconversions,
        string batchId)
    {
        var inserted = 0;
        
        if (initiations.Any())
            inserted += await BatchInsertPreventionAsync(initiations, batchId);
        if (tested.Any())
            inserted += await BatchInsertPreventionAsync(tested, batchId);
        if (negatives.Any())
            inserted += await BatchInsertPreventionAsync(negatives, batchId);
        if (positives.Any())
            inserted += await BatchInsertPreventionAsync(positives, batchId);
        if (seroconversions.Any())
            inserted += await BatchInsertPreventionAsync(seroconversions, batchId);

        return inserted;
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
            FROM [All_Dataset].[dbo].[LineListingsPrep]
            WHERE VisitDate >= @StartDate AND VisitDate < @EndDate";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        command.Parameters.AddWithValue("@StartDate", startDate);
        command.Parameters.AddWithValue("@EndDate", endDate);
        
        await connection.OpenAsync();
        var result = await command.ExecuteScalarAsync();
        return result != null ? (int)result : 0;
    }
}