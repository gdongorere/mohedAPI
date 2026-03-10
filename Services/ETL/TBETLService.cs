using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.ETL;
using Eswatini.Health.Api.Models.Staging;

namespace Eswatini.Health.Api.Services.ETL;

public class TBETLService : ITBETLService
{
    private readonly StagingDbContext _db;
    private readonly IConfiguration _configuration;
    private readonly ILogger<TBETLService> _logger;
    private readonly IFacilityRegionService _facilityRegionService;
    private readonly string _sourceConnectionString;
    private readonly int _batchSize;

    // ============================================================
    // CONFIGURATION SECTION - Change these values as needed
    // ============================================================
    
    /// <summary>
    /// Whether to log TPT discrepancies (when data quality issues found)
    /// </summary>
    private const bool LogTptDiscrepancies = true;
    
    /// <summary>
    /// Date filter for TPT data (set to null to process all data)
    /// </summary>
    private static readonly DateTime? MinProcessDate = null;
    
    /// <summary>
    /// Column name mappings - Change if source column names change
    /// </summary>
    private static class SourceColumns
    {
        public const string FacilityCode = "FacilityCode";
        public const string VisitDate = "VisitDate";
        public const string TPTStartDate = "TPTStartDate";
        public const string AgeGroup = "AgeGroup";
        public const string SexName = "SexName";
        public const string PatientID = "PatientID";
        public const string FirstName = "FirstName";
        public const string LastName = "LastName";
        public const string PIN = "PIN";
        
        // TPT Status columns
        public const string Eligible = "TPT_Eligible";
        public const string StartedTPT = "TPT_StartedTPT";
        public const string Completed = "TPT_Completed";
        public const string StopTPT = "TPT_StopTPT";
        public const string TransferredOut = "TPT_TransferedOut";
        public const string Died = "TPT_Died";
        public const string SelfStopped = "TPT_SelfStopped";
        public const string StopByClinician = "TPT_StopByClinician";
        public const string LTFU = "TPT_LTFU";
    }
    
    /// <summary>
    /// Indicator mappings - Change if staging indicator names need to change
    /// </summary>
    private static class Indicators
    {
        public const string Eligible = "TPT_ELIGIBLE";
        public const string Started = "TPT_STARTED";
        public const string Completed = "TPT_COMPLETED";
        public const string Stopped = "TPT_STOPPED";
        public const string TransferredOut = "TPT_TRANSFERRED_OUT";
        public const string Died = "TPT_DIED";
        public const string SelfStopped = "TPT_SELF_STOPPED";
        public const string StoppedByClinician = "TPT_STOPPED_BY_CLINICIAN";
        public const string LTFU = "TPT_LTFU";
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
        ["UNKNOWN"] = "Other",
        ["Unknown-Sex"] = "Other"
    };
    
    /// <summary>
    /// Default values for missing data
    /// </summary>
    private static class Defaults
    {
        public const string AgeGroup = "Unknown";
        public const string Sex = "Other";
        public const string PopulationType = null;
        public const string TBType = null;
    }
    
    // ============================================================
    // END OF CONFIGURATION SECTION
    // ============================================================

    public TBETLService(
        StagingDbContext db,
        IConfiguration configuration,
        ILogger<TBETLService> logger,
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
            JobName = "TB ETL",
            StartTime = DateTime.UtcNow
        };

        var batchId = $"TB_{DateTime.UtcNow:yyyyMMdd_HHmmss}_UTC";
        var totalStopwatch = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            _logger.LogInformation("🚀 Starting TB ETL with batch {BatchId}", batchId);
            
            // Load existing records for aggregation
            var existingRecords = await ETLHelper.LoadAllExistingRecordsAsync<IndicatorValueTB>(_db, _logger);

            var (recordsRead, inserted, updated, skipped) = await ProcessTBTreatmentAsync(batchId, existingRecords);

            result.Success = true;
            result.BatchId = batchId;
            result.RecordsRead = recordsRead;
            result.RecordsInserted = inserted;
            result.RecordsUpdated = updated;
            result.EndTime = DateTime.UtcNow;

            totalStopwatch.Stop();
            
            ETLHelper.LogETLSummary(_logger, "TB ETL", recordsRead, inserted, updated, skipped, totalStopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.ErrorMessage = ex.Message;
            result.EndTime = DateTime.UtcNow;
            _logger.LogError(ex, "❌ TB ETL failed: {Message}", ex.Message);
            throw;
        }

        return result;
    }

    private async Task<(int RecordsRead, int Inserted, int Updated, int Skipped)> ProcessTBTreatmentAsync(
        string batchId, 
        Dictionary<string, (DateTime UpdatedAt, int Id, int Value)> existingRecords)
    {
        var recordsRead = 0;
        var allRawRecords = new List<IndicatorValueTB>();
        var inserted = 0;
        var updated = 0;
        var skipped = 0;

        var facilityRegions = await _facilityRegionService.GetFacilityRegionsAsync();
        _logger.LogInformation("Found {Count} facility-region mappings", facilityRegions.Count);

        var unmappedFacilities = new HashSet<string>();
        var discrepancyCount = 0;

        // Build query with optional date filter
        var query = $@"
            SELECT 
                [{SourceColumns.FacilityCode}],
                [{SourceColumns.VisitDate}],
                [{SourceColumns.TPTStartDate}],
                [{SourceColumns.AgeGroup}],
                [{SourceColumns.SexName}],
                [{SourceColumns.PatientID}],
                [{SourceColumns.FirstName}],
                [{SourceColumns.LastName}],
                [{SourceColumns.PIN}],
                ISNULL([{SourceColumns.Eligible}], 0) as {SourceColumns.Eligible},
                ISNULL([{SourceColumns.StartedTPT}], 0) as {SourceColumns.StartedTPT},
                ISNULL([{SourceColumns.Completed}], 0) as {SourceColumns.Completed},
                ISNULL([{SourceColumns.StopTPT}], 0) as {SourceColumns.StopTPT},
                ISNULL([{SourceColumns.TransferredOut}], 0) as {SourceColumns.TransferredOut},
                ISNULL([{SourceColumns.Died}], 0) as {SourceColumns.Died},
                ISNULL([{SourceColumns.SelfStopped}], 0) as {SourceColumns.SelfStopped},
                ISNULL([{SourceColumns.StopByClinician}], 0) as {SourceColumns.StopByClinician},
                ISNULL([{SourceColumns.LTFU}], 0) as {SourceColumns.LTFU}
            FROM [All_Dataset].[dbo].[tmpTPTDetail]
            WHERE [{SourceColumns.VisitDate}] IS NOT NULL"
            + (MinProcessDate.HasValue ? $" AND [{SourceColumns.VisitDate}] >= '{MinProcessDate.Value:yyyy-MM-dd}'" : "")
            + $" ORDER BY [{SourceColumns.VisitDate}]";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        _logger.LogInformation("Connected to source database");
        _logger.LogInformation("Executing query: {Query}", query);
        
        using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            recordsRead++;
            
            if (recordsRead % 10000 == 0)
                _logger.LogInformation("Processed {Count:N0} raw TPT records", recordsRead);

            // Read values with null handling
            var facilityCode = reader.IsDBNull(0) ? null : reader.GetString(0).Trim();
            if (string.IsNullOrEmpty(facilityCode))
                continue;

            var visitDate = reader.IsDBNull(1) ? (DateTime?)null : reader.GetDateTime(1);
            if (!visitDate.HasValue)
                continue;

            var tptStartDate = reader.IsDBNull(2) ? (DateTime?)null : reader.GetDateTime(2);
            var ageGroup = reader.IsDBNull(3) ? Defaults.AgeGroup : reader.GetString(3).Trim();
            var sexName = reader.IsDBNull(4) ? Defaults.Sex : reader.GetString(4).Trim();
            
            // Optional fields - read but not used in aggregation
            var patientId = reader.IsDBNull(5) ? null : reader.GetString(5);
            var firstName = reader.IsDBNull(6) ? null : reader.GetString(6);
            var lastName = reader.IsDBNull(7) ? null : reader.GetString(7);
            var pin = reader.IsDBNull(8) ? null : reader.GetString(8);
            
            if (!facilityRegions.TryGetValue(facilityCode, out var regionId))
            {
                unmappedFacilities.Add(facilityCode);
                continue;
            }

            // Map sex to staging value
            var sex = SexMappings.TryGetValue(sexName, out var mappedSex) ? mappedSex : Defaults.Sex;

            var now = DateTime.UtcNow;

            // Get all TPT status values
            var eligibleValue = reader.GetInt32(9);
            var startedValue = reader.GetInt32(10);
            var completedValue = reader.GetInt32(11);
            var stoppedValue = reader.GetInt32(12);
            var transferredOutValue = reader.GetInt32(13);
            var diedValue = reader.GetInt32(14);
            var selfStoppedValue = reader.GetInt32(15);
            var stoppedByClinicianValue = reader.GetInt32(16);
            var ltfUValue = reader.GetInt32(17);

            // Validate data quality - a client can't be in multiple mutually exclusive states
            var activeStates = 0;
            if (completedValue > 0) activeStates++;
            if (stoppedValue > 0) activeStates++;
            if (transferredOutValue > 0) activeStates++;
            if (diedValue > 0) activeStates++;
            if (ltfUValue > 0) activeStates++;
            
            if (activeStates > 1 && LogTptDiscrepancies)
            {
                discrepancyCount++;
                _logger.LogDebug("TPT discrepancy #{Count} at {FacilityCode}/{VisitDate}: Client has multiple terminal states", 
                    discrepancyCount, facilityCode, visitDate.Value.Date);
            }

            // Add TPT_ELIGIBLE if eligible > 0
            if (eligibleValue > 0)
            {
                allRawRecords.Add(new IndicatorValueTB
                {
                    Indicator = Indicators.Eligible,
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = Defaults.PopulationType,
                    TBType = Defaults.TBType,
                    Value = eligibleValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // Add TPT_STARTED if started > 0
            if (startedValue > 0)
            {
                allRawRecords.Add(new IndicatorValueTB
                {
                    Indicator = Indicators.Started,
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = Defaults.PopulationType,
                    TBType = Defaults.TBType,
                    Value = startedValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // Add TPT_COMPLETED if completed > 0
            if (completedValue > 0)
            {
                allRawRecords.Add(new IndicatorValueTB
                {
                    Indicator = Indicators.Completed,
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = Defaults.PopulationType,
                    TBType = Defaults.TBType,
                    Value = completedValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // Add TPT_STOPPED if stopped > 0
            if (stoppedValue > 0)
            {
                allRawRecords.Add(new IndicatorValueTB
                {
                    Indicator = Indicators.Stopped,
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = Defaults.PopulationType,
                    TBType = Defaults.TBType,
                    Value = stoppedValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // Add TPT_TRANSFERRED_OUT if transferred out > 0
            if (transferredOutValue > 0)
            {
                allRawRecords.Add(new IndicatorValueTB
                {
                    Indicator = Indicators.TransferredOut,
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = Defaults.PopulationType,
                    TBType = Defaults.TBType,
                    Value = transferredOutValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // Add TPT_DIED if died > 0
            if (diedValue > 0)
            {
                allRawRecords.Add(new IndicatorValueTB
                {
                    Indicator = Indicators.Died,
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = Defaults.PopulationType,
                    TBType = Defaults.TBType,
                    Value = diedValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // Add TPT_SELF_STOPPED if self stopped > 0
            if (selfStoppedValue > 0)
            {
                allRawRecords.Add(new IndicatorValueTB
                {
                    Indicator = Indicators.SelfStopped,
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = Defaults.PopulationType,
                    TBType = Defaults.TBType,
                    Value = selfStoppedValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // Add TPT_STOPPED_BY_CLINICIAN if stopped by clinician > 0
            if (stoppedByClinicianValue > 0)
            {
                allRawRecords.Add(new IndicatorValueTB
                {
                    Indicator = Indicators.StoppedByClinician,
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = Defaults.PopulationType,
                    TBType = Defaults.TBType,
                    Value = stoppedByClinicianValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // Add TPT_LTFU if LTFU > 0
            if (ltfUValue > 0)
            {
                allRawRecords.Add(new IndicatorValueTB
                {
                    Indicator = Indicators.LTFU,
                    RegionId = regionId,
                    VisitDate = visitDate.Value.Date,
                    AgeGroup = ageGroup,
                    Sex = sex,
                    PopulationType = Defaults.PopulationType,
                    TBType = Defaults.TBType,
                    Value = ltfUValue,
                    CreatedAt = now,
                    UpdatedAt = now
                });
            }

            // Aggregate when we reach batch size
            if (allRawRecords.Count >= _batchSize)
            {
                var aggregated = ETLHelper.AggregateRecords(allRawRecords);
                var (ins, upd, skp, del) = await ETLHelper.UpsertAggregatedRecordsAsync<IndicatorValueTB>(
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
            _logger.LogWarning("Found {Count} unmapped facilities in TPT data: {Facilities}", 
                unmappedFacilities.Count, string.Join(", ", unmappedFacilities.Take(20)));
        }

        // Log discrepancy summary
        if (discrepancyCount > 0 && LogTptDiscrepancies)
        {
            _logger.LogWarning("Found {Count} TPT records with multiple terminal states", discrepancyCount);
        }

        // Process remaining records
        if (allRawRecords.Any())
        {
            var aggregated = ETLHelper.AggregateRecords(allRawRecords);
            var (ins, upd, skp, del) = await ETLHelper.UpsertAggregatedRecordsAsync<IndicatorValueTB>(
                aggregated, _db, _logger, batchId, existingRecords);
            
            inserted += ins;
            updated += upd;
            skipped += skp;
        }

        return (recordsRead, inserted, updated, skipped);
    }

    public async Task<int> GetRecordCountForPeriodAsync(DateTime startDate, DateTime endDate)
    {
        var query = @"
            SELECT COUNT(*) 
            FROM [All_Dataset].[dbo].[tmpTPTDetail]
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