using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;

namespace Eswatini.Health.Api.Services.ETL;

public interface IFacilityRegionService
{
    Task<Dictionary<string, int>> GetFacilityRegionsAsync();
    void InvalidateCache();
}

public class FacilityRegionService : IFacilityRegionService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<FacilityRegionService> _logger;
    private readonly string _sourceConnectionString;
    private static readonly ConcurrentDictionary<string, int> _cache = new();
    private static DateTime _lastCacheUpdate = DateTime.MinValue;
    private static readonly TimeSpan _cacheDuration = TimeSpan.FromHours(1);
    private static readonly SemaphoreSlim _cacheLock = new(1, 1);

    public FacilityRegionService(
        IConfiguration configuration,
        ILogger<FacilityRegionService> logger)
    {
        _configuration = configuration;
        _logger = logger;
        _sourceConnectionString = configuration.GetConnectionString("SourceConnection") 
            ?? throw new InvalidOperationException("SourceConnection not configured");
    }

    public async Task<Dictionary<string, int>> GetFacilityRegionsAsync()
{
    var result = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
    var regionMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
    {
        { "Hhohho", 1 },
        { "Manzini", 2 },
        { "Shiselweni", 3 },
        { "Lubombo", 4 }
    };

    try
    {
        _logger.LogInformation("Loading facility-region mappings from aPrepDetail...");
        
        // First, get all mappings from aPrepDetail (primary source)
        var query = @"
            SELECT DISTINCT FacilityCode, Region 
            FROM [All_Dataset].[dbo].[aPrepDetail] 
            WHERE FacilityCode IS NOT NULL 
              AND FacilityCode != '' 
              AND Region IS NOT NULL 
              AND Region != ''";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        using var reader = await command.ExecuteReaderAsync();
        
        var aPrepDetailCount = 0;
        while (await reader.ReadAsync())
        {
            var facilityCode = reader.GetString(0).Trim();
            var regionName = reader.GetString(1).Trim();
            
            if (regionMap.TryGetValue(regionName, out var regionId))
            {
                result[facilityCode] = regionId;
                aPrepDetailCount++;
            }
            else
            {
                _logger.LogDebug("Unknown region '{RegionName}' for facility {FacilityCode}", regionName, facilityCode);
            }
        }
        
        _logger.LogInformation("Loaded {Count} mappings from aPrepDetail", aPrepDetailCount);

        // Second, get additional mappings from tmpHTSTestedDetail for facilities not in aPrepDetail
        _logger.LogInformation("Checking tmpHTSTestedDetail for additional facility mappings...");
        
        var htsQuery = @"
            SELECT DISTINCT FacilityCode, Region 
            FROM [All_Dataset].[dbo].[tmpHTSTestedDetail] 
            WHERE FacilityCode IS NOT NULL 
              AND FacilityCode != '' 
              AND Region IS NOT NULL 
              AND Region != ''
              AND FacilityCode NOT IN (SELECT DISTINCT FacilityCode FROM [All_Dataset].[dbo].[aPrepDetail] WHERE FacilityCode IS NOT NULL)";

        using var htsConnection = new SqlConnection(_sourceConnectionString);
        using var htsCommand = new SqlCommand(htsQuery, htsConnection);
        
        await htsConnection.OpenAsync();
        using var htsReader = await htsCommand.ExecuteReaderAsync();
        
        var htsCount = 0;
        while (await htsReader.ReadAsync())
        {
            var facilityCode = htsReader.GetString(0).Trim();
            var regionName = htsReader.GetString(1).Trim();
            
            if (regionMap.TryGetValue(regionName, out var regionId) && !result.ContainsKey(facilityCode))
            {
                result[facilityCode] = regionId;
                htsCount++;
            }
        }
        
        _logger.LogInformation("Added {Count} additional mappings from tmpHTSTestedDetail", htsCount);

        // Third, get additional mappings from tmpARTTXOutcomes for facilities not mapped yet
        _logger.LogInformation("Checking tmpARTTXOutcomes for additional facility mappings...");
        
        var artQuery = @"
            SELECT DISTINCT FacilityCode, Region 
            FROM [All_Dataset].[dbo].[tmpARTTXOutcomes] 
            WHERE FacilityCode IS NOT NULL 
              AND FacilityCode != '' 
              AND Region IS NOT NULL 
              AND Region != ''
              AND FacilityCode NOT IN (SELECT DISTINCT FacilityCode FROM [All_Dataset].[dbo].[aPrepDetail] WHERE FacilityCode IS NOT NULL)
              AND FacilityCode NOT IN (SELECT DISTINCT FacilityCode FROM [All_Dataset].[dbo].[tmpHTSTestedDetail] WHERE FacilityCode IS NOT NULL)";

        using var artConnection = new SqlConnection(_sourceConnectionString);
        using var artCommand = new SqlCommand(artQuery, artConnection);
        
        await artConnection.OpenAsync();
        using var artReader = await artCommand.ExecuteReaderAsync();
        
        var artCount = 0;
        while (await artReader.ReadAsync())
        {
            var facilityCode = artReader.GetString(0).Trim();
            var regionName = artReader.GetString(1).Trim();
            
            if (regionMap.TryGetValue(regionName, out var regionId) && !result.ContainsKey(facilityCode))
            {
                result[facilityCode] = regionId;
                artCount++;
            }
        }
        
        _logger.LogInformation("Added {Count} additional mappings from tmpARTTXOutcomes", artCount);

        // Log total mappings found
        _logger.LogInformation("Total facility-region mappings found: {Count}", result.Count);

        // Now check for unmapped ART facilities (critical for missing patients)
        _logger.LogInformation("Checking for unmapped ART facilities...");
        
        var unmappedArtQuery = @"
            SELECT DISTINCT FacilityCode, COUNT(*) as PatientCount
            FROM [All_Dataset].[dbo].[tmpARTTXOutcomes] 
            WHERE FacilityCode IS NOT NULL 
              AND FacilityCode != '' 
              AND TX_CURR = 1
            GROUP BY FacilityCode
            ORDER BY COUNT(*) DESC";

        using var unmappedConnection = new SqlConnection(_sourceConnectionString);
        using var unmappedCommand = new SqlCommand(unmappedArtQuery, unmappedConnection);
        
        await unmappedConnection.OpenAsync();
        using var unmappedReader = await unmappedCommand.ExecuteReaderAsync();
        
        var unmappedFacilities = new List<(string Code, int Count)>();
        var totalUnmappedPatients = 0;
        
        while (await unmappedReader.ReadAsync())
        {
            var facilityCode = unmappedReader.GetString(0).Trim();
            var patientCount = unmappedReader.GetInt32(1);
            
            if (!result.ContainsKey(facilityCode))
            {
                unmappedFacilities.Add((facilityCode, patientCount));
                totalUnmappedPatients += patientCount;
            }
        }
        
        if (unmappedFacilities.Any())
        {
            _logger.LogWarning("Found {Count} unmapped ART facilities with {PatientCount} total patients", 
                unmappedFacilities.Count, totalUnmappedPatients);
            
            // Try to infer region from facility code prefix
            _logger.LogInformation("Attempting to infer region from facility code prefixes...");
            
            var inferredCount = 0;
            var inferredPatients = 0;
            
            foreach (var (facilityCode, patientCount) in unmappedFacilities)
            {
                if (facilityCode.Length >= 1)
                {
                    var prefix = facilityCode.Substring(0, 1).ToUpper();
                    var regionId = prefix switch
                    {
                        "H" => 1,  // Hhohho
                        "M" => 2,  // Manzini
                        "S" => 3,  // Shiselweni
                        "L" => 4,  // Lubombo
                        _ => 0
                    };
                    
                    if (regionId > 0 && !result.ContainsKey(facilityCode))
                    {
                        result[facilityCode] = regionId;
                        inferredCount++;
                        inferredPatients += patientCount;
                        _logger.LogDebug("Inferred mapping for {FacilityCode} (prefix {Prefix}) to region {RegionId} with {PatientCount} patients", 
                            facilityCode, prefix, regionId, patientCount);
                    }
                }
            }
            
            if (inferredCount > 0)
            {
                _logger.LogInformation("Inferred region for {Count} facilities covering {PatientCount} patients", 
                    inferredCount, inferredPatients);
            }
            
            // Log top unmapped facilities that couldn't be inferred
            var stillUnmapped = unmappedFacilities
                .Where(x => !result.ContainsKey(x.Code))
                .OrderByDescending(x => x.Count)
                .Take(10)
                .ToList();
            
            if (stillUnmapped.Any())
            {
                _logger.LogWarning("Top 10 still unmapped facilities after inference:");
                foreach (var (code, count) in stillUnmapped)
                {
                    _logger.LogWarning("  - {FacilityCode}: {PatientCount} patients", code, count);
                }
            }
        }

        // Log region distribution
        var regionDistribution = result
            .GroupBy(x => x.Value)
            .Select(g => new { RegionId = g.Key, Count = g.Count() })
            .OrderBy(x => x.RegionId);
        
        foreach (var region in regionDistribution)
        {
            var regionName = region.RegionId switch
            {
                1 => "Hhohho",
                2 => "Manzini",
                3 => "Shiselweni",
                4 => "Lubombo",
                _ => "Unknown"
            };
            _logger.LogInformation("Region {RegionId} ({RegionName}): {Count} facilities", 
                region.RegionId, regionName, region.Count);
        }

        return result;
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Error loading facility-region mappings");
        
        // Return any mappings we have so far
        if (result.Any())
        {
            _logger.LogWarning("Returning partial mappings ({Count} facilities) due to error", result.Count);
            return result;
        }
        
        throw;
    }
}

    private async Task AddMissingMappingsFromHTS(Dictionary<string, int> existingMappings)
    {
        try
        {
            var query = @"
                SELECT DISTINCT FacilityCode, Region 
                FROM [All_Dataset].[dbo].[tmpHTSTestedDetail] 
                WHERE FacilityCode IS NOT NULL 
                  AND FacilityCode != '' 
                  AND Region IS NOT NULL 
                  AND Region != ''
                  AND FacilityCode NOT IN (SELECT FacilityCode FROM aPrepDetail WHERE FacilityCode IS NOT NULL)";

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
                
                if (regionMap.TryGetValue(regionName, out var regionId) && !existingMappings.ContainsKey(facilityCode))
                {
                    existingMappings[facilityCode] = regionId;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error adding missing mappings from HTS");
        }
    }

    private async Task AddMissingMappingsFromART(Dictionary<string, int> existingMappings)
    {
        try
        {
            var query = @"
                SELECT DISTINCT FacilityCode, Region 
                FROM [All_Dataset].[dbo].[tmpARTTXOutcomes] 
                WHERE FacilityCode IS NOT NULL 
                  AND FacilityCode != '' 
                  AND Region IS NOT NULL 
                  AND Region != ''
                  AND FacilityCode NOT IN (SELECT FacilityCode FROM aPrepDetail WHERE FacilityCode IS NOT NULL)";

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
                
                if (regionMap.TryGetValue(regionName, out var regionId) && !existingMappings.ContainsKey(facilityCode))
                {
                    existingMappings[facilityCode] = regionId;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error adding missing mappings from ART");
        }
    }

    public void InvalidateCache()
    {
        _cache.Clear();
        _lastCacheUpdate = DateTime.MinValue;
    }
}