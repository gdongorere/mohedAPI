using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

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

    private static readonly Dictionary<string, int> RegionPrefixMap = new(StringComparer.OrdinalIgnoreCase)
    {
        { "H", 1 },  // Hhohho
        { "M", 2 },  // Manzini
        { "S", 3 },  // Shiselweni
        { "L", 4 }   // Lubombo
    };

    private static readonly Dictionary<string, int> RegionNameMap = new(StringComparer.OrdinalIgnoreCase)
    {
        { "Hhohho", 1 },
        { "Manzini", 2 },
        { "Shiselweni", 3 },
        { "Lubombo", 4 }
    };

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
        // Check if cache is valid
        if (_cache.Any() && DateTime.UtcNow - _lastCacheUpdate < _cacheDuration)
        {
            _logger.LogDebug("Returning cached facility-region mappings ({Count} facilities)", _cache.Count);
            return new Dictionary<string, int>(_cache);
        }

        await _cacheLock.WaitAsync();
        try
        {
            // Double-check after acquiring lock
            if (_cache.Any() && DateTime.UtcNow - _lastCacheUpdate < _cacheDuration)
            {
                return new Dictionary<string, int>(_cache);
            }

            _logger.LogInformation("Refreshing facility-region mapping cache");
            
            var result = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            
            // STEP 1: Get all explicit mappings from aPrepDetail
            _logger.LogInformation("Loading explicit facility-region mappings from aPrepDetail...");
            
            var explicitMappings = await LoadExplicitMappingsAsync();
            foreach (var kvp in explicitMappings)
            {
                result[kvp.Key] = kvp.Value;
            }
            
            _logger.LogInformation("Loaded {Count} explicit mappings from aPrepDetail", explicitMappings.Count);

            // STEP 2: Find all ART facilities that need mapping
            _logger.LogInformation("Checking for ART facilities that need mapping...");
            
            var artFacilities = await GetARTFacilitiesAsync();
            _logger.LogInformation("Found {Count} distinct ART facilities with patients", artFacilities.Count);

            // STEP 3: Apply first-letter mapping to unmapped facilities
            var mappedByPrefix = 0;
            var totalPatientsMapped = 0;
            var unmappedFacilities = new List<(string Code, int PatientCount)>();

            foreach (var (facilityCode, patientCount) in artFacilities)
            {
                if (!result.ContainsKey(facilityCode))
                {
                    if (TryMapByPrefix(facilityCode, out var regionId))
                    {
                        result[facilityCode] = regionId;
                        mappedByPrefix++;
                        totalPatientsMapped += patientCount;
                        _logger.LogDebug("Mapped {FacilityCode} (prefix) to region {RegionId} with {PatientCount} patients", 
                            facilityCode, regionId, patientCount);
                    }
                    else
                    {
                        unmappedFacilities.Add((facilityCode, patientCount));
                    }
                }
            }

            // Log statistics
            _logger.LogInformation("First-letter mapping added {Count} facilities covering {Patients} patients", 
                mappedByPrefix, totalPatientsMapped);

            if (unmappedFacilities.Any())
            {
                _logger.LogWarning("Still have {Count} unmapped ART facilities with {Patients} total patients", 
                    unmappedFacilities.Count, unmappedFacilities.Sum(x => x.PatientCount));
                
                foreach (var (code, count) in unmappedFacilities.OrderByDescending(x => x.PatientCount).Take(10))
                {
                    _logger.LogWarning("  - {FacilityCode}: {PatientCount} patients", code, count);
                }
            }

            // Update cache
            _cache.Clear();
            foreach (var kvp in result)
            {
                _cache[kvp.Key] = kvp.Value;
            }
            _lastCacheUpdate = DateTime.UtcNow;

            // Log region distribution
            LogRegionDistribution(result);

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading facility-region mappings");
            
            // Return cached data if available, even if expired
            if (_cache.Any())
            {
                _logger.LogWarning("Returning expired cache with {Count} mappings due to error", _cache.Count);
                return new Dictionary<string, int>(_cache);
            }
            
            throw;
        }
        finally
        {
            _cacheLock.Release();
        }
    }

    private async Task<Dictionary<string, int>> LoadExplicitMappingsAsync()
    {
        var result = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        
        var query = @"
            SELECT DISTINCT 
                LTRIM(RTRIM(FacilityCode)) as FacilityCode,
                LTRIM(RTRIM(Region)) as Region
            FROM [All_Dataset].[dbo].[aPrepDetail] 
            WHERE FacilityCode IS NOT NULL 
              AND LTRIM(RTRIM(FacilityCode)) != '' 
              AND Region IS NOT NULL 
              AND LTRIM(RTRIM(Region)) != ''";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        using var reader = await command.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            var facilityCode = reader.GetString(0);
            var regionName = reader.GetString(1);
            
            if (RegionNameMap.TryGetValue(regionName, out var regionId))
            {
                result[facilityCode] = regionId;
            }
            else
            {
                _logger.LogWarning("Unknown region name '{RegionName}' for facility {FacilityCode}", regionName, facilityCode);
            }
        }

        return result;
    }

    private async Task<List<(string Code, int PatientCount)>> GetARTFacilitiesAsync()
    {
        var result = new List<(string, int)>();
        
        var query = @"
            SELECT 
                LTRIM(RTRIM(FacilityCode)) as FacilityCode,
                COUNT(*) as PatientCount
            FROM [All_Dataset].[dbo].[tmpARTTXOutcomes]
            WHERE TX_CURR = 1
              AND FacilityCode IS NOT NULL
              AND LTRIM(RTRIM(FacilityCode)) != ''
            GROUP BY LTRIM(RTRIM(FacilityCode))
            ORDER BY COUNT(*) DESC";

        using var connection = new SqlConnection(_sourceConnectionString);
        using var command = new SqlCommand(query, connection);
        
        await connection.OpenAsync();
        using var reader = await command.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            var facilityCode = reader.GetString(0);
            var patientCount = reader.GetInt32(1);
            result.Add((facilityCode, patientCount));
        }

        return result;
    }

    private bool TryMapByPrefix(string facilityCode, out int regionId)
    {
        regionId = 0;
        
        if (string.IsNullOrEmpty(facilityCode) || facilityCode.Length < 1)
            return false;

        var prefix = facilityCode.Substring(0, 1).ToUpper();
        return RegionPrefixMap.TryGetValue(prefix, out regionId);
    }

    private void LogRegionDistribution(Dictionary<string, int> mappings)
    {
        var distribution = mappings
            .GroupBy(x => x.Value)
            .Select(g => new { RegionId = g.Key, Count = g.Count() })
            .OrderBy(x => x.RegionId);

        foreach (var region in distribution)
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
    }

    public void InvalidateCache()
    {
        _cache.Clear();
        _lastCacheUpdate = DateTime.MinValue;
        _logger.LogInformation("Facility region cache invalidated");
    }
}