using Microsoft.EntityFrameworkCore;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.Targets;
using Eswatini.Health.Api.Models.DTOs.Targets;
using Eswatini.Health.Api.Services.Period;

namespace Eswatini.Health.Api.Services.Targets;

public interface ITargetService
{
    // Get targets with National/Regional filtering
    Task<TargetDto?> GetTargetAsync(string indicator, int? regionId, int year, int? quarter = null, int? month = null);
    Task<List<TargetDto>> GetTargetsAsync(string? indicator = null, int? regionId = null, int? year = null);
    Task<TargetSummaryDto> GetTargetSummaryAsync(string indicator, int year, int? quarter = null, int? month = null);
    
    // CRUD operations (Admin only)
    Task<TargetDto> CreateTargetAsync(CreateTargetRequest request, string userId);
    Task<TargetDto?> UpdateTargetAsync(int id, UpdateTargetRequest request);
    Task<bool> DeleteTargetAsync(int id);
    
    // Dashboard helper
    Task<Dictionary<string, decimal>> GetTargetsForDashboardAsync(DateTime date, int? regionId = null);
}

public class TargetService : ITargetService
{
    private readonly StagingDbContext _db;
    private readonly ILogger<TargetService> _logger;
    private readonly IPeriodService _periodService;

    public TargetService(
        StagingDbContext db,
        ILogger<TargetService> logger,
        IPeriodService periodService)
    {
        _db = db;
        _logger = logger;
        _periodService = periodService;
    }

    public async Task<TargetDto?> GetTargetAsync(string indicator, int? regionId, int year, int? quarter = null, int? month = null)
    {
        try
        {
            var query = _db.IndicatorTargets
                .Where(x => x.Indicator == indicator 
                    && x.Year == year);

            // Handle region filtering (NULL = National)
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);
            else
                query = query.Where(x => x.RegionId == null);

            if (quarter.HasValue)
                query = query.Where(x => x.Quarter == quarter.Value);
            
            if (month.HasValue)
                query = query.Where(x => x.Month == month.Value);

            var target = await query.FirstOrDefaultAsync();
            
            if (target == null)
                return null;

            return MapToDto(target);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting target");
            return null;
        }
    }

    public async Task<List<TargetDto>> GetTargetsAsync(string? indicator = null, int? regionId = null, int? year = null)
    {
        try
        {
            var query = _db.IndicatorTargets.AsQueryable();

            if (!string.IsNullOrEmpty(indicator))
                query = query.Where(x => x.Indicator == indicator);
            
            // regionId = null returns National targets, specific region returns that region's targets
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);
            
            if (year.HasValue)
                query = query.Where(x => x.Year == year.Value);

            var targets = await query
                .OrderBy(x => x.Indicator)
                .ThenBy(x => x.RegionId ?? 0) // Nulls (National) first
                .ThenBy(x => x.Year)
                .ThenBy(x => x.Quarter)
                .ThenBy(x => x.Month)
                .ToListAsync();

            return targets.Select(MapToDto).ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting targets");
            return new List<TargetDto>();
        }
    }

    public async Task<TargetSummaryDto> GetTargetSummaryAsync(string indicator, int year, int? quarter = null, int? month = null)
    {
        try
        {
            var summary = new TargetSummaryDto
            {
                Indicator = indicator,
                Year = year
            };

            // Get National target
            var nationalTarget = await GetTargetAsync(indicator, null, year, quarter, month);
            summary.NationalTarget = nationalTarget?.TargetValue;

            // Get Regional targets
            var regions = new[] { 1, 2, 3, 4 };
            foreach (var regionId in regions)
            {
                var regionTarget = await GetTargetAsync(indicator, regionId, year, quarter, month);
                if (regionTarget != null)
                {
                    summary.RegionalTargets[regionId] = regionTarget.TargetValue;
                }
            }

            // TODO: Get achieved value from indicator data
            // This would come from your indicator services
            // summary.Achieved = await _indicatorService.GetValueAsync(...);
            
            if (summary.Achieved.HasValue && summary.NationalTarget.HasValue && summary.NationalTarget.Value > 0)
            {
                summary.PercentageAchieved = Math.Round(summary.Achieved.Value / summary.NationalTarget.Value * 100, 1);
            }

            return summary;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting target summary");
            return new TargetSummaryDto { Indicator = indicator, Year = year };
        }
    }

    public async Task<TargetDto> CreateTargetAsync(CreateTargetRequest request, string userId)
    {
        try
        {
            // Validate period
            if (request.Quarter.HasValue && request.Month.HasValue)
                throw new ArgumentException("Cannot specify both quarter and month");

            // Check if target already exists
            var existing = await GetTargetAsync(
                request.Indicator, 
                request.RegionId, 
                request.Year, 
                request.Quarter, 
                request.Month);
                
            if (existing != null)
                throw new InvalidOperationException("Target already exists for this period and region");

            var target = new IndicatorTarget
            {
                Indicator = request.Indicator,
                RegionId = request.RegionId,
                Year = request.Year,
                Quarter = request.Quarter,
                Month = request.Month,
                TargetValue = request.TargetValue,
                TargetType = request.TargetType,
                Notes = request.Notes,
                AgeGroup = request.AgeGroup,
                Sex = request.Sex,
                PopulationType = request.PopulationType,
                CreatedBy = userId,
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTime.UtcNow
            };

            _db.IndicatorTargets.Add(target);
            await _db.SaveChangesAsync();

            return MapToDto(target);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating target");
            throw;
        }
    }

    public async Task<TargetDto?> UpdateTargetAsync(int id, UpdateTargetRequest request)
    {
        try
        {
            var target = await _db.IndicatorTargets.FindAsync(id);
            if (target == null)
                return null;

            target.TargetValue = request.TargetValue;
            target.Notes = request.Notes ?? target.Notes;
            target.UpdatedAt = DateTime.UtcNow;

            await _db.SaveChangesAsync();

            return MapToDto(target);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating target {Id}", id);
            throw;
        }
    }

    public async Task<bool> DeleteTargetAsync(int id)
    {
        try
        {
            var target = await _db.IndicatorTargets.FindAsync(id);
            if (target == null)
                return false;

            _db.IndicatorTargets.Remove(target);
            await _db.SaveChangesAsync();

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting target {Id}", id);
            throw;
        }
    }

    public async Task<Dictionary<string, decimal>> GetTargetsForDashboardAsync(DateTime date, int? regionId = null)
    {
        try
        {
            var year = date.Year;
            var month = date.Month;
            var quarter = (month - 1) / 3 + 1;

            var query = _db.IndicatorTargets
                .Where(x => x.Year == year);

            // Filter by region (NULL = National, specific value = that region)
            if (regionId.HasValue)
                query = query.Where(x => x.RegionId == regionId.Value);
            else
                query = query.Where(x => x.RegionId == null); // National targets for dashboard

            // Get targets that apply to this period
            var targets = await query
                .Where(x => x.Month == month || x.Quarter == quarter || (x.Month == null && x.Quarter == null))
                .ToListAsync();

            return targets.ToDictionary(
                x => $"{x.Indicator}_{x.RegionId ?? 0}_{x.Year}_{x.Quarter}_{x.Month}",
                x => x.TargetValue);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting targets for dashboard");
            return new Dictionary<string, decimal>();
        }
    }

    private TargetDto MapToDto(IndicatorTarget target)
    {
        var period = target.Year.ToString();
        if (target.Quarter.HasValue)
            period = $"{target.Year}-Q{target.Quarter}";
        else if (target.Month.HasValue)
            period = $"{target.Year}-{target.Month:D2}";

        return new TargetDto
        {
            Id = target.Id,
            Indicator = target.Indicator,
            RegionId = target.RegionId,
            RegionName = GetRegionName(target.RegionId),
            Year = target.Year,
            Quarter = target.Quarter,
            Month = target.Month,
            TargetValue = target.TargetValue,
            TargetType = target.TargetType,
            Notes = target.Notes,
            Period = period,
            Level = target.RegionId.HasValue ? "Regional" : "National"
        };
    }

    private string GetRegionName(int? regionId)
    {
        if (!regionId.HasValue) return "National";
        
        return regionId.Value switch
        {
            1 => "Hhohho",
            2 => "Manzini",
            3 => "Shiselweni",
            4 => "Lubombo",
            _ => "Unknown"
        };
    }
}