using Microsoft.EntityFrameworkCore;
using Eswatini.Health.Api.Data;
using Eswatini.Health.Api.Models.DTOs.Indicators;
using Eswatini.Health.Api.Services.Period;

namespace Eswatini.Health.Api.Services.Indicators;

public abstract class IndicatorServiceBase
{
    protected readonly StagingDbContext _db;
    protected readonly ILogger _logger;
    protected readonly IPeriodService _periodService;

    protected IndicatorServiceBase(
        StagingDbContext db,
        ILogger logger,
        IPeriodService periodService)
    {
        _db = db;
        _logger = logger;
        _periodService = periodService;
    }

    protected IQueryable<T> GetLatestRecords<T>(IQueryable<T> query) where T : class
    {
        // Note: In a real implementation, you'd use the vw_LatestIndicatorValues views
        // This is handled by EF Core when you query against the views
        return query;
    }

    protected List<IndicatorValueDto> MapToDto<T>(IEnumerable<T> records, string tableType) where T : class
    {
        var result = new List<IndicatorValueDto>();
        
        // This will be implemented by specific services
        return result;
    }

    protected static string GetRegionName(int regionId)
    {
        return regionId switch
        {
            1 => "Hhohho",
            2 => "Manzini",
            3 => "Shiselweni",
            4 => "Lubombo",
            _ => "Unknown"
        };
    }
}