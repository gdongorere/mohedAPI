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
        return query;
    }

    protected List<IndicatorValueDto> MapToDto<T>(IEnumerable<T> records, string tableType) where T : class
    {
        var result = new List<IndicatorValueDto>();
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

    protected static string GetRegionCode(int regionId)
    {
        return regionId switch
        {
            1 => "HH",
            2 => "MN",
            3 => "LB",
            4 => "SH",
            _ => "UN"
        };
    }

    protected static int GetRegionId(string regionCode)
    {
        return regionCode.ToUpper() switch
        {
            "HH" => 1,
            "MN" => 2,
            "LB" => 3,
            "SH" => 4,
            _ => 0
        };
    }
}