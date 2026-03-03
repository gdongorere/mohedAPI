using Microsoft.AspNetCore.Mvc;

namespace Eswatini.Health.Api.Features.Regions;

public static class RegionEndpoints
{
    public static IEndpointRouteBuilder MapRegionEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/regions", GetRegions)
           .WithName("GetRegions")
           .WithOpenApi();

        return app;
    }

    private static IResult GetRegions()
    {
        var regions = new[]
        {
            new { Id = 1, Name = "Hhohho", Code = "HH" },
            new { Id = 2, Name = "Manzini", Code = "MN" },
            new { Id = 3, Name = "Shiselweni", Code = "SH" },
            new { Id = 4, Name = "Lubombo", Code = "LB" }
        };

        return Results.Ok(new
        {
            success = true,
            count = regions.Length,
            data = regions
        });
    }
}