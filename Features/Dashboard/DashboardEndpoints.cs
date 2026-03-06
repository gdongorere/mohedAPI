using Microsoft.AspNetCore.Mvc;
using Eswatini.Health.Api.Services.Indicators;
using Eswatini.Health.Api.Services.Period;
using Eswatini.Health.Api.Common.Helpers;

namespace Eswatini.Health.Api.Features.Dashboard;

public static class DashboardEndpoints
{
    public static IEndpointRouteBuilder MapDashboardEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/dashboard");

        group.MapGet("/", GetDashboard)
            .WithName("GetDashboard")
            .WithOpenApi();

        group.MapGet("/detailed", GetDetailedDashboard)
            .WithName("GetDetailedDashboard")
            .WithOpenApi();

        group.MapGet("/regions", GetRegionalDashboard)
            .WithName("GetRegionalDashboard")
            .WithOpenApi();

        return app;
    }

    private static async Task<IResult> GetDashboard(
        [FromServices] IHIVIndicatorService hivService,
        [FromServices] IPeriodService periodService,
        [FromQuery] string? period = null)
    {
        try
        {
            var targetPeriod = period ?? await periodService.GetLatestAvailablePeriodAsync();

            if (!periodService.IsValidPeriod(targetPeriod))
            {
                return Results.BadRequest(new
                {
                    success = false,
                    message = "Invalid period format. Use format: YYYYQX (e.g., 2025Q3)"
                });
            }

            if (!await hivService.HasDataForPeriodAsync(targetPeriod))
            {
                return Results.Ok(new
                {
                    success = true,
                    timestamp = DateTime.UtcNow,
                    message = $"No data available for period {targetPeriod}",
                    data = new { }
                });
            }

            var dashboardData = await hivService.GetDashboardSummaryAsync(targetPeriod);

            return Results.Ok(new
            {
                success = true,
                timestamp = DateTime.UtcNow,
                data = dashboardData
            });
        }
        catch (Exception ex)
        {
            return Results.Problem(ex.Message, statusCode: 500);
        }
    }

    private static async Task<IResult> GetDetailedDashboard(
        [FromServices] IHIVIndicatorService hivService,
        [FromServices] IPeriodService periodService,
        [FromQuery] string? period = null)
    {
        try
        {
            var targetPeriod = period ?? await periodService.GetLatestAvailablePeriodAsync();

            if (!periodService.IsValidPeriod(targetPeriod))
            {
                return Results.BadRequest(new
                {
                    success = false,
                    message = "Invalid period format. Use format: YYYYQX (e.g., 2025Q3)"
                });
            }

            if (!await hivService.HasDataForPeriodAsync(targetPeriod))
            {
                return Results.Ok(new
                {
                    success = true,
                    timestamp = DateTime.UtcNow,
                    message = $"No data available for period {targetPeriod}",
                    data = new { }
                });
            }

            var detailedData = await hivService.GetDetailedDashboardAsync(targetPeriod);

            return Results.Ok(new
            {
                success = true,
                timestamp = DateTime.UtcNow,
                data = detailedData
            });
        }
        catch (Exception ex)
        {
            return Results.Problem(ex.Message, statusCode: 500);
        }
    }

    private static async Task<IResult> GetRegionalDashboard(
        [FromServices] IHIVIndicatorService hivService,
        [FromServices] IPeriodService periodService,
        [FromQuery] string? period = null)
    {
        try
        {
            var targetPeriod = period ?? await periodService.GetLatestAvailablePeriodAsync();

            if (!periodService.IsValidPeriod(targetPeriod))
            {
                return Results.BadRequest(new
                {
                    success = false,
                    message = "Invalid period format. Use format: YYYYQX (e.g., 2025Q3)"
                });
            }

            var regionalData = await hivService.GetBreakdownByRegionAsync(targetPeriod);

            return Results.Ok(new
            {
                success = true,
                timestamp = DateTime.UtcNow,
                period = targetPeriod,
                count = regionalData.Count,
                data = regionalData
            });
        }
        catch (Exception ex)
        {
            return Results.Problem(ex.Message, statusCode: 500);
        }
    }
}