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

            group.MapGet("/tb", GetTBDashboard)
    .WithName("GetTBDashboard")
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

private static async Task<IResult> GetTBDashboard(
    [FromServices] ITBIndicatorService tbService,
    [FromServices] IPeriodService periodService,
    [FromQuery] DateTime? startDate = null,
    [FromQuery] DateTime? endDate = null,
    [FromQuery] int? regionId = null)
{
    try
    {
        var end = endDate ?? DateTime.UtcNow;
        var start = startDate ?? new DateTime(end.Year, end.Month, 1);

        // Get TB metrics
        var eligible = await tbService.GetTPTEligibleAsync(start, end, regionId);
        var started = await tbService.GetTPTStartedAsync(start, end, regionId);
        var completed = await tbService.GetTPTCompletedAsync(start, end, regionId);
        var stopped = await tbService.GetTPTStoppedAsync(start, end, regionId);
        var transferredOut = await tbService.GetTPTTransferredOutAsync(start, end, regionId);
        var died = await tbService.GetTPTDiedAsync(start, end, regionId);
        var selfStopped = await tbService.GetTPTSelfStoppedAsync(start, end, regionId);
        var stoppedByClinician = await tbService.GetTPTStoppedByClinicianAsync(start, end, regionId);
        var ltfU = await tbService.GetTPTLTFUAsync(start, end, regionId);

        var dashboard = new
        {
            Period = $"{start:yyyy-MM-dd} to {end:yyyy-MM-dd}",
            Summary = new
            {
                Eligible = eligible,
                Started = started,
                Completed = completed,
                InitiationRate = eligible > 0 ? Math.Round((decimal)started / eligible * 100, 1) : 0,
                CompletionRate = started > 0 ? Math.Round((decimal)completed / started * 100, 1) : 0
            },
            Outcomes = new
            {
                Stopped = stopped,
                TransferredOut = transferredOut,
                Died = died,
                SelfStopped = selfStopped,
                StoppedByClinician = stoppedByClinician,
                LTFU = ltfU
            },
            LastUpdated = DateTime.UtcNow
        };

        return Results.Ok(new
        {
            success = true,
            data = dashboard
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(ex.Message, statusCode: 500);
    }
}
}