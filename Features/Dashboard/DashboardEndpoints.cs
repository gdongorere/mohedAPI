using Microsoft.AspNetCore.Mvc;
using Eswatini.Health.Api.Services.Indicators;
using Eswatini.Health.Api.Services.Targets;
using Eswatini.Health.Api.Services.Period;
using Eswatini.Health.Api.Models.DTOs.Dashboard;
using Eswatini.Health.Api.Models.DTOs.Indicators;

namespace Eswatini.Health.Api.Features.Dashboard;

public static class DashboardEndpoints
{
    public static IEndpointRouteBuilder MapDashboardEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/dashboard");

        group.MapGet("/summary", GetDashboardSummary)
            .WithName("GetDashboardSummary")
            .WithOpenApi();

        group.MapGet("/hiv", GetHIVDashboard)
            .WithName("GetHIVDashboard")
            .WithOpenApi();

        group.MapGet("/prevention", GetPreventionDashboard)
            .WithName("GetPreventionDashboard")
            .WithOpenApi();

        return app;
    }

    private static async Task<IResult> GetDashboardSummary(
        [FromServices] IHIVIndicatorService hivService,
        [FromServices] IPreventionIndicatorService preventionService,
        [FromServices] ITargetService targetService,
        [FromServices] IPeriodService periodService,
        [FromQuery] DateTime? asOfDate = null)
    {
        try
        {
            var date = asOfDate ?? DateTime.UtcNow;
            var targets = await targetService.GetTargetsForDashboardAsync(date);

            // Get HIV metrics
            var totalOnArt = await hivService.GetTotalOnArtAsync(date);
            var (vlTested, vlSuppressed) = await hivService.GetViralLoadOutcomesAsync(date);
            
            // Get Prevention metrics
            var monthStart = new DateTime(date.Year, date.Month, 1);
            var hivTests = await preventionService.GetHIVTestsAsync(monthStart, date);
            var hivPositives = await preventionService.GetHIVPositivesAsync(monthStart, date);
            var prepInitiations = await preventionService.GetPrEPInitiationsAsync(monthStart, date);

            var metrics = new List<MetricDto>
            {
                new() {
                    Indicator = "TX_CURR",
                    Name = "Currently on ART",
                    Value = totalOnArt,
                    Target = targets.GetValueOrDefault($"TX_CURR_0_{date.Year}__"),
                    Unit = "number",
                    Trend = "stable"
                },
                new() {
                    Indicator = "VL_SUPPRESSION",
                    Name = "Viral Load Suppression Rate",
                    Value = vlTested > 0 ? Math.Round((decimal)vlSuppressed / vlTested * 100, 1) : 0,
                    Unit = "percentage",
                    Trend = "stable"
                },
                new() {
                    Indicator = "HTS_TST",
                    Name = "HIV Tests (MTD)",
                    Value = hivTests,
                    Unit = "number",
                    Trend = "stable"
                },
                new() {
                    Indicator = "HTS_POS",
                    Name = "HIV Positives (MTD)",
                    Value = hivPositives,
                    Unit = "number",
                    Trend = "stable"
                },
                new() {
                    Indicator = "PREP_NEW",
                    Name = "PrEP Initiations (MTD)",
                    Value = prepInitiations,
                    Unit = "number",
                    Trend = "stable"
                }
            };

            // Calculate percentages of target
            foreach (var metric in metrics.Where(m => m.Target.HasValue && m.Target.Value > 0))
            {
                metric.PercentageOfTarget = Math.Round(metric.Value / metric.Target!.Value * 100, 1);
            }

            var dashboard = new DashboardSummaryDto
            {
                AsOfDate = date,
                Metrics = metrics,
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

    private static async Task<IResult> GetHIVDashboard(
        [FromServices] IHIVIndicatorService hivService,
        [FromServices] IPeriodService periodService,
        [FromQuery] DateTime? asOfDate = null)
    {
        try
        {
            var date = asOfDate ?? DateTime.UtcNow;

            // Get total on ART
            var totalOnArt = await hivService.GetTotalOnArtAsync(date);
            
            // Get viral load outcomes
            var (vlTested, vlSuppressed) = await hivService.GetViralLoadOutcomesAsync(date);
            
            // Get breakdowns
            var bySex = await hivService.GetBreakdownBySexAsync("TX_CURR", date);
            var byAgeGroup = await hivService.GetBreakdownByAgeGroupAsync("TX_CURR", date);

            // Create charts
            var sexChart = new ChartDataDto
            {
                Title = "Clients on ART by Sex",
                ChartType = "pie",
                Labels = bySex.Keys.ToList(),
                Datasets = new List<ChartDatasetDto>
                {
                    new()
                    {
                        Label = "On ART",
                        Data = bySex.Values.Select(v => (decimal)v).ToList(),
                        BackgroundColor = "#4CAF50"
                    }
                }
            };

            var ageChart = new ChartDataDto
            {
                Title = "Clients on ART by Age Group",
                ChartType = "bar",
                Labels = byAgeGroup.Keys.ToList(),
                Datasets = new List<ChartDatasetDto>
                {
                    new()
                    {
                        Label = "On ART",
                        Data = byAgeGroup.Values.Select(v => (decimal)v).ToList(),
                        BorderColor = "#2196F3"
                    }
                }
            };

            var dashboard = new
            {
                AsOfDate = date,
                Summary = new
                {
                    TotalOnArt = totalOnArt,
                    ViralLoadTested = vlTested,
                    ViralLoadSuppressed = vlSuppressed,
                    SuppressionRate = vlTested > 0 ? Math.Round((decimal)vlSuppressed / vlTested * 100, 1) : 0
                },
                Charts = new[] { sexChart, ageChart },
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

    private static async Task<IResult> GetPreventionDashboard(
        [FromServices] IPreventionIndicatorService preventionService,
        [FromServices] IPeriodService periodService,
        [FromQuery] DateTime? startDate = null,
        [FromQuery] DateTime? endDate = null)
    {
        try
        {
            var end = endDate ?? DateTime.UtcNow;
            var start = startDate ?? new DateTime(end.Year, end.Month, 1);

            // Get metrics
            var hivTests = await preventionService.GetHIVTestsAsync(start, end);
            var hivPositives = await preventionService.GetHIVPositivesAsync(start, end);
            var prepInitiations = await preventionService.GetPrEPInitiationsAsync(start, end);
            var seroconversions = await preventionService.GetPrEPSeroconversionsAsync(start, end);

            var dashboard = new
            {
                Period = $"{start:yyyy-MM-dd} to {end:yyyy-MM-dd}",
                Summary = new
                {
                    HIVTests = hivTests,
                    HIVPositives = hivPositives,
                    PositivityRate = hivTests > 0 ? Math.Round((decimal)hivPositives / hivTests * 100, 1) : 0,
                    PrEPInitiations = prepInitiations,
                    Seroconversions = seroconversions,
                    SeroconversionRate = prepInitiations > 0 ? Math.Round((decimal)seroconversions / prepInitiations * 100, 2) : 0
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