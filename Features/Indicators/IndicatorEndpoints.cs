using Microsoft.AspNetCore.Mvc;
using Eswatini.Health.Api.Services.Indicators;
using Eswatini.Health.Api.Services.Period;
using Eswatini.Health.Api.Models.DTOs.Indicators;

namespace Eswatini.Health.Api.Features.Indicators;

public static class IndicatorEndpoints
{
    public static IEndpointRouteBuilder MapIndicatorEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/indicators");

        group.MapGet("/data", GetIndicatorData)
            .WithName("GetIndicatorData")
            .WithOpenApi()
            .Produces<List<IndicatorValueDto>>();

        group.MapGet("/trends", GetIndicatorTrends)
            .WithName("GetIndicatorTrends")
            .WithOpenApi();

        group.MapGet("/available", GetAvailableIndicators)
            .WithName("GetAvailableIndicators")
            .WithOpenApi();

        return app;
    }

    private static async Task<IResult> GetIndicatorData(
        [FromServices] IHIVIndicatorService hivService,
        [FromServices] IPreventionIndicatorService preventionService,
        [FromServices] IPeriodService periodService,
        [FromQuery] string[]? indicators = null,
        [FromQuery] DateTime? startDate = null,
        [FromQuery] DateTime? endDate = null,
        [FromQuery] int? regionId = null,
        [FromQuery] string? ageGroup = null,
        [FromQuery] string? sex = null,
        [FromQuery] string? populationType = null,
        [FromQuery] string? periodType = "daily")
    {
        try
        {
            var request = new IndicatorDataRequest
            {
                Indicators = indicators,
                StartDate = startDate,
                EndDate = endDate ?? DateTime.UtcNow,
                RegionId = regionId,
                AgeGroup = ageGroup,
                Sex = sex,
                PopulationType = populationType,
                PeriodType = periodType
            };

            // Determine which service to use based on indicators
            // This is simplified - in reality you'd need to route to appropriate service
            var hivData = await hivService.GetIndicatorDataAsync(request);
            var preventionData = await preventionService.GetIndicatorDataAsync(request);
            
            var allData = hivData.Concat(preventionData).ToList();

            return Results.Ok(new
            {
                success = true,
                count = allData.Count,
                data = allData
            });
        }
        catch (Exception ex)
        {
            return Results.Problem(ex.Message, statusCode: 500);
        }
    }

    private static async Task<IResult> GetIndicatorTrends(
        [FromServices] IHIVIndicatorService hivService,
        [FromServices] IPreventionIndicatorService preventionService,
        [FromQuery] string[] indicators,
        [FromQuery] DateTime startDate,
        [FromQuery] DateTime endDate,
        [FromQuery] string periodType = "daily")
    {
        try
        {
            var hivTrends = await hivService.GetIndicatorTrendsAsync(indicators, startDate, endDate, periodType);
            var preventionTrends = await preventionService.GetIndicatorTrendsAsync(indicators, startDate, endDate, periodType);

            var allTrends = new Dictionary<string, List<IndicatorValueDto>>();
            foreach (var kvp in hivTrends.Concat(preventionTrends))
            {
                allTrends[kvp.Key] = kvp.Value;
            }

            return Results.Ok(new
            {
                success = true,
                data = allTrends
            });
        }
        catch (Exception ex)
        {
            return Results.Problem(ex.Message, statusCode: 500);
        }
    }

    private static async Task<IResult> GetAvailableIndicators()
    {
        // Return list of available indicators
        var indicators = new[]
        {
            new { Code = "TX_CURR", Name = "Currently on ART", Category = "HIV" },
            new { Code = "TX_NEW", Name = "New on ART", Category = "HIV" },
            new { Code = "TX_VL_TESTED", Name = "Viral Load Tested", Category = "HIV" },
            new { Code = "TX_VL_SUPPRESSED", Name = "Viral Load Suppressed", Category = "HIV" },
            new { Code = "HTS_TST", Name = "HIV Tests Conducted", Category = "Prevention" },
            new { Code = "HTS_POS", Name = "HIV Positive Results", Category = "Prevention" },
            new { Code = "PREP_NEW", Name = "PrEP Initiations", Category = "Prevention" },
            new { Code = "PREP_SEROCONVERSION", Name = "PrEP Seroconversions", Category = "Prevention" }
        };

        return Results.Ok(new
        {
            success = true,
            count = indicators.Length,
            data = indicators
        });
    }
}