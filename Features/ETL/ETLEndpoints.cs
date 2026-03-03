using Microsoft.AspNetCore.Mvc;
using Eswatini.Health.Api.Services.ETL;

namespace Eswatini.Health.Api.Features.ETL;

public static class ETLEndpoints
{
    public static IEndpointRouteBuilder MapETLEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("");

        group.MapPost("/trigger", TriggerETL)
            .WithName("TriggerETL")
            .WithOpenApi()
            .AddEndpointFilter<ETLAuthenticationFilter>();

        group.MapGet("/status/{jobName}", GetETLStatus)
            .WithName("GetETLStatus")
            .WithOpenApi()
            .RequireAuthorization("AdminOnly");

        group.MapGet("/history", GetETLHistory)
            .WithName("GetETLHistory")
            .WithOpenApi()
            .RequireAuthorization("AdminOnly");

        group.MapGet("/last-runs", GetLastRunTimes)
            .WithName("GetLastRunTimes")
            .WithOpenApi()
            .RequireAuthorization("AdminOnly");

        return app;
    }

    private static async Task<IResult> TriggerETL(
        [FromServices] IETLService etlService,
        HttpContext context,
        [FromQuery] string source)
    {
        try
        {
            var triggeredBy = context.Request.Headers["X-ETL-Key"].FirstOrDefault() ?? "system";

            var result = await etlService.RunETLForSourceAsync(source, triggeredBy);

            if (result.Success)
            {
                return Results.Ok(new
                {
                    success = true,
                    message = $"ETL completed successfully for {source}",
                    timestamp = DateTime.UtcNow,
                    data = result
                });
            }
            else
            {
                return Results.BadRequest(new
                {
                    success = false,
                    message = result.ErrorMessage,
                    timestamp = DateTime.UtcNow,
                    data = result
                });
            }
        }
        catch (Exception ex)
        {
            return Results.Ok(new
            {
                success = false,
                message = ex.Message,
                timestamp = DateTime.UtcNow,
                data = new { }
            });
        }
    }

    private static async Task<IResult> GetETLStatus(
        [FromServices] IETLService etlService,
        string jobName)
    {
        try
        {
            var status = await etlService.GetJobStatusAsync(jobName);

            return Results.Ok(new
            {
                success = true,
                timestamp = DateTime.UtcNow,
                data = status
            });
        }
        catch (Exception ex)
        {
            return Results.Ok(new
            {
                success = false,
                message = ex.Message,
                timestamp = DateTime.UtcNow,
                data = new { }
            });
        }
    }

    private static async Task<IResult> GetETLHistory(
        [FromServices] IETLService etlService,
        [FromQuery] string? jobName = null,
        [FromQuery] int limit = 100)
    {
        try
        {
            var history = await etlService.GetETLHistoryAsync(jobName, limit);

            return Results.Ok(new
            {
                success = true,
                timestamp = DateTime.UtcNow,
                count = history.Count,
                data = history
            });
        }
        catch (Exception ex)
        {
            return Results.Ok(new
            {
                success = false,
                message = ex.Message,
                timestamp = DateTime.UtcNow,
                data = new List<object>()
            });
        }
    }

    private static async Task<IResult> GetLastRunTimes(
        [FromServices] IETLService etlService)
    {
        try
        {
            var lastRuns = await etlService.GetLastRunTimesAsync();

            return Results.Ok(new
            {
                success = true,
                timestamp = DateTime.UtcNow,
                data = lastRuns
            });
        }
        catch (Exception ex)
        {
            return Results.Ok(new
            {
                success = false,
                message = ex.Message,
                timestamp = DateTime.UtcNow,
                data = new { }
            });
        }
    }
}

// Custom authentication filter for ETL endpoint (matches old API)
public class ETLAuthenticationFilter : IEndpointFilter
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<ETLAuthenticationFilter> _logger;

    public ETLAuthenticationFilter(IConfiguration configuration, ILogger<ETLAuthenticationFilter> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    public async ValueTask<object?> InvokeAsync(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        var httpContext = context.HttpContext;
        
        if (!httpContext.Request.Headers.TryGetValue("X-ETL-Key", out var apiKey))
        {
            _logger.LogWarning("ETL trigger attempted without API key");
            return Results.Json(new
            {
                success = false,
                message = "ETL API key required"
            }, statusCode: 401);
        }

        var validKey = _configuration["ETL:ApiKey"];
        if (string.IsNullOrEmpty(validKey) || apiKey != validKey)
        {
            _logger.LogWarning("Invalid ETL API key attempted");
            return Results.Json(new
            {
                success = false,
                message = "Invalid ETL API key"
            }, statusCode: 401);
        }

        return await next(context);
    }
}