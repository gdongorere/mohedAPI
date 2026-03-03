using System.Diagnostics;

namespace Eswatini.Health.Api.Middleware;

public class RequestLoggingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<RequestLoggingMiddleware> _logger;

    public RequestLoggingMiddleware(RequestDelegate next, ILogger<RequestLoggingMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            await _next(context);
            stopwatch.Stop();

            if (stopwatch.ElapsedMilliseconds > 1000)
            {
                _logger.LogWarning("Slow request: {Method} {Path} took {Duration}ms",
                    context.Request.Method,
                    context.Request.Path,
                    stopwatch.ElapsedMilliseconds);
            }

            if (context.Response.StatusCode >= 400)
            {
                _logger.LogWarning("Request failed: {Method} {Path} returned {StatusCode}",
                    context.Request.Method,
                    context.Request.Path,
                    context.Response.StatusCode);
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Unhandled exception in {Method} {Path}",
                context.Request.Method,
                context.Request.Path);
            throw;
        }
    }
}