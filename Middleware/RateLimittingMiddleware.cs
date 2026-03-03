using System.Collections.Concurrent;
using System.IdentityModel.Tokens.Jwt;

namespace Eswatini.Health.Api.Middleware;

public class RateLimitingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<RateLimitingMiddleware> _logger;
    private readonly IConfiguration _config;
    
    private static readonly ConcurrentDictionary<string, ClientRequestTracker> _trackers = new();
    
    public RateLimitingMiddleware(RequestDelegate next, ILogger<RateLimitingMiddleware> logger, IConfiguration config)
    {
        _next = next;
        _logger = logger;
        _config = config;
    }
    
    public async Task InvokeAsync(HttpContext context)
    {
        var clientId = GetClientIdentifier(context);
        var tracker = _trackers.GetOrAdd(clientId, _ => new ClientRequestTracker());
        
        var now = DateTime.UtcNow;
        var minuteLimit = _config.GetValue<int>("RateLimiting:RequestsPerMinute", 60);
        var hourLimit = _config.GetValue<int>("RateLimiting:RequestsPerHour", 1000);
        
        tracker.MinuteRequests.RemoveAll(t => t < now.AddMinutes(-1));
        tracker.HourRequests.RemoveAll(t => t < now.AddHours(-1));
        
        if (tracker.MinuteRequests.Count >= minuteLimit)
        {
            _logger.LogWarning("Rate limit exceeded (minute) for client {ClientId}", clientId);
            
            context.Response.StatusCode = 429;
            context.Response.Headers.RetryAfter = "60";
            await context.Response.WriteAsync("Too many requests. Please try again later.");
            return;
        }
        
        if (tracker.HourRequests.Count >= hourLimit)
        {
            _logger.LogWarning("Rate limit exceeded (hour) for client {ClientId}", clientId);
            
            context.Response.StatusCode = 429;
            context.Response.Headers.RetryAfter = "3600";
            await context.Response.WriteAsync("Hourly request limit exceeded.");
            return;
        }
        
        tracker.MinuteRequests.Add(now);
        tracker.HourRequests.Add(now);
        
        await _next(context);
    }
    
    private string GetClientIdentifier(HttpContext context)
    {
        var userId = context.User.FindFirst(JwtRegisteredClaimNames.Sub)?.Value;
        if (!string.IsNullOrEmpty(userId))
            return $"user:{userId}";
        
        return $"ip:{context.Connection.RemoteIpAddress}";
    }
    
    private class ClientRequestTracker
    {
        public List<DateTime> MinuteRequests { get; set; } = new();
        public List<DateTime> HourRequests { get; set; } = new();
    }
}