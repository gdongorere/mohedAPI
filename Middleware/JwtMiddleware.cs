using System.IdentityModel.Tokens.Jwt;
using Eswatini.Health.Api.Services.Auth;

namespace Eswatini.Health.Api.Middleware;

public class JwtMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<JwtMiddleware> _logger;
    
    public JwtMiddleware(RequestDelegate next, ILogger<JwtMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }
    
    public async Task InvokeAsync(HttpContext context, IJwtService jwtService)
    {
        var token = ExtractToken(context);
        
        if (!string.IsNullOrEmpty(token))
        {
            var principal = jwtService.ValidateToken(token);
            
            if (principal != null)
            {
                context.User = principal;
            }
        }
        
        await _next(context);
    }
    
    private string? ExtractToken(HttpContext context)
    {
        var authHeader = context.Request.Headers["Authorization"].FirstOrDefault();
        if (!string.IsNullOrEmpty(authHeader) && authHeader.StartsWith("Bearer "))
        {
            return authHeader["Bearer ".Length..].Trim();
        }
        
        return null;
    }
}