using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;
using System.Security.Claims;
using System.Text.Encodings.Web;

namespace Eswatini.Health.Api.Middleware;

public class ETLAuthenticationSchemeOptions : AuthenticationSchemeOptions
{
    public string? ApiKey { get; set; }
}

public class ETLAuthenticationHandler : AuthenticationHandler<ETLAuthenticationSchemeOptions>
{
    private readonly IConfiguration _configuration;

    public ETLAuthenticationHandler(
        IOptionsMonitor<ETLAuthenticationSchemeOptions> options,
        ILoggerFactory logger,
        UrlEncoder encoder,
        IConfiguration configuration)
        : base(options, logger, encoder)
    {
        _configuration = configuration;
    }

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (!Request.Path.StartsWithSegments("/api/etl"))
        {
            return Task.FromResult(AuthenticateResult.NoResult());
        }

        if (!Request.Headers.TryGetValue("X-ETL-Key", out var apiKey))
        {
            return Task.FromResult(AuthenticateResult.Fail("ETL API key required"));
        }

        var validKey = _configuration["ETL:ApiKey"];
        if (string.IsNullOrEmpty(validKey) || apiKey != validKey)
        {
            return Task.FromResult(AuthenticateResult.Fail("Invalid ETL API key"));
        }

        var claims = new[]
        {
            new Claim(ClaimTypes.Name, "ETL Service"),
            new Claim(ClaimTypes.Role, "ETL"),
            new Claim("ETL_Access", "true")
        };

        var identity = new ClaimsIdentity(claims, Scheme.Name);
        var principal = new ClaimsPrincipal(identity);
        var ticket = new AuthenticationTicket(principal, Scheme.Name);

        return Task.FromResult(AuthenticateResult.Success(ticket));
    }
}