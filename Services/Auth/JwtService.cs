using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using Microsoft.IdentityModel.Tokens;
using Eswatini.Health.Api.Models.App;

namespace Eswatini.Health.Api.Services.Auth;

public interface IJwtService
{
    string GenerateToken(User user);
    ClaimsPrincipal? ValidateToken(string token);
}

public class JwtService : IJwtService
{
    private readonly IConfiguration _config;
    private readonly ILogger<JwtService> _logger;
    private readonly SymmetricSecurityKey _key;

    public JwtService(IConfiguration config, ILogger<JwtService> logger)
    {
        _config = config;
        _logger = logger;

        var jwtKey = _config["Jwt:Key"] ??
            throw new InvalidOperationException("JWT Key not configured");

        _key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwtKey));
    }

    public string GenerateToken(User user)
    {
        var claims = new[]
        {
            new Claim(JwtRegisteredClaimNames.Sub, user.Id),
            new Claim(JwtRegisteredClaimNames.Email, user.Email),
            new Claim(ClaimTypes.Role, user.Role),
            new Claim("name", $"{user.Name} {user.Surname}".Trim())
        };

        var creds = new SigningCredentials(_key, SecurityAlgorithms.HmacSha256);
        var expiry = DateTime.UtcNow.AddMinutes(_config.GetValue<int>("Jwt:ExpiryMinutes", 1440));

        var token = new JwtSecurityToken(
            issuer: _config["Jwt:Issuer"],
            audience: _config["Jwt:Audience"],
            claims: claims,
            expires: expiry,
            signingCredentials: creds
        );

        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    public ClaimsPrincipal? ValidateToken(string token)
    {
        try
        {
            var tokenHandler = new JwtSecurityTokenHandler();
            var parameters = new TokenValidationParameters
            {
                ValidateIssuerSigningKey = true,
                IssuerSigningKey = _key,
                ValidateIssuer = true,
                ValidIssuer = _config["Jwt:Issuer"],
                ValidateAudience = true,
                ValidAudience = _config["Jwt:Audience"],
                ValidateLifetime = true,
                ClockSkew = TimeSpan.Zero
            };

            var principal = tokenHandler.ValidateToken(token, parameters, out _);
            return principal;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Token validation failed");
            return null;
        }
    }
}