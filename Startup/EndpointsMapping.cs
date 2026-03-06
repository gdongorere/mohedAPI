using Eswatini.Health.Api.Features.Auth;
using Eswatini.Health.Api.Features.Dashboard;
using Eswatini.Health.Api.Features.ETL;
using Eswatini.Health.Api.Features.Indicators;
using Eswatini.Health.Api.Features.Regions;
using Eswatini.Health.Api.Features.Targets;
using Eswatini.Health.Api.Features.Users;

namespace Eswatini.Health.Api.Startup;

public static class EndpointMappings
{
    public static WebApplication MapApiEndpoints(this WebApplication app)
    {
        // Health check (public)
        app.MapGet("/health", () => Results.Ok(new
        {
            status = "healthy",
            timestamp = DateTime.UtcNow,
            version = "1.0.0",
            database = "EswatiniHealth_Staging"
        }))
        .WithName("HealthCheck")
        .WithOpenApi()
        .AllowAnonymous();

        // Auth endpoints (public for login/register)
        app.MapGroup("")
           .MapAuthEndpoints();

        // ETL endpoints (API key protected, no JWT required)
        app.MapGroup("/api/etl")
           .MapETLEndpoints();

        // Protected endpoints
        var protectedGroup = app.MapGroup("/api")
           .RequireAuthorization();

        protectedGroup.MapIndicatorEndpoints();
        protectedGroup.MapDashboardEndpoints();
        protectedGroup.MapRegionEndpoints();
        protectedGroup.MapTargetEndpoints();
        protectedGroup.MapUserEndpoints();

        return app;
    }
}