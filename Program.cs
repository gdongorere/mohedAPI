using Serilog;
using Eswatini.Health.Api.Middleware;
using Eswatini.Health.Api.Startup;
using Eswatini.Health.Api.Data;
using Microsoft.EntityFrameworkCore;
using Eswatini.Health.Api.Services.ETL;

namespace Eswatini.Health.Api;

public class Program
{
    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        // Configure Serilog
        Log.Logger = new LoggerConfiguration()
            .ReadFrom.Configuration(builder.Configuration)
            .Enrich.FromLogContext()
            .WriteTo.Console()
            .WriteTo.File("logs/api-.txt", rollingInterval: RollingInterval.Day)
            .CreateLogger();

        builder.Host.UseSerilog();

       // **** ONLY FILTER OUT SQL COMMANDS - KEEP EVERYTHING ELSE ****
        builder.Logging.AddFilter("Microsoft.EntityFrameworkCore.Database.Command", LogLevel.None);
        builder.Logging.AddFilter("Microsoft.EntityFrameworkCore.Database.Connection", LogLevel.Warning); // Keep connection issues
        builder.Logging.AddFilter("Microsoft.EntityFrameworkCore.Database.Transaction", LogLevel.Warning); // Keep transaction issues
        // **** END OF FILTER SECTION ****

        try
        {
            Log.Information("Starting Eswatini Health API");

            // Add services
            builder.Services.AddDatabaseContexts(builder.Configuration);
            builder.Services.AddApplicationServices();
            builder.Services.AddJwtAuthentication(builder.Configuration);
            builder.Services.AddAuthorizationPolicies();
            builder.Services.AddCorsPolicies();
            builder.Services.AddSwaggerWithJwt();
            builder.Services.AddHealthChecks();
            builder.Services.AddHttpContextAccessor();

            builder.Services.AddHostedService<ScheduledETLService>();

            // Add API Key authentication for ETL
            builder.Services.AddAuthentication()
                .AddScheme<ETLAuthenticationSchemeOptions, ETLAuthenticationHandler>(
                    "ETLKey", options => { });

            var app = builder.Build();

            // Configure pipeline
            if (app.Environment.IsDevelopment())
            {
                app.UseSwagger();
                app.UseSwaggerUI();
            }

            app.UseHttpsRedirection();
            app.UseCors("AngularApp");
            
            // Custom middleware
            app.UseMiddleware<RequestLoggingMiddleware>();
            
            app.UseAuthentication();
            app.UseAuthorization();
            
            app.UseMiddleware<JwtMiddleware>();

            // Health checks
            app.MapHealthChecks("/health");

            // Map API endpoints
            app.MapApiEndpoints();

            // Initialize database
            await app.InitializeDatabaseAsync();

            // Apply pending migrations automatically
            //using (var scope = app.Services.CreateScope())
            //{
            //    var db = scope.ServiceProvider.GetRequiredService<StagingDbContext>();
            //    await db.Database.MigrateAsync();
            //}

            Log.Information("API started successfully");
            app.Run();
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "API failed to start");
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }
}