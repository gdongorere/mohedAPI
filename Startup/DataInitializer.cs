using Microsoft.EntityFrameworkCore;
using Serilog;
using Eswatini.Health.Api.Data;

namespace Eswatini.Health.Api.Startup;

public static class DatabaseInitializer
{
    public static async Task InitializeDatabaseAsync(this WebApplication app)
    {
        using var scope = app.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<StagingDbContext>();
        var configuration = app.Configuration;

        try
        {
            Log.Information("Ensuring database exists...");
            await db.Database.EnsureCreatedAsync();

            Log.Information("Database initialization completed");

            // Update admin password from environment if provided
            var adminEmail = configuration["ADMIN_EMAIL"];
            var adminPassword = configuration["ADMIN_PASSWORD"];

            if (!string.IsNullOrEmpty(adminEmail) && !string.IsNullOrEmpty(adminPassword))
            {
                var admin = await db.Users.FirstOrDefaultAsync(u => u.Email == adminEmail);
                if (admin != null)
                {
                    admin.PasswordHash = BCrypt.Net.BCrypt.HashPassword(adminPassword);
                    await db.SaveChangesAsync();
                    Log.Information("Admin password updated from environment");
                }
            }

            // Log database statistics
            var userCount = await db.Users.CountAsync();
            var hivCount = await db.IndicatorValues_HIV.CountAsync();
            var preventionCount = await db.IndicatorValues_Prevention.CountAsync();
            
            Log.Information("Database stats - Users: {UserCount}, HIV Records: {HIVCount}, Prevention Records: {PreventionCount}", 
                userCount, hivCount, preventionCount);
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Database initialization failed");
            throw;
        }
    }
}