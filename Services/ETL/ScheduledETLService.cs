using Eswatini.Health.Api.Services.ETL;

namespace Eswatini.Health.Api.Services.ETL;

public class ScheduledETLService : BackgroundService
{
    private readonly ILogger<ScheduledETLService> _logger;
    private readonly IServiceProvider _serviceProvider;
    private readonly TimeSpan _runTime = TimeSpan.FromHours(0); // Midnight (00:00)

    public ScheduledETLService(
        ILogger<ScheduledETLService> logger,
        IServiceProvider serviceProvider)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Scheduled ETL Service started. Will run daily at midnight.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Calculate time until next run
                var nextRun = CalculateNextRunTime();
                _logger.LogInformation("Next ETL run scheduled at: {NextRun:yyyy-MM-dd HH:mm:ss}", 
                    DateTime.Now.Add(nextRun));

                // Wait until the scheduled time
                await Task.Delay(nextRun, stoppingToken);

                if (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Starting scheduled ETL run at {Time}", DateTime.Now);
                    await RunAllETLsAsync(stoppingToken);
                }
            }
            catch (TaskCanceledException)
            {
                _logger.LogInformation("Scheduled ETL Service was cancelled.");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred in scheduled ETL service");
                
                // Wait 5 minutes before retrying if there's an error
                try { await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken); } catch { break; }
            }
        }
    }

    private TimeSpan CalculateNextRunTime()
    {
        var now = DateTime.Now;
        var nextRun = now.Date.Add(_runTime); // Today at midnight
        
        // If it's already past today's run time, schedule for tomorrow
        if (now > nextRun)
        {
            nextRun = nextRun.AddDays(1);
        }
        
        return nextRun - now;
    }

    private async Task RunAllETLsAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("=".PadRight(60, '='));
        _logger.LogInformation("Starting scheduled ETL run for all sources");
        _logger.LogInformation("=".PadRight(60, '='));

        using var scope = _serviceProvider.CreateScope();
        var etlService = scope.ServiceProvider.GetRequiredService<IETLService>();
        
        var sources = new[] { "hts", "prep", "art" };
        var results = new List<(string Source, bool Success, int Inserted)>();

        foreach (var source in sources)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            try
            {
                _logger.LogInformation("▶️ Running scheduled ETL for source: {Source}", source);
                
                var result = await etlService.RunETLForSourceAsync(source, "scheduler");
                
                results.Add((source, result.Success, result.RecordsInserted));
                
                _logger.LogInformation("✅ {Source} completed: Success={Success}, Inserted={Inserted}, Duration={Duration}ms",
                    source, result.Success, result.RecordsInserted, result.DurationMs);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ Failed to run scheduled ETL for source: {Source}", source);
                results.Add((source, false, 0));
            }
            
            // Small delay between ETL runs to avoid overwhelming the database
            await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
        }

        _logger.LogInformation("=".PadRight(60, '='));
        _logger.LogInformation("Scheduled ETL run completed - Summary");
        _logger.LogInformation("=".PadRight(60, '='));
        foreach (var result in results)
        {
            var status = result.Success ? "✅" : "❌";
            _logger.LogInformation($"{status} {result.Source.PadRight(10)}: Inserted {result.Inserted,10:N0} records");
        }
        _logger.LogInformation("=".PadRight(60, '='));
    }

    public override async Task StopAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Scheduled ETL Service is stopping...");
        await base.StopAsync(stoppingToken);
    }
}