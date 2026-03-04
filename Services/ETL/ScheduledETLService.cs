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
        _logger.LogInformation("=".PadRight(70, '='));
        _logger.LogInformation("🕛 SCHEDULED ETL SERVICE STARTED - Will run daily at midnight");
        _logger.LogInformation("📊 Mode: PROCESSING ALL HISTORICAL DATA (inserts + updates)");
        _logger.LogInformation("⚡ Performance: OPTIMIZED batch updates");
        _logger.LogInformation("=".PadRight(70, '='));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var nextRun = CalculateNextRunTime();
                _logger.LogInformation("Next ETL run scheduled at: {NextRun:yyyy-MM-dd HH:mm:ss}", 
                    DateTime.Now.Add(nextRun));

                await Task.Delay(nextRun, stoppingToken);

                if (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogInformation("");
                    _logger.LogInformation("🚀 " + "=".PadRight(66, '=') + " 🚀");
                    _logger.LogInformation("🚀  STARTING SCHEDULED ETL RUN - PROCESSING ALL DATA  🚀");
                    _logger.LogInformation("🚀 " + "=".PadRight(66, '=') + " 🚀");
                    _logger.LogInformation("");
                    
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
        using var scope = _serviceProvider.CreateScope();
        var etlService = scope.ServiceProvider.GetRequiredService<IETLService>();
        
        var sources = new[] { "hts", "prep", "art" };
        var results = new List<(string Source, int Read, int Inserted, int Updated, int Duration)>();

        foreach (var source in sources)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            try
            {
                _logger.LogInformation("▶️ Running scheduled ETL for source: {Source}", source.ToUpper());
                
                var result = await etlService.RunETLForSourceAsync(source, "scheduler");
                
                results.Add((source, result.RecordsRead, result.RecordsInserted, result.RecordsUpdated, (int)result.DurationMs));
                
                _logger.LogInformation("✅ {Source} completed: Read={Read,8:N0} | Inserted={Inserted,6:N0} | Updated={Updated,6:N0} | Duration={Duration,5}ms",
                    source.ToUpper(), result.RecordsRead, result.RecordsInserted, result.RecordsUpdated, result.DurationMs);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ Failed to run scheduled ETL for source: {Source}", source);
                results.Add((source, 0, 0, 0, 0));
            }
            
            // Small delay between ETL runs to avoid overwhelming the database
            await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
        }

        _logger.LogInformation("");
        _logger.LogInformation("📊 " + "=".PadRight(66, '=') + " 📊");
        _logger.LogInformation("📊  SCHEDULED ETL RUN COMPLETE - FINAL SUMMARY              📊");
        _logger.LogInformation("📊 " + "=".PadRight(66, '=') + " 📊");
        _logger.LogInformation($"{"Source",-8} {"Records Read",12} {"Inserted",12} {"Updated",12} {"Duration",10}");
        _logger.LogInformation("-".PadRight(70, '-'));
        
        foreach (var result in results)
        {
            _logger.LogInformation($"{result.Source.ToUpper(),-8} {result.Read,12:N0} {result.Inserted,12:N0} {result.Updated,12:N0} {result.Duration,10}ms");
        }
        
        _logger.LogInformation("-".PadRight(70, '-'));
        
        var totalRead = results.Sum(r => r.Read);
        var totalInserted = results.Sum(r => r.Inserted);
        var totalUpdated = results.Sum(r => r.Updated);
        var totalDuration = results.Sum(r => r.Duration);
        
        _logger.LogInformation($"{"TOTAL",-8} {totalRead,12:N0} {totalInserted,12:N0} {totalUpdated,12:N0} {totalDuration,10}ms");
        _logger.LogInformation("📊 " + "=".PadRight(66, '=') + " 📊");
        _logger.LogInformation("");
    }

    public override async Task StopAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Scheduled ETL Service is stopping...");
        await base.StopAsync(stoppingToken);
    }
}