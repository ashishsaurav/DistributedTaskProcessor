using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using DistributedTaskProcessor.Infrastructure.Data;
using DistributedTaskProcessor.Shared.Models;

namespace DistributedTaskProcessor.Coordinator.Services;

/// <summary>
/// Background service for automatically fetching and ingesting source data
/// </summary>
public class DataFetcherBackgroundService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<DataFetcherBackgroundService> _logger;
    private readonly int _intervalSeconds;

    public DataFetcherBackgroundService(
        IServiceProvider serviceProvider,
        ILogger<DataFetcherBackgroundService> logger,
        IConfiguration configuration)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _intervalSeconds = configuration.GetValue("DataIngestionIntervalSeconds", 30);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Data Fetcher Background Service started with interval {IntervalSeconds}s", _intervalSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _serviceProvider.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TaskDbContext>();

                // Check if there's pending source data to process
                var pendingSourceData = await dbContext.SourceMarketData
                    .FirstOrDefaultAsync(s => s.Status == "Pending", stoppingToken);

                if (pendingSourceData != null)
                {
                    _logger.LogInformation("Found pending source data: {SourceDataId}", pendingSourceData.Id);
                    // Mark as processing
                    pendingSourceData.Status = "Processing";
                    await dbContext.SaveChangesAsync(stoppingToken);
                }

                await Task.Delay(TimeSpan.FromSeconds(_intervalSeconds), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Data Fetcher Background Service cancelled during shutdown");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in Data Fetcher Background Service");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        _logger.LogInformation("Data Fetcher Background Service stopped");
    }
}
