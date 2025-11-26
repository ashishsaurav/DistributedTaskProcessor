namespace DistributedTaskProcessor.Collector.Services;

/// <summary>
/// Background service wrapper for AdvancedResultCollectorService
/// Ensures continuous result collection and batch processing
/// </summary>
public class AdvancedResultCollectorBackgroundService : BackgroundService
{
    private readonly AdvancedResultCollectorService _advancedCollectorService;
    private readonly ILogger<AdvancedResultCollectorBackgroundService> _logger;

    public AdvancedResultCollectorBackgroundService(
        AdvancedResultCollectorService advancedCollectorService,
        ILogger<AdvancedResultCollectorBackgroundService> logger)
    {
        _advancedCollectorService = advancedCollectorService;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Advanced Result Collector Background Service started");

        try
        {
            await _advancedCollectorService.StartCollectorAsync(stoppingToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in Advanced Result Collector Background Service");
        }

        _logger.LogInformation("Advanced Result Collector Background Service stopped");
    }
}
