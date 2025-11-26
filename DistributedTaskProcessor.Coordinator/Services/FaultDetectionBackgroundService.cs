namespace DistributedTaskProcessor.Coordinator.Services;

using DistributedTaskProcessor.Shared.Configuration;

/// <summary>
/// Background service wrapper for FaultDetectionService
/// Runs fault detection every interval
/// </summary>
public class FaultDetectionBackgroundService : BackgroundService
{
    private readonly FaultDetectionService _faultDetectionService;
    private readonly ILogger<FaultDetectionBackgroundService> _logger;
    private readonly int _intervalSeconds;

    public FaultDetectionBackgroundService(
        FaultDetectionService faultDetectionService,
        ILogger<FaultDetectionBackgroundService> logger,
        IConfiguration configuration)
    {
        _faultDetectionService = faultDetectionService;
        _logger = logger;
        _intervalSeconds = configuration.GetSection("MultiTenant").Get<MultiTenantSettings>()?.HeartbeatIntervalSeconds ?? 60;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Fault Detection Background Service started with interval {IntervalSeconds}s", _intervalSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Run comprehensive fault detection
                await _faultDetectionService.DetectAndRecoverFailuresAsync(stoppingToken);

                await Task.Delay(TimeSpan.FromSeconds(_intervalSeconds), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                // Expected when stoppingToken is triggered during shutdown
                _logger.LogInformation("Fault Detection Background Service cancelled during shutdown");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in Fault Detection Background Service");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        _logger.LogInformation("Fault Detection Background Service stopped");
    }
}
