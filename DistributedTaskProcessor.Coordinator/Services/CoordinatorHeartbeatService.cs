using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using DistributedTaskProcessor.Infrastructure.Data;

namespace DistributedTaskProcessor.Coordinator.Services;

public class CoordinatorHeartbeatService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<CoordinatorHeartbeatService> _logger;
    private readonly LeaderElectionService _leaderElection;

    public CoordinatorHeartbeatService(
        IServiceProvider serviceProvider,
        ILogger<CoordinatorHeartbeatService> logger,
        LeaderElectionService leaderElection)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _leaderElection = leaderElection;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Coordinator Heartbeat Service started");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _serviceProvider.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TaskDbContext>();

                var coordinator = await dbContext.CoordinatorStates
                    .FirstOrDefaultAsync(c => c.CoordinatorId == _leaderElection.CoordinatorId, stoppingToken);

                if (coordinator != null)
                {
                    coordinator.LastHeartbeat = DateTime.UtcNow;
                    await dbContext.SaveChangesAsync(stoppingToken);

                    _logger.LogDebug("Heartbeat sent for Coordinator {CoordinatorId} (IsLeader: {IsLeader})",
                        _leaderElection.CoordinatorId, _leaderElection.IsLeader);
                }

                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error sending coordinator heartbeat");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }
}
