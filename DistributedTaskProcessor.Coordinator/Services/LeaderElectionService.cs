using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using DistributedTaskProcessor.Infrastructure.Data;
using DistributedTaskProcessor.Shared.Models;
using DistributedTaskProcessor.Shared.Configuration;

namespace DistributedTaskProcessor.Coordinator.Services;

public class LeaderElectionService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<LeaderElectionService> _logger;
    private readonly SystemSettings _systemSettings;
    private readonly string _coordinatorId;
    private bool _isLeader;
    private readonly SemaphoreSlim _leaderLock = new(1, 1);

    public LeaderElectionService(
        IServiceProvider serviceProvider,
        ILogger<LeaderElectionService> logger,
        SystemSettings systemSettings)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _systemSettings = systemSettings;
        _coordinatorId = $"{Environment.MachineName}_{Guid.NewGuid().ToString()[..8]}";
    }

    public bool IsLeader => _isLeader;
    public string CoordinatorId => _coordinatorId;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Leader Election Service started for Coordinator: {CoordinatorId}", _coordinatorId);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _serviceProvider.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TaskDbContext>();

                await TryBecomeLeaderAsync(dbContext, stoppingToken);
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Leader Election Service stopping...");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in leader election");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        // Graceful shutdown: Step down as leader
        try
        {
            await StepDownAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during graceful shutdown");
        }

        _logger.LogInformation("Leader Election Service stopped");
    }

    private async Task TryBecomeLeaderAsync(TaskDbContext dbContext, CancellationToken cancellationToken)
    {
        await _leaderLock.WaitAsync(cancellationToken);
        try
        {
            var cutoffTime = DateTime.UtcNow.AddSeconds(-_systemSettings.LeaderTimeoutSeconds);

            // Find active leader
            var currentLeader = await dbContext.CoordinatorStates
                .FirstOrDefaultAsync(c => c.IsLeader && c.LastHeartbeat > cutoffTime, cancellationToken);

            if (currentLeader == null)
            {
                // No active leader - attempt to become leader
                var myState = await dbContext.CoordinatorStates
                    .FirstOrDefaultAsync(c => c.CoordinatorId == _coordinatorId, cancellationToken);

                if (myState == null)
                {
                    myState = new CoordinatorState
                    {
                        Id = Guid.NewGuid(),
                        CoordinatorId = _coordinatorId,
                        IsLeader = true,
                        LastHeartbeat = DateTime.UtcNow,
                        MachineName = Environment.MachineName
                    };
                    await dbContext.CoordinatorStates.AddAsync(myState, cancellationToken);
                }
                else
                {
                    myState.IsLeader = true;
                    myState.LastHeartbeat = DateTime.UtcNow;
                }

                await dbContext.SaveChangesAsync(cancellationToken);

                if (!_isLeader)
                {
                    _isLeader = true;
                    _logger.LogInformation("✓ Became LEADER: {CoordinatorId}", _coordinatorId);
                }
            }
            else if (currentLeader.CoordinatorId == _coordinatorId)
            {
                // Update heartbeat
                currentLeader.LastHeartbeat = DateTime.UtcNow;
                await dbContext.SaveChangesAsync(cancellationToken);
                _isLeader = true;
            }
            else
            {
                // Another coordinator is leader
                if (_isLeader)
                {
                    _isLeader = false;
                    _logger.LogInformation("Stepped down as leader. Current leader: {LeaderId}",
                        currentLeader.CoordinatorId);
                }
            }
        }
        finally
        {
            _leaderLock.Release();
        }
    }

    private async Task StepDownAsync()
    {
        if (!_isLeader) return;

        try
        {
            using var scope = _serviceProvider.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<TaskDbContext>();

            var myState = await dbContext.CoordinatorStates
                .FirstOrDefaultAsync(c => c.CoordinatorId == _coordinatorId);

            if (myState != null)
            {
                myState.IsLeader = false;
                await dbContext.SaveChangesAsync();
                _logger.LogInformation("Gracefully stepped down as leader");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error stepping down as leader");
        }
    }

    public override void Dispose()
    {
        _leaderLock?.Dispose();
        base.Dispose();
    }
}
