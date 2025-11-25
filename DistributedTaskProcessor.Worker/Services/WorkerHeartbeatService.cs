using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using DistributedTaskProcessor.Infrastructure.Data;
using DistributedTaskProcessor.Shared.Models;
using DistributedTaskProcessor.Shared.Configuration;

namespace DistributedTaskProcessor.Worker.Services;

public class WorkerHeartbeatService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<WorkerHeartbeatService> _logger;
    private readonly SystemSettings _systemSettings;
    private readonly string _workerId;

    public WorkerHeartbeatService(
        IServiceProvider serviceProvider,
        ILogger<WorkerHeartbeatService> logger,
        SystemSettings systemSettings)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _systemSettings = systemSettings;
        _workerId = $"{Environment.MachineName}_Worker_{Guid.NewGuid().ToString()[..8]}";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Worker Heartbeat Service started for {WorkerId}", _workerId);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _serviceProvider.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TaskDbContext>();

                var heartbeat = await dbContext.WorkerHeartbeats
                    .FirstOrDefaultAsync(w => w.WorkerId == _workerId, stoppingToken);

                if (heartbeat == null)
                {
                    heartbeat = new WorkerHeartbeat
                    {
                        Id = Guid.NewGuid(),
                        WorkerId = _workerId,
                        LastHeartbeat = DateTime.UtcNow,
                        Status = "Active",
                        MachineName = Environment.MachineName,
                        ActiveTasks = 0
                    };
                    await dbContext.WorkerHeartbeats.AddAsync(heartbeat, stoppingToken);
                }
                else
                {
                    heartbeat.LastHeartbeat = DateTime.UtcNow;
                    heartbeat.Status = "Active";
                }

                await dbContext.SaveChangesAsync(stoppingToken);
                _logger.LogDebug("Heartbeat sent for worker {WorkerId}", _workerId);

                await Task.Delay(
                    TimeSpan.FromSeconds(_systemSettings.WorkerHeartbeatIntervalSeconds),
                    stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Worker Heartbeat Service stopping for {WorkerId}...", _workerId);
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error sending worker heartbeat");
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
        }

        // Graceful shutdown: Mark worker as inactive
        try
        {
            using var scope = _serviceProvider.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<TaskDbContext>();

            var heartbeat = await dbContext.WorkerHeartbeats
                .FirstOrDefaultAsync(w => w.WorkerId == _workerId, CancellationToken.None);

            if (heartbeat != null)
            {
                heartbeat.Status = "Inactive";
                heartbeat.LastHeartbeat = DateTime.UtcNow;
                await dbContext.SaveChangesAsync(CancellationToken.None);
                _logger.LogInformation("Worker {WorkerId} marked as inactive", _workerId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error marking worker as inactive during shutdown");
        }

        _logger.LogInformation("Worker Heartbeat Service stopped for {WorkerId}", _workerId);
    }
}
