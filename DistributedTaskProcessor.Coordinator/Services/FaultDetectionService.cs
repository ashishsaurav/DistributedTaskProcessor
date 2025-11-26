using Microsoft.EntityFrameworkCore;
using DistributedTaskProcessor.Infrastructure.Data;
using TaskStatus = DistributedTaskProcessor.Shared.Models.TaskStatus;

namespace DistributedTaskProcessor.Coordinator.Services;

/// <summary>
/// Detects and handles worker failures and task timeouts
/// </summary>
public interface IFaultDetectionService
{
    System.Threading.Tasks.Task DetectAndRecoverFailuresAsync(CancellationToken cancellationToken = default);
}

public class FaultDetectionService : IFaultDetectionService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<FaultDetectionService> _logger;
    private const int HeartbeatTimeoutSeconds = 60;
    private const int ProcessingTimeoutSeconds = 1200; // 20 minutes

    public FaultDetectionService(
        IServiceProvider serviceProvider,
        ILogger<FaultDetectionService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    public async System.Threading.Tasks.Task DetectAndRecoverFailuresAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using (var scope = _serviceProvider.CreateScope())
            {
                var dbContext = scope.ServiceProvider.GetRequiredService<TaskDbContext>();

                // Detect dead workers (no heartbeat)
                await DetectDeadWorkersAsync(dbContext, cancellationToken);

                // Detect stalled tasks (processing too long)
                await DetectStalledTasksAsync(dbContext, cancellationToken);

                // Reassign failed tasks
                await ReassignFailedTasksInternalAsync(dbContext, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in fault detection");
        }
    }

    private async System.Threading.Tasks.Task DetectDeadWorkersAsync(TaskDbContext dbContext, CancellationToken cancellationToken)
    {
        var deadlineTime = DateTime.UtcNow.AddSeconds(-HeartbeatTimeoutSeconds);

        var deadWorkers = await dbContext.WorkerHeartbeats
            .Where(w => w.LastHeartbeat < deadlineTime && w.Status != "Dead")
            .ToListAsync(cancellationToken);

        foreach (var worker in deadWorkers)
        {
            _logger.LogWarning(
                "Worker {WorkerId} detected as DEAD. Last heartbeat: {LastHeartbeat}",
                worker.WorkerId, worker.LastHeartbeat);

            worker.Status = "Dead";
            worker.LastHeartbeat = DateTime.UtcNow;

            // Reassign tasks from this worker
            await ReassignWorkerTasksAsync(dbContext, worker.WorkerId, cancellationToken);
        }

        if (deadWorkers.Count > 0)
        {
            await dbContext.SaveChangesAsync(cancellationToken);
        }
    }

    private async System.Threading.Tasks.Task DetectStalledTasksAsync(TaskDbContext dbContext, CancellationToken cancellationToken)
    {
        var stalledTime = DateTime.UtcNow.AddSeconds(-ProcessingTimeoutSeconds);

        var stalledTasks = await dbContext.Tasks
            .Where(t =>
                t.Status == TaskStatus.InProgress &&
                t.CreatedAt < stalledTime)
            .ToListAsync(cancellationToken);

        foreach (var task in stalledTasks)
        {
            _logger.LogWarning(
                "Task {TaskId} detected as STALLED. Created: {CreatedAt}",
                task.TaskId, task.CreatedAt);

            task.Status = TaskStatus.Failed;
            task.ErrorMessage = "Task timeout - processing took too long";
            task.RetryCount++;
        }

        if (stalledTasks.Count > 0)
        {
            await dbContext.SaveChangesAsync(cancellationToken);
        }
    }

    public async System.Threading.Tasks.Task ReassignFailedTasksAsync(CancellationToken cancellationToken)
    {
        using (var scope = _serviceProvider.CreateScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TaskDbContext>();
            await ReassignFailedTasksInternalAsync(dbContext, cancellationToken);
        }
    }

    private async System.Threading.Tasks.Task ReassignFailedTasksInternalAsync(TaskDbContext dbContext, CancellationToken cancellationToken)
    {
        const int maxRetries = 3;

        var failedTasks = await dbContext.Tasks
            .Where(t =>
                t.Status == TaskStatus.Failed &&
                t.RetryCount < maxRetries)
            .ToListAsync(cancellationToken);

        foreach (var task in failedTasks)
        {
            task.Status = TaskStatus.Pending;
            task.AssignedWorker = null;

            _logger.LogInformation(
                "Reassigning failed task {TaskId}. Retry {RetryCount} of {MaxRetries}",
                task.TaskId, task.RetryCount, maxRetries);
        }

        if (failedTasks.Count > 0)
        {
            await dbContext.SaveChangesAsync(cancellationToken);
            _logger.LogInformation("Reassigned {Count} failed tasks", failedTasks.Count);
        }
    }

    private async System.Threading.Tasks.Task ReassignWorkerTasksAsync(TaskDbContext dbContext, string workerId, CancellationToken cancellationToken)
    {
        var workerTasks = await dbContext.Tasks
            .Where(t => t.AssignedWorker == workerId && t.Status == TaskStatus.InProgress)
            .ToListAsync(cancellationToken);

        foreach (var task in workerTasks)
        {
            task.Status = TaskStatus.Pending;
            task.AssignedWorker = null;
            task.RetryCount++;

            _logger.LogInformation(
                "Reassigning task {TaskId} from dead worker {WorkerId}",
                task.TaskId, workerId);
        }

        if (workerTasks.Count > 0)
        {
            await dbContext.SaveChangesAsync(cancellationToken);
            _logger.LogInformation(
                "Reassigned {Count} tasks from dead worker {WorkerId}",
                workerTasks.Count, workerId);
        }
    }
}
