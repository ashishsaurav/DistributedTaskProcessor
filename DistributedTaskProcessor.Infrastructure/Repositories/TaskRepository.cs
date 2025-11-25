using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using DistributedTaskProcessor.Shared.Models;
using DistributedTaskProcessor.Infrastructure.Data;
using TaskStatus = DistributedTaskProcessor.Shared.Models.TaskStatus;

namespace DistributedTaskProcessor.Infrastructure.Repositories;

public class TaskRepository : ITaskRepository
{
    private readonly TaskDbContext _context;
    private readonly ILogger<TaskRepository> _logger;

    public TaskRepository(TaskDbContext context, ILogger<TaskRepository> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task<List<TaskEntity>> GetPendingTasksAsync(int limit, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _context.Tasks
                .Where(t => t.Status == TaskStatus.Pending)
                .OrderBy(t => t.CreatedAt)
                .Take(limit)
                .AsNoTracking()
                .ToListAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving pending tasks");
            throw;
        }
    }

    public async Task CreateTasksAsync(List<TaskEntity> tasks, CancellationToken cancellationToken = default)
    {
        try
        {
            await _context.Tasks.AddRangeAsync(tasks, cancellationToken);
            await _context.SaveChangesAsync(cancellationToken);
            _logger.LogInformation("Created {Count} tasks in database", tasks.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating tasks");
            throw;
        }
    }

    public async Task<TaskEntity?> GetTaskByIdAsync(Guid taskId, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _context.Tasks
                .FirstOrDefaultAsync(t => t.TaskId == taskId, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving task {TaskId}", taskId);
            throw;
        }
    }

    public async Task UpdateTaskStatusAsync(
        Guid taskId,
        TaskStatus status,
        string? workerId = null,
        string? errorMessage = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var task = await _context.Tasks.FindAsync(new object[] { taskId }, cancellationToken);
            if (task != null)
            {
                var oldStatus = task.Status;
                task.Status = status;
                task.AssignedWorker = workerId;
                task.UpdatedAt = DateTime.UtcNow;
                task.ErrorMessage = errorMessage;

                await _context.SaveChangesAsync(cancellationToken);

                // Log audit trail
                await LogAuditAsync(taskId, "STATUS_UPDATE", workerId, oldStatus, status, errorMessage, cancellationToken);

                _logger.LogDebug("Updated task {TaskId} status from {OldStatus} to {NewStatus}",
                    taskId, oldStatus, status);
            }
            else
            {
                _logger.LogWarning("Task {TaskId} not found for status update", taskId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating task status for {TaskId}", taskId);
            throw;
        }
    }

    public async Task<List<TaskEntity>> GetStalledTasksAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        try
        {
            var cutoffTime = DateTime.UtcNow - timeout;
            return await _context.Tasks
                .Where(t => (t.Status == TaskStatus.InProgress || t.Status == TaskStatus.Assigned)
                            && t.UpdatedAt < cutoffTime)
                .ToListAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving stalled tasks");
            throw;
        }
    }

    public async Task IncrementRetryCountAsync(Guid taskId, CancellationToken cancellationToken = default)
    {
        try
        {
            var task = await _context.Tasks.FindAsync(new object[] { taskId }, cancellationToken);
            if (task != null)
            {
                task.RetryCount++;
                task.UpdatedAt = DateTime.UtcNow;
                await _context.SaveChangesAsync(cancellationToken);

                _logger.LogDebug("Incremented retry count for task {TaskId} to {RetryCount}",
                    taskId, task.RetryCount);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error incrementing retry count for task {TaskId}", taskId);
            throw;
        }
    }

    public async Task<int> GetPendingTaskCountAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _context.Tasks
                .CountAsync(t => t.Status == TaskStatus.Pending, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting pending task count");
            throw;
        }
    }

    public async Task<int> GetTaskCountByStatusAsync(TaskStatus status, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _context.Tasks
                .CountAsync(t => t.Status == status, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting task count for status {Status}", status);
            throw;
        }
    }

    public async Task LogAuditAsync(
        Guid taskId,
        string action,
        string? performedBy,
        TaskStatus oldStatus,
        TaskStatus newStatus,
        string? details = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var auditLog = new AuditLog
            {
                Id = Guid.NewGuid(),
                TaskId = taskId,
                Action = action,
                PerformedBy = performedBy,
                OldStatus = oldStatus,
                NewStatus = newStatus,
                Details = details,
                Timestamp = DateTime.UtcNow
            };

            await _context.AuditLogs.AddAsync(auditLog, cancellationToken);
            await _context.SaveChangesAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error logging audit for task {TaskId}", taskId);
            // Don't throw - audit logging failures shouldn't break main flow
        }
    }
}
