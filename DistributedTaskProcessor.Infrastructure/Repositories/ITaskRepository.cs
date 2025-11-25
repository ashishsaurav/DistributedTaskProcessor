using DistributedTaskProcessor.Shared.Models;
using TaskStatus = DistributedTaskProcessor.Shared.Models.TaskStatus;

namespace DistributedTaskProcessor.Infrastructure.Repositories;

public interface ITaskRepository
{
    // Task CRUD operations
    Task<List<TaskEntity>> GetPendingTasksAsync(int limit, CancellationToken cancellationToken = default);
    Task CreateTasksAsync(List<TaskEntity> tasks, CancellationToken cancellationToken = default);
    Task<TaskEntity?> GetTaskByIdAsync(Guid taskId, CancellationToken cancellationToken = default);

    // Task status management
    Task UpdateTaskStatusAsync(Guid taskId, TaskStatus status, string? workerId = null, string? errorMessage = null, CancellationToken cancellationToken = default);

    // Failure detection and recovery
    Task<List<TaskEntity>> GetStalledTasksAsync(TimeSpan timeout, CancellationToken cancellationToken = default);
    Task IncrementRetryCountAsync(Guid taskId, CancellationToken cancellationToken = default);

    // Monitoring and statistics
    Task<int> GetPendingTaskCountAsync(CancellationToken cancellationToken = default);
    Task<int> GetTaskCountByStatusAsync(TaskStatus status, CancellationToken cancellationToken = default);

    // Audit logging
    Task LogAuditAsync(Guid taskId, string action, string? performedBy, TaskStatus oldStatus, TaskStatus newStatus, string? details = null, CancellationToken cancellationToken = default);
}
