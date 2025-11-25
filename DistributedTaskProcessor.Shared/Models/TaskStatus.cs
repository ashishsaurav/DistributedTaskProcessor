namespace DistributedTaskProcessor.Shared.Models;

/// <summary>
/// Represents the lifecycle status of a task
/// </summary>
public enum TaskStatus
{
    /// <summary>
    /// Task has been created but not yet assigned
    /// </summary>
    Pending = 0,

    /// <summary>
    /// Task has been assigned to a worker
    /// </summary>
    Assigned = 1,

    /// <summary>
    /// Task is currently being processed
    /// </summary>
    InProgress = 2,

    /// <summary>
    /// Task completed successfully
    /// </summary>
    Completed = 3,

    /// <summary>
    /// Task failed during processing
    /// </summary>
    Failed = 4,

    /// <summary>
    /// Task exceeded max retries and moved to dead letter queue
    /// </summary>
    DeadLetter = 5
}
