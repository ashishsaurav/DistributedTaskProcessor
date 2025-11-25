using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace DistributedTaskProcessor.Shared.Models;

/// <summary>
/// Represents a task in the distributed processing system
/// </summary>
[Table("Tasks")]
public class TaskEntity
{
    [Key]
    public Guid TaskId { get; set; }

    [Required]
    [MaxLength(50)]
    public string Symbol { get; set; } = string.Empty;

    [Required]
    [MaxLength(50)]
    public string Fund { get; set; } = string.Empty;

    public DateTime RunDate { get; set; }

    public long StartRow { get; set; }

    public long EndRow { get; set; }

    public TaskStatus Status { get; set; }

    [MaxLength(100)]
    public string? AssignedWorker { get; set; }

    public int RetryCount { get; set; } = 0;

    public DateTime CreatedAt { get; set; }

    public DateTime UpdatedAt { get; set; }

    [MaxLength(500)]
    public string? ErrorMessage { get; set; }
}

/// <summary>
/// Tracks worker heartbeats for health monitoring
/// </summary>
[Table("WorkerHeartbeats")]
public class WorkerHeartbeat
{
    [Key]
    public Guid Id { get; set; }

    [Required]
    [MaxLength(100)]
    public string WorkerId { get; set; } = string.Empty;

    public DateTime LastHeartbeat { get; set; }

    [Required]
    [MaxLength(20)]
    public string Status { get; set; } = "Active";

    public int ActiveTasks { get; set; } = 0;

    [MaxLength(50)]
    public string MachineName { get; set; } = string.Empty;
}

/// <summary>
/// Manages coordinator state for leader election
/// </summary>
[Table("CoordinatorStates")]
public class CoordinatorState
{
    [Key]
    public Guid Id { get; set; }

    [Required]
    [MaxLength(100)]
    public string CoordinatorId { get; set; } = string.Empty;

    public bool IsLeader { get; set; } = false;

    public DateTime LastHeartbeat { get; set; }

    [MaxLength(50)]
    public string MachineName { get; set; } = string.Empty;
}

/// <summary>
/// Audit trail for all task state transitions
/// </summary>
[Table("AuditLogs")]
public class AuditLog
{
    [Key]
    public Guid Id { get; set; }

    public Guid TaskId { get; set; }

    [Required]
    [MaxLength(50)]
    public string Action { get; set; } = string.Empty;

    [MaxLength(100)]
    public string? PerformedBy { get; set; }

    public TaskStatus OldStatus { get; set; }

    public TaskStatus NewStatus { get; set; }

    [MaxLength(1000)]
    public string? Details { get; set; }

    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}
