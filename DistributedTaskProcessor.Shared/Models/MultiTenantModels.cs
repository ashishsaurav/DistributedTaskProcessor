namespace DistributedTaskProcessor.Shared.Models;

/// <summary>
/// Represents a tenant in the multi-tenant system
/// </summary>
public class Tenant
{
    public Guid TenantId { get; set; }
    public string TenantName { get; set; } = string.Empty;
    public string Status { get; set; } = "Active"; // Active, Suspended
    public int MaxWorkers { get; set; } = 10;
    public int MaxPartitionSize { get; set; } = 10000;
    public int MaxConcurrentTasks { get; set; } = 50;
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
}

/// <summary>
/// Represents raw source data to be processed
/// </summary>
public class SourceData
{
    public Guid SourceDataId { get; set; }
    public Guid TenantId { get; set; }
    public string? Fund { get; set; }
    public string? Symbol { get; set; }
    public DateTime RunDate { get; set; }
    public long TotalRows { get; set; }
    public long ProcessedRows { get; set; }
    public string Status { get; set; } = "Pending";
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// Represents a task partition for worker processing
/// </summary>
public class TaskPartition
{
    public Guid PartitionId { get; set; }
    public Guid SourceDataId { get; set; }
    public Guid TenantId { get; set; }
    public long StartRow { get; set; }
    public long EndRow { get; set; }
    public string? AssignedWorker { get; set; }
    public string Status { get; set; } = "Pending";
    public int RetryCount { get; set; } = 0;
    public DateTime? ProcessingStartTime { get; set; }
    public DateTime? ProcessingEndTime { get; set; }
    public string? ErrorMessage { get; set; }
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// Tenant-aware task message for Kafka
/// </summary>
public class TenantTaskMessage
{
    public Guid TaskId { get; set; }
    public Guid TenantId { get; set; }
    public Guid SourceDataId { get; set; }
    public long StartRow { get; set; }
    public long EndRow { get; set; }
    public string Fund { get; set; } = string.Empty;
    public string Symbol { get; set; } = string.Empty;
    public DateTime RunDate { get; set; }
    public int RetryCount { get; set; }
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// Tenant-aware result message from workers
/// </summary>
public class TenantResultMessage
{
    public Guid ResultId { get; set; }
    public Guid TaskId { get; set; }
    public Guid TenantId { get; set; }
    public decimal CalculatedValue { get; set; }
    public int RowsProcessed { get; set; }
    public int ProcessingTimeMs { get; set; }
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public DateTime Timestamp { get; set; }
}

/// <summary>
/// Worker health status with tenant context
/// </summary>
public class WorkerHealthStatus
{
    public string WorkerId { get; set; } = string.Empty;
    public string MachineName { get; set; } = string.Empty;
    public string Status { get; set; } = "Active"; // Active, Idle, Failed, Recovering
    public int HealthScore { get; set; } = 100; // 0-100
    public int ActiveTasks { get; set; }
    public int MaxConcurrentTasks { get; set; } = 5;
    public decimal CpuUsage { get; set; } // Percentage
    public decimal MemoryUsage { get; set; } // Percentage
    public decimal ErrorRate { get; set; } // Percentage
    public long TotalTasksCompleted { get; set; }
    public int TotalTasksFailed { get; set; }
    public DateTime LastHeartbeat { get; set; }
    public List<Guid> CurrentTenants { get; set; } = new();
}

/// <summary>
/// Task allocation strategy
/// </summary>
public enum TaskAllocationStrategy
{
    /// <summary>Assign to least loaded worker</summary>
    LeastLoaded,

    /// <summary>Assign to healthiest worker</summary>
    HealthFirst,

    /// <summary>Round-robin across workers</summary>
    RoundRobin,

    /// <summary>Assign based on historical performance</summary>
    PerformanceBased
}
