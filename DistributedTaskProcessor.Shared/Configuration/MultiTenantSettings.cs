namespace DistributedTaskProcessor.Shared.Configuration;

/// <summary>
/// Configuration settings for multi-tenant distributed task processing
/// </summary>
public class MultiTenantSettings
{
    /// <summary>Minimum partition size (rows)</summary>
    public int MinPartitionSize { get; set; } = 5000;

    /// <summary>Maximum partition size (rows)</summary>
    public int MaxPartitionSize { get; set; } = 15000;

    /// <summary>Task allocation strategy: LeastLoaded, HealthFirst, RoundRobin, PerformanceBased</summary>
    public string AllocationStrategy { get; set; } = "HealthFirst";

    /// <summary>Heartbeat check interval in seconds</summary>
    public int HeartbeatIntervalSeconds { get; set; } = 60;

    /// <summary>Worker heartbeat timeout in seconds</summary>
    public int HeartbeatTimeoutSeconds { get; set; } = 120;

    /// <summary>Task processing timeout in seconds</summary>
    public int TaskTimeoutSeconds { get; set; } = 1200; // 20 minutes

    /// <summary>Maximum retry attempts for failed tasks</summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>Result batch size for collector</summary>
    public int ResultBatchSize { get; set; } = 1000;

    /// <summary>Result batch flush interval in milliseconds</summary>
    public int ResultBatchFlushIntervalMs { get; set; } = 5000;

    /// <summary>Enable multi-tenant isolation</summary>
    public bool EnableTenantIsolation { get; set; } = true;

    /// <summary>Enable fault detection and auto-recovery</summary>
    public bool EnableFaultDetection { get; set; } = true;

    /// <summary>Enable health-based task allocation</summary>
    public bool EnableHealthBasedAllocation { get; set; } = true;
}
