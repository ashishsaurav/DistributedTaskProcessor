namespace DistributedTaskProcessor.Shared.Monitoring;

/// <summary>
/// Interface for metrics collection and monitoring
/// </summary>
public interface IMetricsService
{
    void RecordTaskStarted(Guid taskId, string workerId);
    void RecordTaskCompleted(Guid taskId, string workerId, TimeSpan duration, int rowsProcessed);
    void RecordTaskFailed(Guid taskId, string workerId, string errorType);
    void RecordWorkerStartup(string workerId);
    void RecordWorkerShutdown(string workerId);
    void RecordMessageProduced(string topic, long latencyMs);
    void RecordMessageConsumed(string topic, long latencyMs);
    void RecordConsumerLag(string topic, int partition, long lag);
    void UpdateWorkerLoad(string workerId, int activeTasks);

    WorkerMetrics GetWorkerMetrics(string workerId);
    Dictionary<string, WorkerMetrics> GetAllWorkerMetrics();
    MetricsSnapshot GetSnapshot();
}

/// <summary>
/// Supporting types used by the metrics service
/// </summary>
public class WorkerMetrics
{
    public string WorkerId { get; set; } = string.Empty;
    public DateTime? StartupTime { get; set; }
    public DateTime? ShutdownTime { get; set; }
    public DateTime LastHeartbeat { get; set; } = DateTime.UtcNow;

    public long TasksStarted;
    public long TasksCompleted;
    public long TasksFailed;
    public long ActiveTasks;
    public long TotalRowsProcessed;

    public List<double> ProcessingTimes { get; } = new();

    public bool IsActive() => (DateTime.UtcNow - LastHeartbeat).TotalSeconds < 60;

    public double AverageProcessingTimeMs => ProcessingTimes.Any() ? ProcessingTimes.Average() : 0;
    public double SuccessRate => TasksStarted > 0 ? (double)TasksCompleted / TasksStarted * 100 : 0;
}

public class MetricsSnapshot
{
    public DateTime Timestamp { get; set; }

    // Task metrics
    public long TotalTasksStarted { get; set; }
    public long TotalTasksCompleted { get; set; }
    public long TotalTasksFailed { get; set; }
    public int TasksInProgress { get; set; }
    public long TotalRowsProcessed { get; set; }

    // Worker metrics
    public int TotalWorkers { get; set; }
    public int ActiveWorkers { get; set; }
    public int IdleWorkers { get; set; }

    // Performance
    public double AverageTaskDurationMs { get; set; }
    public double TasksPerSecond { get; set; }
    public double RowsPerSecond { get; set; }

    // Errors
    public Dictionary<string, long> ErrorBreakdown { get; set; } = new();

    // Kafka
    public List<KafkaTopicSnapshot> KafkaTopics { get; set; } = new();

    // Events
    public List<MetricEvent> RecentEvents { get; set; } = new();
}

public class KafkaTopicSnapshot
{
    public string Topic { get; set; } = string.Empty;
    public long MessagesProduced { get; set; }
    public long MessagesConsumed { get; set; }
    public double AverageProduceLatencyMs { get; set; }
    public double AverageConsumeLatencyMs { get; set; }
    public long TotalConsumerLag { get; set; }
}

public class MetricEvent
{
    public string EventType { get; set; } = string.Empty;
    public string? WorkerId { get; set; }
    public Guid? TaskId { get; set; }
    public TimeSpan? Duration { get; set; }
    public int? RowsProcessed { get; set; }
    public string? ErrorType { get; set; }
    public DateTime Timestamp { get; set; }
}



