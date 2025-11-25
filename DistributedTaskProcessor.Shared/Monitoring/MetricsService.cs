using System.Collections.Concurrent;

namespace DistributedTaskProcessor.Shared.Monitoring;

/// <summary>
/// Centralized metrics collection service for monitoring system performance
/// </summary>
public class MetricsService : IMetricsService
{
    private readonly ConcurrentDictionary<string, WorkerMetrics> _workerMetrics = new();
    private readonly ConcurrentDictionary<Guid, TaskExecutionMetrics> _taskMetrics = new();
    private readonly ConcurrentQueue<MetricEvent> _recentEvents = new();
    private readonly object _lock = new();

    // Aggregate metrics
    private long _totalTasksStarted;
    private long _totalTasksCompleted;
    private long _totalTasksFailed;
    private long _totalRowsProcessed;
    private readonly ConcurrentDictionary<string, long> _errorTypeCount = new();

    // Kafka metrics
    private readonly ConcurrentDictionary<string, KafkaTopicMetrics> _kafkaMetrics = new();

    private const int MaxRecentEvents = 1000;

    public void RecordTaskStarted(Guid taskId, string workerId)
    {
        Interlocked.Increment(ref _totalTasksStarted);

        _taskMetrics[taskId] = new TaskExecutionMetrics
        {
            TaskId = taskId,
            WorkerId = workerId,
            StartTime = DateTime.UtcNow
        };

        var workerMetric = _workerMetrics.GetOrAdd(workerId, _ => new WorkerMetrics { WorkerId = workerId });
        Interlocked.Increment(ref workerMetric.TasksStarted);
        Interlocked.Increment(ref workerMetric.ActiveTasks);

        AddEvent(new MetricEvent
        {
            EventType = "TaskStarted",
            WorkerId = workerId,
            TaskId = taskId,
            Timestamp = DateTime.UtcNow
        });
    }

    public void RecordTaskCompleted(Guid taskId, string workerId, TimeSpan duration, int rowsProcessed)
    {
        Interlocked.Increment(ref _totalTasksCompleted);
        Interlocked.Add(ref _totalRowsProcessed, rowsProcessed);

        if (_taskMetrics.TryRemove(taskId, out var metrics))
        {
            metrics.EndTime = DateTime.UtcNow;
            metrics.Duration = duration;
            metrics.RowsProcessed = rowsProcessed;
            metrics.Success = true;
        }

        if (_workerMetrics.TryGetValue(workerId, out var workerMetric))
        {
            Interlocked.Increment(ref workerMetric.TasksCompleted);
            Interlocked.Decrement(ref workerMetric.ActiveTasks);

            lock (workerMetric.ProcessingTimes)
            {
                workerMetric.ProcessingTimes.Add(duration.TotalMilliseconds);
                if (workerMetric.ProcessingTimes.Count > 100)
                {
                    workerMetric.ProcessingTimes.RemoveAt(0);
                }
            }

            Interlocked.Add(ref workerMetric.TotalRowsProcessed, rowsProcessed);
        }

        AddEvent(new MetricEvent
        {
            EventType = "TaskCompleted",
            WorkerId = workerId,
            TaskId = taskId,
            Duration = duration,
            RowsProcessed = rowsProcessed,
            Timestamp = DateTime.UtcNow
        });
    }

    public void RecordTaskFailed(Guid taskId, string workerId, string errorType)
    {
        Interlocked.Increment(ref _totalTasksFailed);
        _errorTypeCount.AddOrUpdate(errorType, 1, (_, count) => count + 1);

        if (_taskMetrics.TryRemove(taskId, out var metrics))
        {
            metrics.EndTime = DateTime.UtcNow;
            metrics.Success = false;
            metrics.ErrorType = errorType;
        }

        if (_workerMetrics.TryGetValue(workerId, out var workerMetric))
        {
            Interlocked.Increment(ref workerMetric.TasksFailed);
            Interlocked.Decrement(ref workerMetric.ActiveTasks);
        }

        AddEvent(new MetricEvent
        {
            EventType = "TaskFailed",
            WorkerId = workerId,
            TaskId = taskId,
            ErrorType = errorType,
            Timestamp = DateTime.UtcNow
        });
    }

    public void RecordWorkerStartup(string workerId)
    {
        var workerMetric = _workerMetrics.GetOrAdd(workerId, _ => new WorkerMetrics
        {
            WorkerId = workerId,
            StartupTime = DateTime.UtcNow
        });

        AddEvent(new MetricEvent
        {
            EventType = "WorkerStartup",
            WorkerId = workerId,
            Timestamp = DateTime.UtcNow
        });
    }

    public void RecordWorkerShutdown(string workerId)
    {
        if (_workerMetrics.TryGetValue(workerId, out var workerMetric))
        {
            workerMetric.ShutdownTime = DateTime.UtcNow;
        }

        AddEvent(new MetricEvent
        {
            EventType = "WorkerShutdown",
            WorkerId = workerId,
            Timestamp = DateTime.UtcNow
        });
    }

    public void UpdateWorkerLoad(string workerId, int activeTasks)
    {
        if (_workerMetrics.TryGetValue(workerId, out var workerMetric))
        {
            Interlocked.Exchange(ref workerMetric.ActiveTasks, activeTasks);
            workerMetric.LastHeartbeat = DateTime.UtcNow;
        }
    }

    public void RecordMessageProduced(string topic, long latencyMs)
    {
        var kafkaMetric = _kafkaMetrics.GetOrAdd(topic, _ => new KafkaTopicMetrics { Topic = topic });
        Interlocked.Increment(ref kafkaMetric.MessagesProduced);
        Interlocked.Add(ref kafkaMetric.TotalProduceLatencyMs, latencyMs);
    }

    public void RecordMessageConsumed(string topic, long latencyMs)
    {
        var kafkaMetric = _kafkaMetrics.GetOrAdd(topic, _ => new KafkaTopicMetrics { Topic = topic });
        Interlocked.Increment(ref kafkaMetric.MessagesConsumed);
        Interlocked.Add(ref kafkaMetric.TotalConsumeLatencyMs, latencyMs);
    }

    public void RecordConsumerLag(string topic, int partition, long lag)
    {
        var kafkaMetric = _kafkaMetrics.GetOrAdd(topic, _ => new KafkaTopicMetrics { Topic = topic });
        kafkaMetric.ConsumerLag[partition] = lag;
    }

    public MetricsSnapshot GetSnapshot()
    {
        var workers = _workerMetrics.Values.ToList();
        var activeWorkers = workers.Count(w => w.IsActive());

        return new MetricsSnapshot
        {
            Timestamp = DateTime.UtcNow,

            // Task metrics
            TotalTasksStarted = Interlocked.Read(ref _totalTasksStarted),
            TotalTasksCompleted = Interlocked.Read(ref _totalTasksCompleted),
            TotalTasksFailed = Interlocked.Read(ref _totalTasksFailed),
            TasksInProgress = _taskMetrics.Count,
            TotalRowsProcessed = Interlocked.Read(ref _totalRowsProcessed),

            // Worker metrics
            TotalWorkers = workers.Count,
            ActiveWorkers = activeWorkers,
            IdleWorkers = workers.Count - activeWorkers,

            // Performance
            AverageTaskDurationMs = CalculateAverageTaskDuration(workers),
            TasksPerSecond = CalculateTaskThroughput(workers),
            RowsPerSecond = CalculateRowThroughput(workers),

            // Error metrics
            ErrorBreakdown = _errorTypeCount.ToDictionary(kvp => kvp.Key, kvp => kvp.Value),

            // Kafka metrics
            KafkaTopics = _kafkaMetrics.Values.Select(km => new KafkaTopicSnapshot
            {
                Topic = km.Topic,
                MessagesProduced = Interlocked.Read(ref km.MessagesProduced),
                MessagesConsumed = Interlocked.Read(ref km.MessagesConsumed),
                AverageProduceLatencyMs = km.MessagesProduced > 0
                    ? (double)km.TotalProduceLatencyMs / km.MessagesProduced
                    : 0,
                AverageConsumeLatencyMs = km.MessagesConsumed > 0
                    ? (double)km.TotalConsumeLatencyMs / km.MessagesConsumed
                    : 0,
                TotalConsumerLag = km.ConsumerLag.Values.Sum()
            }).ToList(),

            // Recent events
            RecentEvents = GetRecentEvents(50)
        };
    }

    public WorkerMetrics GetWorkerMetrics(string workerId)
    {
        return _workerMetrics.TryGetValue(workerId, out var metrics)
            ? metrics
            : new WorkerMetrics { WorkerId = workerId };
    }

    public Dictionary<string, WorkerMetrics> GetAllWorkerMetrics()
    {
        return _workerMetrics.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
    }

    private void AddEvent(MetricEvent evt)
    {
        _recentEvents.Enqueue(evt);

        while (_recentEvents.Count > MaxRecentEvents)
        {
            _recentEvents.TryDequeue(out _);
        }
    }

    private List<MetricEvent> GetRecentEvents(int count)
    {
        return _recentEvents.ToList().TakeLast(count).ToList();
    }

    private double CalculateAverageTaskDuration(List<WorkerMetrics> workers)
    {
        var allDurations = workers.SelectMany(w => w.ProcessingTimes).ToList();
        return allDurations.Any() ? allDurations.Average() : 0;
    }

    private double CalculateTaskThroughput(List<WorkerMetrics> workers)
    {
        var totalCompleted = workers.Sum(w => w.TasksCompleted);
        var oldestWorker = workers.Where(w => w.StartupTime.HasValue)
                                  .OrderBy(w => w.StartupTime)
                                  .FirstOrDefault();

        if (oldestWorker?.StartupTime == null) return 0;

        var elapsedSeconds = (DateTime.UtcNow - oldestWorker.StartupTime.Value).TotalSeconds;
        return elapsedSeconds > 0 ? totalCompleted / elapsedSeconds : 0;
    }

    private double CalculateRowThroughput(List<WorkerMetrics> workers)
    {
        var totalRows = workers.Sum(w => w.TotalRowsProcessed);
        var oldestWorker = workers.Where(w => w.StartupTime.HasValue)
                                  .OrderBy(w => w.StartupTime)
                                  .FirstOrDefault();

        if (oldestWorker?.StartupTime == null) return 0;

        var elapsedSeconds = (DateTime.UtcNow - oldestWorker.StartupTime.Value).TotalSeconds;
        return elapsedSeconds > 0 ? totalRows / elapsedSeconds : 0;
    }
}

public class KafkaTopicMetrics
{
    public string Topic { get; set; } = string.Empty;
    public long MessagesProduced;
    public long MessagesConsumed;
    public long TotalProduceLatencyMs;
    public long TotalConsumeLatencyMs;
    public ConcurrentDictionary<int, long> ConsumerLag { get; } = new();
}

public class TaskExecutionMetrics
{
    public Guid TaskId { get; set; }
    public string WorkerId { get; set; } = string.Empty;
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public TimeSpan Duration { get; set; }
    public int RowsProcessed { get; set; }
    public bool Success { get; set; }
    public string? ErrorType { get; set; }
}
