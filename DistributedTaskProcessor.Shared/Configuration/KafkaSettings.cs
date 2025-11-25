namespace DistributedTaskProcessor.Shared.Configuration;

public class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:5081";
    public string TaskTopic { get; set; } = "task-queue";
    public string ResultTopic { get; set; } = "result-queue";
    public string DeadLetterTopic { get; set; } = "dead-letter-queue";
    public string WorkerGroupId { get; set; } = "worker-group";
    public string CollectorGroupId { get; set; } = "collector-group";
    public int MaxRetries { get; set; } = 3;
    public int SessionTimeoutMs { get; set; } = 45000;
    public int HeartbeatIntervalMs { get; set; } = 15000;
}
