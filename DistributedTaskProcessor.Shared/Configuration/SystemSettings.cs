namespace DistributedTaskProcessor.Shared.Configuration;

public class SystemSettings
{
    public int RowsPerTask { get; set; } = 1000;
    public int StalledTaskTimeoutMinutes { get; set; } = 5;
    public int WorkerHeartbeatIntervalSeconds { get; set; } = 30;
    public int CoordinatorHeartbeatIntervalSeconds { get; set; } = 10;
}
