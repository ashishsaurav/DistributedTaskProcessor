namespace DistributedTaskProcessor.Shared.Models;

public class TaskMessage
{
    public Guid TaskId { get; set; } = Guid.Empty;
    public string Symbol { get; set; } = string.Empty;
    public DateTime RunDate { get; set; } = DateTime.MinValue;
    public string Fund { get; set; } = string.Empty;
    public int StartRow { get; set; } = 0;
    public int EndRow { get; set; } = 0;
    public int RetryCount { get; set; } = 0;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
