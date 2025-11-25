using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using DistributedTaskProcessor.Infrastructure.Repositories;
using DistributedTaskProcessor.Infrastructure.Kafka;
using DistributedTaskProcessor.Shared.Models;
using DistributedTaskProcessor.Shared.Configuration;
using TaskStatus = DistributedTaskProcessor.Shared.Models.TaskStatus;

namespace DistributedTaskProcessor.Coordinator.Services;

public class FailureDetectionService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<FailureDetectionService> _logger;
    private readonly IKafkaProducerService _kafkaProducer;
    private readonly KafkaSettings _kafkaSettings;
    private readonly SystemSettings _systemSettings;

    public FailureDetectionService(
        IServiceProvider serviceProvider,
        ILogger<FailureDetectionService> logger,
        IKafkaProducerService kafkaProducer,
        KafkaSettings kafkaSettings,
        SystemSettings systemSettings)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _kafkaProducer = kafkaProducer;
        _kafkaSettings = kafkaSettings;
        _systemSettings = systemSettings;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Failure Detection Service started");

        // Wait before starting checks
        await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await DetectAndRecoverStalledTasksAsync(stoppingToken);
                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Failure Detection Service stopping...");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in failure detection");
                await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
            }
        }

        // Graceful shutdown: Flush pending messages
        try
        {
            await _kafkaProducer.FlushAsync(stoppingToken);
            _logger.LogInformation("Failure Detection Service flushed all pending messages");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error flushing messages during shutdown");
        }

        _logger.LogInformation("Failure Detection Service stopped");
    }

    private async Task DetectAndRecoverStalledTasksAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<ITaskRepository>();

        var timeout = TimeSpan.FromMinutes(_systemSettings.StalledTaskTimeoutMinutes);
        var stalledTasks = await repository.GetStalledTasksAsync(timeout, cancellationToken);

        if (stalledTasks.Count == 0)
        {
            return;
        }

        _logger.LogWarning("Detected {Count} stalled tasks", stalledTasks.Count);

        foreach (var task in stalledTasks)
        {
            try
            {
                await repository.IncrementRetryCountAsync(task.TaskId, cancellationToken);

                if (task.RetryCount >= _kafkaSettings.MaxRetries)
                {
                    _logger.LogError("Task {TaskId} exceeded max retries ({MaxRetries}), moving to DLQ",
                        task.TaskId, _kafkaSettings.MaxRetries);

                    await repository.UpdateTaskStatusAsync(
                        task.TaskId,
                        TaskStatus.DeadLetter,
                        null,
                        $"Exceeded max retries: {_kafkaSettings.MaxRetries}",
                        cancellationToken);

                    // Publish to Dead Letter Queue
                    var dlqMessage = new TaskMessage
                    {
                        TaskId = task.TaskId,
                        Symbol = task.Symbol,
                        Fund = task.Fund,
                        RunDate = task.RunDate,
                        StartRow = (int)task.StartRow,
                        EndRow = (int)task.EndRow,
                        RetryCount = task.RetryCount + 1
                    };

                    await _kafkaProducer.ProduceAsync(
                        _kafkaSettings.DeadLetterTopic,
                        task.TaskId.ToString(),
                        dlqMessage,
                        cancellationToken);
                }
                else
                {
                    // Calculate exponential backoff delay: 2^(retryCount-1) seconds, max 60 seconds
                    var backoffSeconds = Math.Min(Math.Pow(2, task.RetryCount), 60);
                    var backoffDelay = TimeSpan.FromSeconds(backoffSeconds);

                    _logger.LogInformation("Applying exponential backoff ({BackoffSeconds}s) for stalled task {TaskId}, retry attempt {RetryCount}",
                        backoffSeconds, task.TaskId, task.RetryCount + 1);

                    // Reassign task
                    await repository.UpdateTaskStatusAsync(
                        task.TaskId,
                        TaskStatus.Pending,
                        null,
                        $"Reassigned after stall detection with exponential backoff {backoffDelay.TotalSeconds}s (retry {task.RetryCount + 1})",
                        cancellationToken);

                    // Wait before republishing to apply backoff
                    await Task.Delay(backoffDelay, cancellationToken);

                    var retryMessage = new TaskMessage
                    {
                        TaskId = task.TaskId,
                        Symbol = task.Symbol,
                        Fund = task.Fund,
                        RunDate = task.RunDate,
                        StartRow = (int)task.StartRow,
                        EndRow = (int)task.EndRow,
                        RetryCount = task.RetryCount + 1
                    };

                    var partitionKey = $"{task.Symbol}_{task.Fund}";
                    await _kafkaProducer.ProduceAsync(
                        _kafkaSettings.TaskTopic,
                        partitionKey,
                        retryMessage,
                        cancellationToken);

                    _logger.LogInformation("Reassigned stalled task {TaskId}, retry attempt {RetryCount}",
                        task.TaskId, task.RetryCount + 1);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error recovering stalled task {TaskId}", task.TaskId);
            }
        }

        await _kafkaProducer.FlushAsync(cancellationToken);
    }
}
