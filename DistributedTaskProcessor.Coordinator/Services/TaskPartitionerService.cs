using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using DistributedTaskProcessor.Infrastructure.Repositories;
using DistributedTaskProcessor.Infrastructure.Kafka;
using DistributedTaskProcessor.Shared.Models;
using DistributedTaskProcessor.Shared.Configuration;
using TaskStatus = DistributedTaskProcessor.Shared.Models.TaskStatus;

namespace DistributedTaskProcessor.Coordinator.Services;

public class TaskPartitionerService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<TaskPartitionerService> _logger;
    private readonly IKafkaProducerService _kafkaProducer;
    private readonly LeaderElectionService _leaderElection;
    private readonly KafkaSettings _kafkaSettings;
    private readonly SystemSettings _systemSettings;
    private bool _hasInitialized;

    public TaskPartitionerService(
        IServiceProvider serviceProvider,
        ILogger<TaskPartitionerService> logger,
        IKafkaProducerService kafkaProducer,
        LeaderElectionService leaderElection,
        KafkaSettings kafkaSettings,
        SystemSettings systemSettings)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _kafkaProducer = kafkaProducer;
        _leaderElection = leaderElection;
        _kafkaSettings = kafkaSettings;
        _systemSettings = systemSettings;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Task Partitioner Service started");

        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (_leaderElection.IsLeader)
                {
                    if (!_hasInitialized)
                    {
                        await PartitionAndPublishTasksAsync(stoppingToken);
                        _hasInitialized = true;
                    }
                }
                else
                {
                    _hasInitialized = false;
                }

                await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Task Partitioner Service stopping...");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in task partitioner");
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
        }

        // Graceful shutdown: Flush any pending messages
        try
        {
            await _kafkaProducer.FlushAsync(stoppingToken);
            _logger.LogInformation("Task Partitioner flushed all pending messages");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error flushing messages during shutdown");
        }

        _logger.LogInformation("Task Partitioner Service stopped");
    }

    private async Task PartitionAndPublishTasksAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var taskRepository = scope.ServiceProvider.GetRequiredService<ITaskRepository>();
        var sourceRepository = scope.ServiceProvider.GetRequiredService<ISourceDataRepository>();

        _logger.LogInformation("Starting task partitioning from SourceMarketData...");

        var runDate = DateTime.UtcNow.Date;

        // FIX: Get actual row counts by Symbol/Fund from SourceMarketData
        var rowCounts = await sourceRepository.GetRowCountsBySymbolFundAsync(runDate, cancellationToken);

        if (rowCounts.Count == 0)
        {
            _logger.LogWarning("No source data found for RunDate: {RunDate}. No tasks created.", runDate);
            return;
        }

        var tasks = new List<TaskEntity>();
        var rowsPerTask = _systemSettings.RowsPerTask;

        foreach (var entry in rowCounts)
        {
            // FIX: Parse the key correctly (format is "Symbol_Fund")
            var parts = entry.Key.Split('_', 2); // Split only on first underscore

            if (parts.Length < 2)
            {
                _logger.LogWarning("Invalid key format: {Key}", entry.Key);
                continue;
            }

            var symbol = parts[0];
            var fund = parts[1]; // This will now be "Fund_Alpha", "Fund_Beta" etc.
            var totalRows = entry.Value;

            _logger.LogInformation("Processing {Symbol}/{Fund}: {TotalRows} rows", symbol, fund, totalRows);

            for (long startRow = 0; startRow < totalRows; startRow += rowsPerTask)
            {
                var endRow = Math.Min(startRow + rowsPerTask - 1, totalRows - 1);

                var task = new TaskEntity
                {
                    TaskId = Guid.NewGuid(),
                    Symbol = symbol,
                    Fund = fund, // Now correctly assigned
                    RunDate = runDate,
                    StartRow = startRow,
                    EndRow = endRow,
                    Status = TaskStatus.Pending,
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow,
                    RetryCount = 0
                };
                tasks.Add(task);
            }
        }

        // Validation: Ensure we have tasks to process
        if (tasks.Count == 0)
        {
            _logger.LogWarning("No tasks generated from source data for RunDate: {RunDate}", runDate);
            return;
        }

        // Save to database
        await taskRepository.CreateTasksAsync(tasks, cancellationToken);
        _logger.LogInformation("Created {Count} tasks in database", tasks.Count);

        // Publish to Kafka
        var publishedCount = 0;
        foreach (var task in tasks)
        {
            var taskMessage = new TaskMessage
            {
                TaskId = task.TaskId,
                Symbol = task.Symbol,
                Fund = task.Fund,
                RunDate = task.RunDate,
                StartRow = (int)task.StartRow,
                EndRow = (int)task.EndRow,
                RetryCount = 0,
                CreatedAt = task.CreatedAt
            };

            var partitionKey = $"{task.Symbol}_{task.Fund}";

            // Log first task for verification
            if (publishedCount == 0)
            {
                _logger.LogInformation("First task - TaskId: {TaskId}, Symbol: {Symbol}, Fund: {Fund}, Rows: {Start}-{End}",
                    taskMessage.TaskId, taskMessage.Symbol, taskMessage.Fund, taskMessage.StartRow, taskMessage.EndRow);
            }

            await _kafkaProducer.ProduceAsync(
                _kafkaSettings.TaskTopic,
                partitionKey,
                taskMessage,
                cancellationToken);

            publishedCount++;

            if (publishedCount % 100 == 0)
            {
                _logger.LogDebug("Published {Count}/{Total} tasks", publishedCount, tasks.Count);
            }
        }

        await _kafkaProducer.FlushAsync(cancellationToken);
        _logger.LogInformation("✓ Published all {Count} tasks to Kafka", publishedCount);
    }

}
