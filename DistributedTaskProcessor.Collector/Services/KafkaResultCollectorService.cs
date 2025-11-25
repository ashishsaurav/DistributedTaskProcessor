using Confluent.Kafka;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using DistributedTaskProcessor.Shared.Models;
using DistributedTaskProcessor.Shared.Configuration;
using DistributedTaskProcessor.Shared.Monitoring;
using DistributedTaskProcessor.Infrastructure.Repositories;
using TaskStatus = DistributedTaskProcessor.Shared.Models.TaskStatus;

namespace DistributedTaskProcessor.Collector.Services;

public class KafkaResultCollectorService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<KafkaResultCollectorService> _logger;
    private readonly IMetricsService _metricsService;
    private readonly KafkaSettings _kafkaSettings;
    private readonly string _collectorId;
    private IConsumer<string, string>? _consumer;

    public KafkaResultCollectorService(
        IServiceProvider serviceProvider,
        ILogger<KafkaResultCollectorService> logger,
        IMetricsService metricsService,
        KafkaSettings kafkaSettings)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _metricsService = metricsService;
        _kafkaSettings = kafkaSettings;
        _collectorId = $"{Environment.MachineName}_Collector_{Guid.NewGuid().ToString()[..8]}";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaSettings.BootstrapServers,
            GroupId = _kafkaSettings.CollectorGroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            SessionTimeoutMs = _kafkaSettings.SessionTimeoutMs,
            HeartbeatIntervalMs = _kafkaSettings.HeartbeatIntervalMs,
            MaxPollIntervalMs = 300000,
            IsolationLevel = IsolationLevel.ReadCommitted
        };

        _consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
                _logger.LogError("Kafka Collector Error: {Reason}", error.Reason))
            .SetPartitionsAssignedHandler((c, partitions) =>
                _logger.LogInformation("Collector {CollectorId} assigned partitions: {Partitions}",
                    _collectorId, string.Join(", ", partitions.Select(p => p.Partition.Value))))
            .Build();

        _consumer.Subscribe(_kafkaSettings.ResultTopic);
        _logger.LogInformation("Collector {CollectorId} started consuming from topic: {Topic}",
            _collectorId, _kafkaSettings.ResultTopic);

        _metricsService.RecordWorkerStartup(_collectorId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(stoppingToken);
                    if (consumeResult?.Message != null)
                    {
                        await ProcessResultAsync(consumeResult, stoppingToken);
                    }
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Error consuming result");
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Collector {CollectorId} shutting down gracefully...", _collectorId);
        }
        finally
        {
            _consumer.Close();
            _consumer.Dispose();
            _metricsService.RecordWorkerShutdown(_collectorId);
            _logger.LogInformation("Collector {CollectorId} stopped", _collectorId);
        }
    }

    private async Task ProcessResultAsync(ConsumeResult<string, string> consumeResult, CancellationToken cancellationToken)
    {
        ProcessedTaskResult? resultMessage = null;

        try
        {
            resultMessage = JsonSerializer.Deserialize<ProcessedTaskResult>(consumeResult.Message.Value);
            if (resultMessage == null)
            {
                _logger.LogWarning("Received null result message");
                _consumer!.Commit(consumeResult);
                return;
            }

            _logger.LogInformation("Collector {CollectorId} processing result for task {TaskId} (Success: {Success}, Rows: {Rows})",
                _collectorId, resultMessage.TaskId, resultMessage.Success, resultMessage.ProcessedRows);

            using var scope = _serviceProvider.CreateScope();
            var taskRepository = scope.ServiceProvider.GetRequiredService<ITaskRepository>();
            var resultRepository = scope.ServiceProvider.GetRequiredService<IResultRepository>();

            if (resultMessage.Success && resultMessage.Results.Count > 0)
            {
                // Bulk insert to destination database
                await resultRepository.BulkInsertResultsAsync(resultMessage.Results, cancellationToken);

                // Update task status to Completed
                await taskRepository.UpdateTaskStatusAsync(
                    resultMessage.TaskId,
                    TaskStatus.Completed,
                    resultMessage.WorkerId,
                    null,
                    cancellationToken);

                _logger.LogInformation("✓ Collector {CollectorId} stored {Count} rows for task {TaskId}",
                    _collectorId, resultMessage.Results.Count, resultMessage.TaskId);
            }
            else
            {
                // Mark task as failed
                await taskRepository.UpdateTaskStatusAsync(
                    resultMessage.TaskId,
                    TaskStatus.Failed,
                    resultMessage.WorkerId,
                    resultMessage.ErrorMessage,
                    cancellationToken);

                _logger.LogWarning("Task {TaskId} marked as failed: {Error}",
                    resultMessage.TaskId, resultMessage.ErrorMessage);
            }

            // Commit offset only after successful processing
            _consumer!.Commit(consumeResult);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing result for task {TaskId}", resultMessage?.TaskId);
            // Don't commit - will be retried
            throw;
        }
    }

    public override void Dispose()
    {
        _consumer?.Dispose();
        base.Dispose();
    }
}
