using Confluent.Kafka;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using DistributedTaskProcessor.Shared.Models;
using DistributedTaskProcessor.Shared.Configuration;
using DistributedTaskProcessor.Infrastructure.Repositories;
using DistributedTaskProcessor.Infrastructure.Kafka;
using TaskStatus = DistributedTaskProcessor.Shared.Models.TaskStatus;

namespace DistributedTaskProcessor.Worker.Services;

public class KafkaTaskConsumerService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<KafkaTaskConsumerService> _logger;
    private readonly IKafkaProducerService _kafkaProducer;
    private readonly KafkaSettings _kafkaSettings;
    private readonly string _workerId;
    private IConsumer<string, string>? _consumer;

    public KafkaTaskConsumerService(
        IServiceProvider serviceProvider,
        ILogger<KafkaTaskConsumerService> logger,
        IKafkaProducerService kafkaProducer,
        KafkaSettings kafkaSettings)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _kafkaProducer = kafkaProducer;
        _kafkaSettings = kafkaSettings;
        _workerId = $"{Environment.MachineName}_Worker_{Guid.NewGuid().ToString()[..8]}";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaSettings.BootstrapServers,
            GroupId = _kafkaSettings.WorkerGroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            SessionTimeoutMs = _kafkaSettings.SessionTimeoutMs,
            HeartbeatIntervalMs = _kafkaSettings.HeartbeatIntervalMs,
            MaxPollIntervalMs = 300000,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            IsolationLevel = IsolationLevel.ReadCommitted
        };

        _consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, error) => _logger.LogError("Kafka Error: {Reason}", error.Reason))
            .SetPartitionsAssignedHandler((c, partitions) =>
                _logger.LogInformation("Worker {WorkerId} assigned partitions: {Partitions}",
                    _workerId, string.Join(", ", partitions.Select(p => p.Partition.Value))))
            .Build();

        _consumer.Subscribe(_kafkaSettings.TaskTopic);
        _logger.LogInformation("Worker {WorkerId} started consuming from topic: {Topic}",
            _workerId, _kafkaSettings.TaskTopic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(stoppingToken);
                    if (consumeResult?.Message != null)
                    {
                        await ProcessTaskAsync(consumeResult, stoppingToken);
                    }
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Error consuming message");
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Worker {WorkerId} shutting down", _workerId);
        }
        finally
        {
            _consumer.Close();
            _consumer.Dispose();
        }
    }

    private async Task ProcessTaskAsync(ConsumeResult<string, string> consumeResult, CancellationToken cancellationToken)
    {
        TaskMessage? taskMessage = null;

        try
        {
            // FIX: Use case-insensitive deserialization
            var jsonOptions = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true // Add this for robustness
            };

            taskMessage = JsonSerializer.Deserialize<TaskMessage>(consumeResult.Message.Value, jsonOptions);

            if (taskMessage == null)
            {
                _logger.LogWarning("Received null task message");
                _consumer!.Commit(consumeResult);
                return;
            }

            // FIX: Add validation for empty GUID
            if (taskMessage.TaskId == Guid.Empty)
            {
                _logger.LogError("Received task with empty GUID. Message: {Message}", consumeResult.Message.Value);
                _consumer!.Commit(consumeResult);
                return;
            }

            // FIX: Validate required fields
            if (string.IsNullOrEmpty(taskMessage.Symbol) || string.IsNullOrEmpty(taskMessage.Fund))
            {
                _logger.LogError("Received task with missing Symbol/Fund. TaskId: {TaskId}, Symbol: '{Symbol}', Fund: '{Fund}'",
                    taskMessage.TaskId, taskMessage.Symbol, taskMessage.Fund);
                _consumer!.Commit(consumeResult);
                return;
            }

            _logger.LogInformation("Worker {WorkerId} processing task {TaskId} ({Symbol}/{Fund}, rows {Start}-{End})",
                _workerId, taskMessage.TaskId, taskMessage.Symbol, taskMessage.Fund,
                taskMessage.StartRow, taskMessage.EndRow);

            taskMessage = JsonSerializer.Deserialize<TaskMessage>(consumeResult.Message.Value);
            if (taskMessage == null) return;

            _logger.LogInformation("Worker {WorkerId} processing task {TaskId} ({Symbol}/{Fund}, rows {Start}-{End})",
                _workerId, taskMessage.TaskId, taskMessage.Symbol, taskMessage.Fund,
                taskMessage.StartRow, taskMessage.EndRow);

            using var scope = _serviceProvider.CreateScope();
            var taskRepository = scope.ServiceProvider.GetRequiredService<ITaskRepository>();
            var sourceRepository = scope.ServiceProvider.GetRequiredService<ISourceDataRepository>();

            // Update status to InProgress
            await taskRepository.UpdateTaskStatusAsync(
                taskMessage.TaskId,
                TaskStatus.InProgress,
                _workerId,
                null,
                cancellationToken);

            // Fetch source data
            var sourceData = await sourceRepository.GetDataRangeAsync(
                taskMessage.Symbol,
                taskMessage.Fund,
                taskMessage.RunDate,
                taskMessage.StartRow,
                taskMessage.EndRow,
                cancellationToken);

            if (sourceData.Count == 0)
            {
                _logger.LogWarning("No source data found for task {TaskId}", taskMessage.TaskId);
            }

            // Execute algorithm
            var results = await ExecuteCalculationAlgorithmAsync(taskMessage.TaskId, sourceData, cancellationToken);

            // Publish results
            var resultMessage = new ProcessedTaskResult
            {
                TaskId = taskMessage.TaskId,
                Results = results,
                Success = true,
                WorkerId = _workerId,
                ProcessedRows = results.Count,
                CompletedAt = DateTime.UtcNow
            };

            await _kafkaProducer.ProduceAsync(
                _kafkaSettings.ResultTopic,
                taskMessage.TaskId.ToString(),
                resultMessage,
                cancellationToken);

            // Commit offset only after successful processing
            _consumer!.Commit(consumeResult);

            _logger.LogInformation("✓ Worker {WorkerId} completed task {TaskId}: {Rows} rows processed",
                _workerId, taskMessage.TaskId, results.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Worker {WorkerId} failed task {TaskId}", _workerId, taskMessage?.TaskId);

            if (taskMessage != null)
            {
                using var scope = _serviceProvider.CreateScope();
                var taskRepository = scope.ServiceProvider.GetRequiredService<ITaskRepository>();

                await taskRepository.UpdateTaskStatusAsync(
                    taskMessage.TaskId,
                    TaskStatus.Failed,
                    _workerId,
                    ex.Message,
                    cancellationToken);

                var failureMessage = new ProcessedTaskResult
                {
                    TaskId = taskMessage.TaskId,
                    Success = false,
                    ErrorMessage = ex.Message,
                    WorkerId = _workerId,
                    CompletedAt = DateTime.UtcNow
                };

                await _kafkaProducer.ProduceAsync(
                    _kafkaSettings.ResultTopic,
                    taskMessage.TaskId.ToString(),
                    failureMessage,
                    cancellationToken);

                // Commit offset even on failure
                _consumer!.Commit(consumeResult);
            }
        }
    }

    private async Task<List<CalculatedResult>> ExecuteCalculationAlgorithmAsync(
        Guid taskId,
        List<SourceMarketData> sourceData,
        CancellationToken cancellationToken)
    {
        // Simulate processing time
        await Task.Delay(TimeSpan.FromMilliseconds(sourceData.Count * 2), cancellationToken);

        var results = new List<CalculatedResult>();
        var prices = sourceData.Select(s => s.Price).ToList();

        for (int i = 0; i < sourceData.Count; i++)
        {
            var source = sourceData[i];

            // Calculate moving average (last 5 entries)
            var movingAvg = CalculateMovingAverage(prices, i, 5);

            // Calculate volume-weighted price
            var vwap = CalculateVWAP(sourceData, i, 5);

            // Calculate price change percentage
            var priceChange = i > 0
                ? ((source.Price - sourceData[i - 1].Price) / sourceData[i - 1].Price) * 100
                : 0;

            // Calculate volatility index
            var volatility = CalculateVolatility(prices, i, 10);

            // Determine trend
            var trend = DetermineTrend(priceChange, movingAvg, source.Price);

            results.Add(new CalculatedResult
            {
                TaskId = taskId,
                SourceId = source.Id,
                Symbol = source.Symbol,
                Fund = source.Fund,
                RunDate = source.RunDate,
                Price = source.Price,
                Volume = source.Volume,
                MovingAverage = movingAvg,
                VolumeWeightedPrice = vwap,
                PriceChange = priceChange,
                VolatilityIndex = volatility,
                TrendIndicator = trend,
                CreatedAt = DateTime.UtcNow
            });
        }

        return results;
    }

    private decimal CalculateMovingAverage(List<decimal> prices, int currentIndex, int period)
    {
        var startIndex = Math.Max(0, currentIndex - period + 1);
        var count = currentIndex - startIndex + 1;
        var sum = prices.Skip(startIndex).Take(count).Sum();
        return count > 0 ? sum / count : 0;
    }

    private decimal CalculateVWAP(List<SourceMarketData> data, int currentIndex, int period)
    {
        var startIndex = Math.Max(0, currentIndex - period + 1);
        var subset = data.Skip(startIndex).Take(currentIndex - startIndex + 1);

        var totalVolumePrice = subset.Sum(s => s.Price * s.Volume);
        var totalVolume = subset.Sum(s => s.Volume);

        return totalVolume > 0 ? totalVolumePrice / totalVolume : 0;
    }

    private decimal CalculateVolatility(List<decimal> prices, int currentIndex, int period)
    {
        var startIndex = Math.Max(0, currentIndex - period + 1);
        var subset = prices.Skip(startIndex).Take(currentIndex - startIndex + 1).ToList();

        if (subset.Count < 2) return 0;

        var mean = subset.Average();
        var variance = subset.Sum(p => (p - mean) * (p - mean)) / subset.Count;
        return (decimal)Math.Sqrt((double)variance);
    }

    private string DetermineTrend(decimal priceChange, decimal movingAvg, decimal currentPrice)
    {
        if (priceChange > 2 && currentPrice > movingAvg) return "Strong Uptrend";
        if (priceChange > 0 && currentPrice > movingAvg) return "Uptrend";
        if (priceChange < -2 && currentPrice < movingAvg) return "Strong Downtrend";
        if (priceChange < 0 && currentPrice < movingAvg) return "Downtrend";
        return "Neutral";
    }

    public override void Dispose()
    {
        _consumer?.Dispose();
        base.Dispose();
    }
}
