using Confluent.Kafka;
using System.Text.Json;
using DistributedTaskProcessor.Shared.Configuration;
using Microsoft.Extensions.Logging;

namespace DistributedTaskProcessor.Infrastructure.Kafka;

public interface IKafkaProducerService
{
    Task ProduceAsync<T>(string topic, string key, T message, CancellationToken cancellationToken = default);
    Task FlushAsync(CancellationToken cancellationToken = default);
}

public class KafkaProducerService : IKafkaProducerService, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducerService> _logger;
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    public KafkaProducerService(KafkaSettings kafkaSettings, ILogger<KafkaProducerService> logger)
    {
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = kafkaSettings.BootstrapServers,

            // Idempotency ensures exactly-once delivery
            EnableIdempotence = true,

            // Wait for all in-sync replicas to acknowledge
            Acks = Acks.All,

            // Retry settings
            MessageSendMaxRetries = 3,
            RetryBackoffMs = 1000,

            // Timeout settings
            RequestTimeoutMs = 30000,

            // Batching for performance
            LingerMs = 10,
            BatchSize = 16384,

            // Compression
            CompressionType = CompressionType.Snappy,

            // Client identification
            ClientId = $"producer-{Environment.MachineName}"
        };

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                _logger.LogError("Kafka Producer Error: {Reason}", error.Reason);
            })
            .SetLogHandler((_, logMessage) =>
            {
                if (logMessage.Level <= SyslogLevel.Warning)
                {
                    _logger.LogWarning("Kafka Producer Log: {Message}", logMessage.Message);
                }
            })
            .Build();

        _logger.LogInformation("Kafka Producer initialized with bootstrap servers: {Servers}",
            kafkaSettings.BootstrapServers);
    }

    public async Task ProduceAsync<T>(string topic, string key, T message, CancellationToken cancellationToken = default)
    {
        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            // FIX: Use default serialization (PascalCase to match C# properties)
            var jsonMessage = JsonSerializer.Serialize(message, new JsonSerializerOptions
            {
                WriteIndented = false,
                PropertyNamingPolicy = null // Changed from JsonNamingPolicy.CamelCase
            });

            var kafkaMessage = new Message<string, string>
            {
                Key = key,
                Value = jsonMessage,
                Timestamp = new Timestamp(DateTime.UtcNow)
            };

            var deliveryResult = await _producer.ProduceAsync(topic, kafkaMessage, cancellationToken);

            _logger.LogDebug("Message delivered to {Topic}, Partition: {Partition}, Offset: {Offset}, Key: {Key}",
                deliveryResult.Topic,
                deliveryResult.Partition.Value,
                deliveryResult.Offset.Value,
                key);
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex, "Failed to produce message to topic {Topic}: {Reason}",
                topic, ex.Error.Reason);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error producing message to topic {Topic}", topic);
            throw;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            _producer.Flush(cancellationToken);
            _logger.LogDebug("Kafka producer flushed successfully");
            await Task.CompletedTask;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error flushing Kafka producer");
            throw;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public void Dispose()
    {
        try
        {
            _producer?.Flush(TimeSpan.FromSeconds(10));
            _producer?.Dispose();
            _semaphore?.Dispose();
            _logger.LogInformation("Kafka Producer disposed");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error disposing Kafka producer");
        }
    }
}
