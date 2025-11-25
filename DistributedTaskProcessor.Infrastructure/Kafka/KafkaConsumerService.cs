using Confluent.Kafka;
using System.Text.Json;
using DistributedTaskProcessor.Shared.Configuration;
using Microsoft.Extensions.Logging;

namespace DistributedTaskProcessor.Infrastructure.Kafka;

public abstract class KafkaConsumerServiceBase<TMessage> : IDisposable where TMessage : class
{
    protected readonly ILogger Logger;
    protected readonly KafkaSettings KafkaSettings;
    protected IConsumer<string, string>? Consumer;

    protected KafkaConsumerServiceBase(
        KafkaSettings kafkaSettings,
        ILogger logger)
    {
        KafkaSettings = kafkaSettings;
        Logger = logger;
    }

    protected IConsumer<string, string> CreateConsumer(string groupId, string topic)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = KafkaSettings.BootstrapServers,
            GroupId = groupId,

            // Manual offset management for fault tolerance
            EnableAutoCommit = false,

            // Start from earliest if no committed offset exists
            AutoOffsetReset = AutoOffsetReset.Earliest,

            // Session and heartbeat configuration
            SessionTimeoutMs = KafkaSettings.SessionTimeoutMs,
            HeartbeatIntervalMs = KafkaSettings.HeartbeatIntervalMs,

            // Max poll configuration
            MaxPollIntervalMs = 300000, // 5 minutes

            // Partition assignment strategy
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,

            // Isolation level
            IsolationLevel = IsolationLevel.ReadCommitted,

            // Client identification
            ClientId = $"consumer-{groupId}-{Environment.MachineName}"
        };

        var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                Logger.LogError("Kafka Consumer Error: {Reason}", error.Reason);
            })
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                Logger.LogInformation("Partitions assigned: {Partitions}",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition.Value}]")));
            })
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                Logger.LogInformation("Partitions revoked: {Partitions}",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition.Value}]")));
            })
            .SetPartitionsLostHandler((c, partitions) =>
            {
                Logger.LogWarning("Partitions lost: {Partitions}",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition.Value}]")));
            })
            .Build();

        consumer.Subscribe(topic);
        Logger.LogInformation("Kafka Consumer subscribed to topic: {Topic} with group: {GroupId}",
            topic, groupId);

        return consumer;
    }

    protected TMessage? DeserializeMessage(string jsonMessage)
    {
        try
        {
            return JsonSerializer.Deserialize<TMessage>(jsonMessage, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error deserializing message: {Message}", jsonMessage);
            return null;
        }
    }

    public virtual void Dispose()
    {
        try
        {
            Consumer?.Close();
            Consumer?.Dispose();
            Logger.LogInformation("Kafka Consumer disposed");
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error disposing Kafka consumer");
        }
    }
}
