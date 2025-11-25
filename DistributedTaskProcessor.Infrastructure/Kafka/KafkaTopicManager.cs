using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using DistributedTaskProcessor.Shared.Configuration;

namespace DistributedTaskProcessor.Infrastructure.Kafka;

public interface IKafkaTopicManager
{
    Task CreateTopicsAsync(CancellationToken cancellationToken = default);
    Task<bool> TopicExistsAsync(string topicName, CancellationToken cancellationToken = default);
}

public class KafkaTopicManager : IKafkaTopicManager
{
    private readonly KafkaSettings _kafkaSettings;
    private readonly ILogger<KafkaTopicManager> _logger;

    public KafkaTopicManager(KafkaSettings kafkaSettings, ILogger<KafkaTopicManager> logger)
    {
        _kafkaSettings = kafkaSettings;
        _logger = logger;
    }

    public async Task CreateTopicsAsync(CancellationToken cancellationToken = default)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = _kafkaSettings.BootstrapServers
        };

        using var adminClient = new AdminClientBuilder(config).Build();

        try
        {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
            var existingTopics = metadata.Topics.Select(t => t.Topic).ToHashSet();

            var topicsToCreate = new List<TopicSpecification>();

            // Task Queue - Higher partitions for parallel processing
            if (!existingTopics.Contains(_kafkaSettings.TaskTopic))
            {
                topicsToCreate.Add(new TopicSpecification
                {
                    Name = _kafkaSettings.TaskTopic,
                    NumPartitions = 3,
                    ReplicationFactor = 1,
                    Configs = new Dictionary<string, string>
                {
                    { "retention.ms", "604800000" }, // 7 days
                    { "cleanup.policy", "delete" },
                    { "compression.type", "snappy" }
                }
                });
            }

            // Result Queue - Higher partitions for parallel collection
            if (!existingTopics.Contains(_kafkaSettings.ResultTopic))
            {
                topicsToCreate.Add(new TopicSpecification
                {
                    Name = _kafkaSettings.ResultTopic,
                    NumPartitions = 3,
                    ReplicationFactor = 1,
                    Configs = new Dictionary<string, string>
                {
                    { "retention.ms", "604800000" }, // 7 days
                    { "cleanup.policy", "delete" },
                    { "compression.type", "snappy" }
                }
                });
            }

            // Dead Letter Queue - Single partition, longer retention
            if (!existingTopics.Contains(_kafkaSettings.DeadLetterTopic))
            {
                topicsToCreate.Add(new TopicSpecification
                {
                    Name = _kafkaSettings.DeadLetterTopic,
                    NumPartitions = 1,
                    ReplicationFactor = 1,
                    Configs = new Dictionary<string, string>
                {
                    { "retention.ms", "2592000000" }, // 30 days
                    { "cleanup.policy", "delete" }
                }
                });
            }

            if (topicsToCreate.Any())
            {
                await adminClient.CreateTopicsAsync(topicsToCreate);
                _logger.LogInformation("✓ Successfully created {Count} Kafka topics", topicsToCreate.Count);

                foreach (var topic in topicsToCreate)
                {
                    _logger.LogInformation("  - {Topic} (Partitions: {Partitions})",
                        topic.Name, topic.NumPartitions);
                }
            }
            else
            {
                _logger.LogInformation("All required Kafka topics already exist");
            }
        }
        catch (CreateTopicsException ex)
        {
            foreach (var result in ex.Results)
            {
                if (result.Error.Code == ErrorCode.TopicAlreadyExists)
                {
                    _logger.LogInformation("Topic {Topic} already exists", result.Topic);
                }
                else if (result.Error.Code != ErrorCode.NoError)
                {
                    _logger.LogError("Failed to create topic {Topic}: {Error}",
                        result.Topic, result.Error.Reason);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating Kafka topics");
            throw;
        }
    }

    public async Task<bool> TopicExistsAsync(string topicName, CancellationToken cancellationToken = default)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = _kafkaSettings.BootstrapServers
        };

        using var adminClient = new AdminClientBuilder(config).Build();

        try
        {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
            var exists = metadata.Topics.Any(t => t.Topic == topicName);

            await Task.CompletedTask; // To satisfy async signature
            return exists;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking if topic {Topic} exists", topicName);
            return false;
        }
    }
}
