using Microsoft.AspNetCore.Mvc;
using DistributedTaskProcessor.Infrastructure.Kafka;
using DistributedTaskProcessor.Shared.Configuration;

namespace DistributedTaskProcessor.Coordinator.Controllers;

[ApiController]
[Route("api/[controller]")]
public class KafkaController : ControllerBase
{
    private readonly IKafkaTopicManager _topicManager;
    private readonly KafkaSettings _kafkaSettings;
    private readonly ILogger<KafkaController> _logger;

    public KafkaController(
        IKafkaTopicManager topicManager,
        KafkaSettings kafkaSettings,
        ILogger<KafkaController> logger)
    {
        _topicManager = topicManager;
        _kafkaSettings = kafkaSettings;
        _logger = logger;
    }

    [HttpGet("topics/verify")]
    public async Task<IActionResult> VerifyTopics()
    {
        var topics = new[]
        {
            _kafkaSettings.TaskTopic,
            _kafkaSettings.ResultTopic,
            _kafkaSettings.DeadLetterTopic
        };

        var results = new Dictionary<string, bool>();

        foreach (var topic in topics)
        {
            var exists = await _topicManager.TopicExistsAsync(topic);
            results[topic] = exists;
        }

        return Ok(new
        {
            KafkaServer = _kafkaSettings.BootstrapServers,
            Topics = results,
            AllTopicsExist = results.Values.All(v => v)
        });
    }

    [HttpPost("topics/create")]
    public async Task<IActionResult> CreateTopics()
    {
        try
        {
            await _topicManager.CreateTopicsAsync();
            return Ok(new { Message = "Topics created successfully" });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating topics");
            return StatusCode(500, new { Error = ex.Message });
        }
    }
}
