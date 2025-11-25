using Microsoft.EntityFrameworkCore;
using DistributedTaskProcessor.Infrastructure.Data;
using DistributedTaskProcessor.Infrastructure.Repositories;
using DistributedTaskProcessor.Infrastructure.Kafka;
using DistributedTaskProcessor.Shared.Configuration;
using DistributedTaskProcessor.Worker.Services;

var builder = Host.CreateApplicationBuilder(args);

var kafkaSettings = builder.Configuration.GetSection("Kafka").Get<KafkaSettings>() ?? new KafkaSettings();
var systemSettings = builder.Configuration.GetSection("System").Get<SystemSettings>() ?? new SystemSettings();

builder.Services.AddSingleton(kafkaSettings);
builder.Services.AddSingleton(systemSettings);

builder.Services.AddDbContext<TaskDbContext>(options =>
    options.UseSqlServer(
        builder.Configuration.GetConnectionString("DefaultConnection"),
        sqlOptions => sqlOptions.EnableRetryOnFailure(
            maxRetryCount: 3,
            maxRetryDelay: TimeSpan.FromSeconds(5),
            errorNumbersToAdd: null
        )
    ));

builder.Services.AddScoped<ITaskRepository, TaskRepository>();
builder.Services.AddScoped<ISourceDataRepository, SourceDataRepository>();
builder.Services.AddSingleton<IKafkaProducerService, KafkaProducerService>();

// Add topic manager for verification (optional)
builder.Services.AddSingleton<IKafkaTopicManager, KafkaTopicManager>();

builder.Services.AddHostedService<KafkaTaskConsumerService>();
builder.Services.AddHostedService<WorkerHeartbeatService>();

var host = builder.Build();

// Optional: Verify topics exist before starting
using (var scope = host.Services.CreateScope())
{
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
    var topicManager = scope.ServiceProvider.GetRequiredService<IKafkaTopicManager>();

    logger.LogInformation("Verifying Kafka topics...");
    var taskTopicExists = await topicManager.TopicExistsAsync(kafkaSettings.TaskTopic);

    if (!taskTopicExists)
    {
        logger.LogWarning("Task topic does not exist. Please start Coordinator first to create topics.");
    }
    else
    {
        logger.LogInformation("✓ Kafka topics verified");
    }
}

host.Run();
