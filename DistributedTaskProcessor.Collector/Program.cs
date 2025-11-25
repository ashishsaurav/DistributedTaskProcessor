using Microsoft.EntityFrameworkCore;
using DistributedTaskProcessor.Infrastructure.Data;
using DistributedTaskProcessor.Infrastructure.Repositories;
using DistributedTaskProcessor.Shared.Configuration;
using DistributedTaskProcessor.Collector.Services;

var builder = Host.CreateApplicationBuilder(args);

// Configuration
var kafkaSettings = builder.Configuration.GetSection("Kafka").Get<KafkaSettings>() ?? new KafkaSettings();

builder.Services.AddSingleton(kafkaSettings);

// Database
builder.Services.AddDbContext<TaskDbContext>(options =>
    options.UseSqlServer(
        builder.Configuration.GetConnectionString("DefaultConnection"),
        sqlOptions => sqlOptions.EnableRetryOnFailure(
            maxRetryCount: 3,
            maxRetryDelay: TimeSpan.FromSeconds(5),
            errorNumbersToAdd: null
        )
    ));

// Repositories
builder.Services.AddScoped<ITaskRepository, TaskRepository>();
builder.Services.AddScoped<IResultRepository, ResultRepository>();

// Background Service
builder.Services.AddHostedService<KafkaResultCollectorService>();

var host = builder.Build();
host.Run();
