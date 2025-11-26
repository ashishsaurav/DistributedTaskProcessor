using Microsoft.EntityFrameworkCore;
using DistributedTaskProcessor.Infrastructure.Data;
using DistributedTaskProcessor.Infrastructure.Repositories;
using DistributedTaskProcessor.Shared.Configuration;
using DistributedTaskProcessor.Shared.Monitoring;
using DistributedTaskProcessor.Collector.Services;

var builder = Host.CreateApplicationBuilder(args);

// Configuration
var kafkaSettings = builder.Configuration.GetSection("Kafka").Get<KafkaSettings>() ?? new KafkaSettings();
var multiTenantSettings = builder.Configuration.GetSection("MultiTenant").Get<MultiTenantSettings>() ?? new MultiTenantSettings();

builder.Services.AddSingleton(kafkaSettings);
builder.Services.AddSingleton(multiTenantSettings);

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

// Register MetricsService as singleton
var metricsService = new global::DistributedTaskProcessor.Shared.Monitoring.MetricsService();
builder.Services.AddSingleton<IMetricsService>(metricsService);

// Multi-Tenant Services
builder.Services.AddScoped<AdvancedResultCollectorService>();

// Background Services
builder.Services.AddHostedService<KafkaResultCollectorService>();
builder.Services.AddHostedService<AdvancedResultCollectorBackgroundService>();

var host = builder.Build();
host.Run();
