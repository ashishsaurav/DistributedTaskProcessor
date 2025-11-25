using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using DistributedTaskProcessor.Infrastructure.Data;
using DistributedTaskProcessor.Infrastructure.Repositories;
using DistributedTaskProcessor.Infrastructure.Kafka;
using DistributedTaskProcessor.Shared.Configuration;
using DistributedTaskProcessor.Coordinator.Services;

var builder = WebApplication.CreateBuilder(args);

// Configuration
var kafkaSettings = builder.Configuration.GetSection("Kafka").Get<KafkaSettings>() ?? new KafkaSettings();
var systemSettings = builder.Configuration.GetSection("System").Get<SystemSettings>() ?? new SystemSettings();

builder.Services.AddSingleton(kafkaSettings);
builder.Services.AddSingleton(systemSettings);

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

// Repositories and Services
builder.Services.AddScoped<ITaskRepository, TaskRepository>();
builder.Services.AddScoped<ISourceDataRepository, SourceDataRepository>();
builder.Services.AddSingleton<IKafkaProducerService, KafkaProducerService>();

// Kafka Topic Manager
builder.Services.AddSingleton<IKafkaTopicManager, KafkaTopicManager>();

// Background Services
builder.Services.AddSingleton<LeaderElectionService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<LeaderElectionService>());

builder.Services.AddHostedService<TaskPartitionerService>();
builder.Services.AddHostedService<FailureDetectionService>();
builder.Services.AddHostedService<CoordinatorHeartbeatService>();

// CORS for Dashboard
builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowAll", builder =>
    {
        builder.AllowAnyOrigin()
               .AllowAnyMethod()
               .AllowAnyHeader();
    });
});

// API
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Health Checks
builder.Services.AddHealthChecks()
    .AddDbContextCheck<TaskDbContext>("database", HealthStatus.Unhealthy, new[] { "db", "sql" });

var app = builder.Build();

// Initialize infrastructure on startup
using (var scope = app.Services.CreateScope())
{
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();

    try
    {
        logger.LogInformation("Initializing infrastructure...");

        // 1. Create/Update Database
        //logger.LogInformation("Setting up database...");
        //var db = scope.ServiceProvider.GetRequiredService<TaskDbContext>();
        //await db.Database.MigrateAsync();
        //logger.LogInformation("✓ Database ready");

        // 2. Create Kafka Topics
        logger.LogInformation("Setting up Kafka topics...");
        var topicManager = scope.ServiceProvider.GetRequiredService<IKafkaTopicManager>();
        await topicManager.CreateTopicsAsync();
        logger.LogInformation("✓ Kafka topics ready");

        logger.LogInformation("✓ Infrastructure initialization complete");
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Failed to initialize infrastructure");
        throw;
    }
}

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// Enable CORS
app.UseCors("AllowAll");

// Static files for Dashboard
app.UseDefaultFiles();
app.UseStaticFiles();

// Fallback to index.html for SPA
app.Use(async (context, next) =>
{
    await next();
    if (context.Response.StatusCode == 404 &&
        !Path.HasExtension(context.Request.Path.Value) &&
        !context.Request.Path.Value.StartsWith("/api"))
    {
        context.Request.Path = "/index.html";
        await next();
    }
});

app.MapControllers();
app.MapHealthChecks("/health");

app.Run();
