using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using DistributedTaskProcessor.Infrastructure.Data;
using DistributedTaskProcessor.Infrastructure.Repositories;
using DistributedTaskProcessor.Infrastructure.Kafka;
using DistributedTaskProcessor.Shared.Configuration;
using DistributedTaskProcessor.Shared.Models;
using DistributedTaskProcessor.Coordinator.Services;

var builder = WebApplication.CreateBuilder(args);

// Configuration
var kafkaSettings = builder.Configuration.GetSection("Kafka").Get<KafkaSettings>() ?? new KafkaSettings();
var systemSettings = builder.Configuration.GetSection("System").Get<SystemSettings>() ?? new SystemSettings();
var multiTenantSettings = builder.Configuration.GetSection("MultiTenant").Get<MultiTenantSettings>() ?? new MultiTenantSettings();

builder.Services.AddSingleton(kafkaSettings);
builder.Services.AddSingleton(systemSettings);
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

// Repositories and Services
builder.Services.AddScoped<ITaskRepository, TaskRepository>();
builder.Services.AddScoped<ISourceDataRepository, SourceDataRepository>();
builder.Services.AddSingleton<IKafkaProducerService, KafkaProducerService>();

// Kafka Topic Manager
builder.Services.AddSingleton<IKafkaTopicManager, KafkaTopicManager>();

// Multi-Tenant Services
builder.Services.AddScoped<DataFetcherService>();
builder.Services.AddScoped<AdvancedTaskPartitionerService>();
builder.Services.AddSingleton<FaultDetectionService>();

// Background Services
builder.Services.AddSingleton<LeaderElectionService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<LeaderElectionService>());

builder.Services.AddHostedService<CoordinatorHeartbeatService>();
builder.Services.AddHostedService<FaultDetectionBackgroundService>();
builder.Services.AddHostedService<DataFetcherBackgroundService>();
builder.Services.AddHostedService<TaskPartitionerBackgroundService>();

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

// SignalR for Real-Time Updates
builder.Services.AddSignalR();

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

        // 3. Create sample data for testing
        var dbContext = scope.ServiceProvider.GetRequiredService<TaskDbContext>();
        var existingTasks = await dbContext.Tasks.AnyAsync();

        if (!existingTasks)
        {
            logger.LogInformation("Creating sample test data...");
            var sampleTasks = new List<TaskEntity>();

            for (int i = 0; i < 50; i++)
            {
                sampleTasks.Add(new TaskEntity
                {
                    TaskId = Guid.NewGuid(),
                    Symbol = i % 5 == 0 ? "AAPL" : i % 5 == 1 ? "GOOGL" : i % 5 == 2 ? "MSFT" : i % 5 == 3 ? "AMZN" : "TSLA",
                    Fund = i % 3 == 0 ? "Fund-A" : i % 3 == 1 ? "Fund-B" : "Fund-C",
                    RunDate = DateTime.UtcNow.Date,
                    StartRow = i * 1000,
                    EndRow = (i + 1) * 1000 - 1,
                    Status = TaskStatus.Pending,
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow,
                    TenantId = Guid.Empty,
                    RetryCount = 0
                });
            }

            await dbContext.Tasks.AddRangeAsync(sampleTasks);
            await dbContext.SaveChangesAsync();
            logger.LogInformation("✓ Created {TaskCount} sample tasks", sampleTasks.Count);
        }

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
