using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using DistributedTaskProcessor.Infrastructure.Data;
using DistributedTaskProcessor.Infrastructure.Kafka;
using DistributedTaskProcessor.Shared.Models;
using DistributedTaskProcessor.Shared.Configuration;
using System.Text.Json;
using TaskStatus = DistributedTaskProcessor.Shared.Models.TaskStatus;

namespace DistributedTaskProcessor.Coordinator.Services;

/// <summary>
/// Background service for partitioning data and sending tasks to Kafka
/// </summary>
public class TaskPartitionerBackgroundService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<TaskPartitionerBackgroundService> _logger;
    private readonly int _intervalSeconds;

    public TaskPartitionerBackgroundService(
        IServiceProvider serviceProvider,
        ILogger<TaskPartitionerBackgroundService> logger,
        IConfiguration configuration)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _intervalSeconds = configuration.GetValue("TaskPartitioningIntervalSeconds", 10);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Task Partitioner Background Service started with interval {IntervalSeconds}s", _intervalSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _serviceProvider.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TaskDbContext>();
                var kafkaProducer = scope.ServiceProvider.GetRequiredService<IKafkaProducerService>();
                var kafkaSettings = scope.ServiceProvider.GetRequiredService<DistributedTaskProcessor.Shared.Configuration.KafkaSettings>();

                // Get pending tasks (not yet sent to Kafka)
                var pendingTasks = await dbContext.Tasks
                    .Where(t => t.Status == TaskStatus.Pending)
                    .Take(100)
                    .ToListAsync(stoppingToken);

                if (pendingTasks.Count > 0)
                {
                    _logger.LogInformation("Found {TaskCount} pending tasks. Sending to Kafka...", pendingTasks.Count);

                    foreach (var task in pendingTasks)
                    {
                        try
                        {
                            var taskMessage = new TaskMessage
                            {
                                TaskId = task.TaskId,
                                Symbol = task.Symbol,
                                Fund = task.Fund,
                                RunDate = task.CreatedAt.Date,
                                StartRow = task.StartRow,
                                EndRow = task.EndRow,
                                TenantId = task.TenantId
                            };

                            await kafkaProducer.ProduceAsync(
                                kafkaSettings.TaskTopic,
                                task.TaskId.ToString(),
                                taskMessage,
                                stoppingToken);

                            // Mark task as sent (InProgress will be set by worker)
                            task.Status = TaskStatus.InProgress;

                            _logger.LogInformation("Task {TaskId} sent to Kafka", task.TaskId);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error sending task {TaskId} to Kafka", task.TaskId);
                        }
                    }

                    await dbContext.SaveChangesAsync(stoppingToken);
                }

                await Task.Delay(TimeSpan.FromSeconds(_intervalSeconds), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Task Partitioner Background Service cancelled during shutdown");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in Task Partitioner Background Service");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        _logger.LogInformation("Task Partitioner Background Service stopped");
    }
}
