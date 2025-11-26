using Microsoft.EntityFrameworkCore;
using DistributedTaskProcessor.Infrastructure.Data;
using DistributedTaskProcessor.Shared.Models;
using TaskStatus = DistributedTaskProcessor.Shared.Models.TaskStatus;

namespace DistributedTaskProcessor.Collector.Services;

/// <summary>
/// Advanced collector with batch processing and result aggregation
/// </summary>
public interface IAdvancedResultCollectorService
{
    System.Threading.Tasks.Task StartCollectorAsync(CancellationToken cancellationToken = default);
}

public class AdvancedResultCollectorService : IAdvancedResultCollectorService
{
    private readonly TaskDbContext _dbContext;
    private readonly ILogger<AdvancedResultCollectorService> _logger;
    private const int BatchSize = 1000;
    private const int FlushIntervalMs = 5000;

    public AdvancedResultCollectorService(
        TaskDbContext dbContext,
        ILogger<AdvancedResultCollectorService> logger)
    {
        _dbContext = dbContext;
        _logger = logger;
    }

    public async System.Threading.Tasks.Task StartCollectorAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Advanced Result Collector started");

        var batch = new List<TenantResultMessage>();
        var lastFlush = DateTime.UtcNow;

        // Process results with batching
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Add result to batch (pseudo-code)
                // var result = await _kafkaConsumer.ConsumeAsync(...);
                // batch.Add(result);

                // Flush if batch full or timeout reached
                if (batch.Count >= BatchSize ||
                    (DateTime.UtcNow - lastFlush).TotalMilliseconds > FlushIntervalMs)
                {
                    await FlushBatchAsync(batch, cancellationToken);
                    batch.Clear();
                    lastFlush = DateTime.UtcNow;
                }

                await System.Threading.Tasks.Task.Delay(100, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in result collector");
            }
        }

        // Final flush
        if (batch.Count > 0)
        {
            await FlushBatchAsync(batch, cancellationToken);
        }
    }

    private async System.Threading.Tasks.Task FlushBatchAsync(
        List<TenantResultMessage> results,
        CancellationToken cancellationToken)
    {
        if (results.Count == 0)
            return;

        try
        {
            _logger.LogInformation("Flushing batch of {Count} results", results.Count);

            // Group by tenant for isolated processing
            var groupedByTenant = results.GroupBy(r => r.TenantId);

            foreach (var tenantGroup in groupedByTenant)
            {
                try
                {
                    await ProcessTenantResultsAsync(tenantGroup.ToList(), cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing results for tenant {TenantId}", tenantGroup.Key);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error flushing result batch");
        }
    }

    private async System.Threading.Tasks.Task ProcessTenantResultsAsync(
        List<TenantResultMessage> tenantResults,
        CancellationToken cancellationToken)
    {
        using var transaction = await _dbContext.Database.BeginTransactionAsync(cancellationToken);

        try
        {
            foreach (var result in tenantResults)
            {
                // Update task status
                var task = await _dbContext.Tasks
                    .FirstOrDefaultAsync(t => t.TaskId == result.TaskId, cancellationToken);

                if (task != null)
                {
                    task.Status = result.Success ? TaskStatus.Completed : TaskStatus.Failed;
                    if (!result.Success)
                    {
                        task.ErrorMessage = result.ErrorMessage;
                    }
                }

                _logger.LogInformation(
                    "Collected result for task {TaskId}. Status: {Status}",
                    result.TaskId, result.Success ? "Success" : "Failed");
            }

            await _dbContext.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);

            _logger.LogInformation(
                "Successfully processed {Count} results",
                tenantResults.Count);
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync(cancellationToken);
            _logger.LogError(ex, "Error processing tenant results");
            throw;
        }
    }
}
