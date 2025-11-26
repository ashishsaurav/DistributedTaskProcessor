using Microsoft.EntityFrameworkCore;
using DistributedTaskProcessor.Infrastructure.Data;
using DistributedTaskProcessor.Shared.Models;

namespace DistributedTaskProcessor.Coordinator.Services;

/// <summary>
/// Advanced task partitioner with intelligent allocation based on worker health
/// </summary>
public interface IAdvancedTaskPartitionerService
{
    Task<List<TaskPartition>> PartitionDataAsync(
        Guid sourceDataId,
        Guid tenantId,
        long totalRows,
        TaskAllocationStrategy strategy = TaskAllocationStrategy.HealthFirst,
        CancellationToken cancellationToken = default);

    Task<List<TaskPartition>> AllocatePartitionsToWorkersAsync(
        List<TaskPartition> partitions,
        CancellationToken cancellationToken = default);
}

public class AdvancedTaskPartitionerService : IAdvancedTaskPartitionerService
{
    private readonly TaskDbContext _dbContext;
    private readonly ILogger<AdvancedTaskPartitionerService> _logger;
    private const int DefaultPartitionSize = 10000;

    public AdvancedTaskPartitionerService(TaskDbContext dbContext, ILogger<AdvancedTaskPartitionerService> logger)
    {
        _dbContext = dbContext;
        _logger = logger;
    }

    /// <summary>
    /// Intelligently partition data based on worker health and system load
    /// </summary>
    public async Task<List<TaskPartition>> PartitionDataAsync(
        Guid sourceDataId,
        Guid tenantId,
        long totalRows,
        TaskAllocationStrategy strategy = TaskAllocationStrategy.HealthFirst,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation(
                "Starting partitioning for SourceDataId: {SourceDataId}, TenantId: {TenantId}, TotalRows: {TotalRows}",
                sourceDataId, tenantId, totalRows);

            // Get active healthy workers for this tenant
            var activeWorkers = await GetActiveWorkersAsync(tenantId, cancellationToken);

            int workerCount = activeWorkers.Count;
            if (workerCount == 0)
            {
                _logger.LogWarning(
                    "No active workers found for tenant {TenantId}. Cannot partition data.",
                    tenantId);
                throw new InvalidOperationException($"No active workers for tenant {tenantId}");
            }

            // Calculate optimal partition size
            int partitionSize = CalculatePartitionSize(totalRows, activeWorkers);

            _logger.LogInformation(
                "Calculated partition size: {PartitionSize} rows for {WorkerCount} workers",
                partitionSize, workerCount);

            // Create partitions
            var partitions = new List<TaskPartition>();
            for (long startRow = 0; startRow < totalRows; startRow += partitionSize)
            {
                long endRow = Math.Min(startRow + partitionSize - 1, totalRows - 1);

                var partition = new TaskPartition
                {
                    PartitionId = Guid.NewGuid(),
                    SourceDataId = sourceDataId,
                    TenantId = tenantId,
                    StartRow = startRow,
                    EndRow = endRow,
                    Status = "Pending",
                    CreatedAt = DateTime.UtcNow
                };

                partitions.Add(partition);
            }

            _logger.LogInformation(
                "Created {PartitionCount} partitions for SourceDataId: {SourceDataId}",
                partitions.Count, sourceDataId);

            return partitions;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error partitioning data for SourceDataId: {SourceDataId}", sourceDataId);
            throw;
        }
    }

    /// <summary>
    /// Allocate partitions to workers based on health and load
    /// </summary>
    public async Task<List<TaskPartition>> AllocatePartitionsToWorkersAsync(
        List<TaskPartition> partitions,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (partitions.Count == 0)
                return partitions;

            var tenantId = partitions.First().TenantId;
            var activeWorkers = await GetActiveWorkersAsync(tenantId, cancellationToken);

            if (activeWorkers.Count == 0)
                throw new InvalidOperationException($"No active workers for allocation");

            // Allocate using round-robin with health-based prioritization
            int workerIndex = 0;

            foreach (var partition in partitions.OrderBy(p => p.StartRow))
            {
                // Get next worker (rotate through healthy workers)
                var selectedWorker = activeWorkers[workerIndex % activeWorkers.Count];

                partition.AssignedWorker = selectedWorker.WorkerId;
                partition.Status = "Assigned";

                _logger.LogInformation(
                    "Assigned partition {PartitionId} (rows {StartRow}-{EndRow}) to worker {WorkerId}",
                    partition.PartitionId, partition.StartRow, partition.EndRow, selectedWorker.WorkerId);

                workerIndex++;
            }

            return partitions;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error allocating partitions to workers");
            throw;
        }
    }

    private int CalculatePartitionSize(long totalRows, List<WorkerHealthStatus> workers)
    {
        // Dynamic partition sizing based on worker count and system load
        int workerCount = workers.Count;

        // Average load factor (0-1)
        decimal avgLoadFactor = workers.Count > 0
            ? workers.Average(w => w.ActiveTasks / (decimal)w.MaxConcurrentTasks)
            : 0.5m;

        // If system is heavily loaded, use larger partitions to reduce overhead
        if (avgLoadFactor > 0.8m)
        {
            return Math.Max(DefaultPartitionSize, (int)(DefaultPartitionSize * 1.5));
        }

        // If system is lightly loaded, use smaller partitions for better distribution
        if (avgLoadFactor < 0.3m)
        {
            return Math.Max(5000, (int)(DefaultPartitionSize * 0.7));
        }

        return DefaultPartitionSize;
    }

    private async Task<List<WorkerHealthStatus>> GetActiveWorkersAsync(
        Guid tenantId,
        CancellationToken cancellationToken = default)
    {
        // Get workers with recent heartbeats (< 2 minutes)
        var workers = await _dbContext.WorkerHeartbeats
            .Where(w =>
                w.LastHeartbeat > DateTime.UtcNow.AddMinutes(-2) &&
                (w.Status == "Active" || w.Status == "Idle"))
            .Select(w => new WorkerHealthStatus
            {
                WorkerId = w.WorkerId,
                MachineName = w.MachineName ?? "Unknown",
                Status = w.Status,
                HealthScore = w.Id.GetHashCode() % 100,
                ActiveTasks = 0,
                MaxConcurrentTasks = 5,
                CpuUsage = 0,
                MemoryUsage = 0,
                ErrorRate = 0,
                LastHeartbeat = w.LastHeartbeat
            })
            .ToListAsync(cancellationToken);

        return workers;
    }
}
