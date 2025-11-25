using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using DistributedTaskProcessor.Coordinator.Services;
using DistributedTaskProcessor.Infrastructure.Data;
using DistributedTaskProcessor.Shared.Models;
using TaskStatus = DistributedTaskProcessor.Shared.Models.TaskStatus;

namespace DistributedTaskProcessor.Coordinator.Controllers;

[ApiController]
[Route("api/[controller]")]
public class HealthController : ControllerBase
{
    private readonly LeaderElectionService _leaderElection;
    private readonly TaskDbContext _dbContext;
    private readonly ILogger<HealthController> _logger;

    public HealthController(
        LeaderElectionService leaderElection,
        TaskDbContext dbContext,
        ILogger<HealthController> logger)
    {
        _leaderElection = leaderElection;
        _dbContext = dbContext;
        _logger = logger;
    }

    [HttpGet]
    public async Task<IActionResult> Get()
    {
        try
        {
            // Check database connectivity
            var canConnect = await _dbContext.Database.CanConnectAsync();

            // Get task statistics
            var taskStats = await _dbContext.Tasks
                .GroupBy(t => t.Status)
                .Select(g => new { Status = g.Key, Count = g.Count() })
                .ToListAsync();

            return Ok(new
            {
                Status = "Healthy",
                CoordinatorId = _leaderElection.CoordinatorId,
                IsLeader = _leaderElection.IsLeader,
                MachineName = Environment.MachineName,
                DatabaseConnected = canConnect,
                TaskStatistics = taskStats,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Health check failed");
            return StatusCode(500, new
            {
                Status = "Unhealthy",
                Error = ex.Message,
                Timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpGet("leader")]
    public IActionResult GetLeaderStatus()
    {
        return Ok(new
        {
            CoordinatorId = _leaderElection.CoordinatorId,
            IsLeader = _leaderElection.IsLeader,
            MachineName = Environment.MachineName,
            Timestamp = DateTime.UtcNow
        });
    }

    [HttpGet("detailed")]
    public async Task<IActionResult> GetDetailedHealth()
    {
        try
        {
            var pendingTasks = await _dbContext.Tasks.CountAsync(t => t.Status == TaskStatus.Pending);
            var inProgressTasks = await _dbContext.Tasks.CountAsync(t => t.Status == TaskStatus.InProgress);
            var completedTasks = await _dbContext.Tasks.CountAsync(t => t.Status == TaskStatus.Completed);
            var failedTasks = await _dbContext.Tasks.CountAsync(t => t.Status == TaskStatus.Failed);
            var deadLetterTasks = await _dbContext.Tasks.CountAsync(t => t.Status == TaskStatus.DeadLetter);

            var activeWorkers = await _dbContext.WorkerHeartbeats
                .Where(w => w.LastHeartbeat > DateTime.UtcNow.AddMinutes(-2))
                .CountAsync();

            var activeCoordinators = await _dbContext.CoordinatorStates
                .Where(c => c.LastHeartbeat > DateTime.UtcNow.AddSeconds(-30))
                .CountAsync();

            return Ok(new
            {
                Coordinator = new
                {
                    Id = _leaderElection.CoordinatorId,
                    IsLeader = _leaderElection.IsLeader,
                    MachineName = Environment.MachineName
                },
                Tasks = new
                {
                    Pending = pendingTasks,
                    InProgress = inProgressTasks,
                    Completed = completedTasks,
                    Failed = failedTasks,
                    DeadLetter = deadLetterTasks,
                    Total = pendingTasks + inProgressTasks + completedTasks + failedTasks + deadLetterTasks
                },
                Workers = new
                {
                    Active = activeWorkers
                },
                Coordinators = new
                {
                    Active = activeCoordinators
                },
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Detailed health check failed");
            return StatusCode(500, new { Error = ex.Message });
        }
    }
}
