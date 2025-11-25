using Microsoft.EntityFrameworkCore;
using DistributedTaskProcessor.Shared.Models;
using System.Collections.Generic;
using System.Reflection.Emit;

namespace DistributedTaskProcessor.Infrastructure.Data;

public class TaskDbContext : DbContext
{
    public TaskDbContext(DbContextOptions<TaskDbContext> options) : base(options) { }

    public DbSet<SourceMarketData> SourceMarketData { get; set; } = null!;
    public DbSet<CalculatedResult> CalculatedResults { get; set; } = null!;
    public DbSet<TaskEntity> Tasks { get; set; } = null!;
    public DbSet<WorkerHeartbeat> WorkerHeartbeats { get; set; } = null!;
    public DbSet<CoordinatorState> CoordinatorStates { get; set; } = null!;
    public DbSet<AuditLog> AuditLogs { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        // SourceMarketData
        modelBuilder.Entity<SourceMarketData>(entity =>
        {
            entity.ToTable("SourceMarketData");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).ValueGeneratedOnAdd();
        });

        // CalculatedResults
        modelBuilder.Entity<CalculatedResult>(entity =>
        {
            entity.ToTable("CalculatedResults");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).ValueGeneratedOnAdd();
        });

        // TaskEntity
        modelBuilder.Entity<TaskEntity>(entity =>
        {
            entity.ToTable("Tasks");
            entity.HasKey(t => t.TaskId);
            entity.HasIndex(t => new { t.Status, t.CreatedAt });
            entity.HasIndex(t => new { t.Symbol, t.Fund, t.RunDate });
        });

        // WorkerHeartbeat
        modelBuilder.Entity<WorkerHeartbeat>(entity =>
        {
            entity.ToTable("WorkerHeartbeats");
            entity.HasKey(w => w.Id);
            entity.HasIndex(w => w.WorkerId).IsUnique();
        });

        // CoordinatorState
        modelBuilder.Entity<CoordinatorState>(entity =>
        {
            entity.ToTable("CoordinatorStates");
            entity.HasKey(c => c.Id);
            entity.HasIndex(c => new { c.IsLeader, c.LastHeartbeat });
        });

        // AuditLog
        modelBuilder.Entity<AuditLog>(entity =>
        {
            entity.ToTable("AuditLogs");
            entity.HasKey(a => a.Id);
            entity.HasIndex(a => new { a.TaskId, a.Timestamp });
        });
    }
}
