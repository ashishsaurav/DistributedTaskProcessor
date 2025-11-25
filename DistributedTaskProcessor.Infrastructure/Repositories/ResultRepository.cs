using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using DistributedTaskProcessor.Shared.Models;
using DistributedTaskProcessor.Infrastructure.Data;
using System.Data;

namespace DistributedTaskProcessor.Infrastructure.Repositories;

public class ResultRepository : IResultRepository
{
    private readonly TaskDbContext _context;
    private readonly string _connectionString;
    private readonly ILogger<ResultRepository> _logger;

    public ResultRepository(
        TaskDbContext context,
        IConfiguration configuration,
        ILogger<ResultRepository> logger)
    {
        _context = context;
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new ArgumentNullException("DefaultConnection is not configured");
        _logger = logger;
    }

    public async Task BulkInsertResultsAsync(List<CalculatedResult> results, CancellationToken cancellationToken = default)
    {
        if (results.Count == 0) return;

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            using var bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = "CalculatedResults",
                BatchSize = 1000,
                BulkCopyTimeout = 300
            };

            // Map columns
            bulkCopy.ColumnMappings.Add("TaskId", "TaskId");
            bulkCopy.ColumnMappings.Add("SourceId", "SourceId");
            bulkCopy.ColumnMappings.Add("Symbol", "Symbol");
            bulkCopy.ColumnMappings.Add("Fund", "Fund");
            bulkCopy.ColumnMappings.Add("RunDate", "RunDate");
            bulkCopy.ColumnMappings.Add("Price", "Price");
            bulkCopy.ColumnMappings.Add("Volume", "Volume");
            bulkCopy.ColumnMappings.Add("MovingAverage", "MovingAverage");
            bulkCopy.ColumnMappings.Add("VolumeWeightedPrice", "VolumeWeightedPrice");
            bulkCopy.ColumnMappings.Add("PriceChange", "PriceChange");
            bulkCopy.ColumnMappings.Add("VolatilityIndex", "VolatilityIndex");
            bulkCopy.ColumnMappings.Add("TrendIndicator", "TrendIndicator");
            bulkCopy.ColumnMappings.Add("CreatedAt", "CreatedAt");

            var dataTable = ConvertToDataTable(results);
            await bulkCopy.WriteToServerAsync(dataTable, cancellationToken);

            _logger.LogInformation("Bulk inserted {Count} calculated results", results.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error bulk inserting results");
            throw;
        }
    }

    private DataTable ConvertToDataTable(List<CalculatedResult> results)
    {
        var table = new DataTable();

        table.Columns.Add("TaskId", typeof(Guid));
        table.Columns.Add("SourceId", typeof(long));
        table.Columns.Add("Symbol", typeof(string));
        table.Columns.Add("Fund", typeof(string));
        table.Columns.Add("RunDate", typeof(DateTime));
        table.Columns.Add("Price", typeof(decimal));
        table.Columns.Add("Volume", typeof(long));
        table.Columns.Add("MovingAverage", typeof(decimal));
        table.Columns.Add("VolumeWeightedPrice", typeof(decimal));
        table.Columns.Add("PriceChange", typeof(decimal));
        table.Columns.Add("VolatilityIndex", typeof(decimal));
        table.Columns.Add("TrendIndicator", typeof(string));
        table.Columns.Add("CreatedAt", typeof(DateTime));

        foreach (var result in results)
        {
            table.Rows.Add(
                result.TaskId,
                result.SourceId,
                result.Symbol,
                result.Fund,
                result.RunDate,
                result.Price,
                result.Volume,
                result.MovingAverage ?? (object)DBNull.Value,
                result.VolumeWeightedPrice ?? (object)DBNull.Value,
                result.PriceChange ?? (object)DBNull.Value,
                result.VolatilityIndex ?? (object)DBNull.Value,
                result.TrendIndicator ?? (object)DBNull.Value,
                result.CreatedAt
            );
        }

        return table;
    }

    public async Task<int> GetResultCountByTaskIdAsync(Guid taskId, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _context.CalculatedResults
                .CountAsync(r => r.TaskId == taskId, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting result count for task {TaskId}", taskId);
            throw;
        }
    }

    public async Task<List<CalculatedResult>> GetResultsByTaskIdAsync(Guid taskId, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _context.CalculatedResults
                .Where(r => r.TaskId == taskId)
                .AsNoTracking()
                .ToListAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting results for task {TaskId}", taskId);
            throw;
        }
    }
}
