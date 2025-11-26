using Microsoft.EntityFrameworkCore;
using DistributedTaskProcessor.Infrastructure.Data;
using DistributedTaskProcessor.Shared.Models;

namespace DistributedTaskProcessor.Coordinator.Services;

/// <summary>
/// Service to fetch raw data from source and create source data records
/// </summary>
public interface IDataFetcherService
{
    Task<SourceData> FetchDataAsync(
        Guid tenantId,
        string fund,
        string symbol,
        DateTime fromDate,
        DateTime toDate,
        CancellationToken cancellationToken = default);
}

public class DataFetcherService : IDataFetcherService
{
    private readonly TaskDbContext _dbContext;
    private readonly ILogger<DataFetcherService> _logger;

    public DataFetcherService(TaskDbContext dbContext, ILogger<DataFetcherService> logger)
    {
        _dbContext = dbContext;
        _logger = logger;
    }

    public async Task<SourceData> FetchDataAsync(
        Guid tenantId,
        string fund,
        string symbol,
        DateTime fromDate,
        DateTime toDate,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation(
                "Starting data fetch for tenant {TenantId}, fund: {Fund}, symbol: {Symbol}, date range: {FromDate} to {ToDate}",
                tenantId, fund, symbol, fromDate, toDate);

            // Count rows matching criteria
            // NOTE: Assuming you have a Tasks table with source data
            var totalRows = await _dbContext.Tasks
                .Where(t => t.Symbol == symbol)
                .CountAsync(cancellationToken);

            if (totalRows == 0)
            {
                _logger.LogWarning(
                    "No rows found for tenant {TenantId}, fund: {Fund}, symbol: {Symbol}",
                    tenantId, fund, symbol);
            }

            // Create source data record
            var sourceData = new SourceData
            {
                SourceDataId = Guid.NewGuid(),
                TenantId = tenantId,
                Fund = fund,
                Symbol = symbol,
                RunDate = DateTime.UtcNow.Date,
                TotalRows = totalRows,
                ProcessedRows = 0,
                Status = "Pending",
                CreatedAt = DateTime.UtcNow
            };

            _logger.LogInformation(
                "Data fetch completed. SourceDataId: {SourceDataId}, TotalRows: {TotalRows}",
                sourceData.SourceDataId, sourceData.TotalRows);

            return sourceData;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching data for tenant {TenantId}", tenantId);
            throw;
        }
    }
}
