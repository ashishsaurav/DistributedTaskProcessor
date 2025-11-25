using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using DistributedTaskProcessor.Shared.Models;
using DistributedTaskProcessor.Infrastructure.Data;

namespace DistributedTaskProcessor.Infrastructure.Repositories;

public class SourceDataRepository : ISourceDataRepository
{
    private readonly TaskDbContext _context;
    private readonly ILogger<SourceDataRepository> _logger;

    public SourceDataRepository(TaskDbContext context, ILogger<SourceDataRepository> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task<List<SourceMarketData>> GetDataRangeAsync(
        string symbol,
        string fund,
        DateTime runDate,
        long startRow,
        long endRow,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // Optimized: Use ID-based filtering instead of Skip/Take for better performance
            // First, get the IDs at the boundaries
            var allIds = await _context.SourceMarketData
                .Where(s => s.Symbol == symbol && s.Fund == fund && s.RunDate == runDate)
                .OrderBy(s => s.Id)
                .Select(s => s.Id)
                .ToListAsync(cancellationToken);

            if (allIds.Count == 0)
                return new List<SourceMarketData>();

            // Get the start and end IDs based on row offsets
            var minId = allIds[(int)Math.Min(startRow, allIds.Count - 1)];
            var maxId = allIds[(int)Math.Min(endRow, allIds.Count - 1)];

            // Now fetch the actual data using ID-based filtering (much faster for large datasets)
            var data = await _context.SourceMarketData
                .Where(s => s.Symbol == symbol && s.Fund == fund && s.RunDate == runDate
                            && s.Id >= minId && s.Id <= maxId)
                .OrderBy(s => s.Id)
                .AsNoTracking()
                .ToListAsync(cancellationToken);

            _logger.LogDebug("Retrieved {Count} rows for {Symbol}/{Fund} (rows {Start}-{End})",
                data.Count, symbol, fund, startRow, endRow);

            return data;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving source data range");
            throw;
        }
    }

    public async Task<Dictionary<string, long>> GetRowCountsBySymbolFundAsync(
    DateTime runDate,
    CancellationToken cancellationToken = default)
    {
        try
        {
            var counts = await _context.SourceMarketData
                .Where(s => s.RunDate.Date == runDate.Date)
                .GroupBy(s => new { s.Symbol, s.Fund })
                .Select(g => new
                {
                    // FIX: Format the key correctly to preserve underscores in fund names
                    Key = g.Key.Symbol + "_" + g.Key.Fund,  // This creates "AAPL_Fund_Alpha"
                    Count = g.LongCount()
                })
                .ToDictionaryAsync(x => x.Key, x => x.Count, cancellationToken);

            _logger.LogInformation("Found {Count} Symbol/Fund combinations for RunDate: {RunDate}",
                counts.Count, runDate.ToString("yyyy-MM-dd"));

            foreach (var entry in counts)
            {
                _logger.LogDebug("  {Key}: {Count} rows", entry.Key, entry.Value);
            }

            return counts;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting row counts by symbol/fund");
            throw;
        }
    }


    public async Task<List<string>> GetDistinctSymbolsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _context.SourceMarketData
                .Select(s => s.Symbol)
                .Distinct()
                .ToListAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting distinct symbols");
            throw;
        }
    }

    public async Task<List<string>> GetDistinctFundsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _context.SourceMarketData
                .Select(s => s.Fund)
                .Distinct()
                .ToListAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting distinct funds");
            throw;
        }
    }
}
