using DistributedTaskProcessor.Shared.Models;

namespace DistributedTaskProcessor.Infrastructure.Repositories;

public interface ISourceDataRepository
{
    Task<List<SourceMarketData>> GetDataRangeAsync(string symbol, string fund, DateTime runDate, long startRow, long endRow, CancellationToken cancellationToken = default);
    Task<Dictionary<string, long>> GetRowCountsBySymbolFundAsync(DateTime runDate, CancellationToken cancellationToken = default);
    Task<List<string>> GetDistinctSymbolsAsync(CancellationToken cancellationToken = default);
    Task<List<string>> GetDistinctFundsAsync(CancellationToken cancellationToken = default);
}
