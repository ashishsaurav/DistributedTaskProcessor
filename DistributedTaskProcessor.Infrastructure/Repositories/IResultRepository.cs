using DistributedTaskProcessor.Shared.Models;

namespace DistributedTaskProcessor.Infrastructure.Repositories;

public interface IResultRepository
{
    Task BulkInsertResultsAsync(List<CalculatedResult> results, CancellationToken cancellationToken = default);
    Task<int> GetResultCountByTaskIdAsync(Guid taskId, CancellationToken cancellationToken = default);
    Task<List<CalculatedResult>> GetResultsByTaskIdAsync(Guid taskId, CancellationToken cancellationToken = default);
}
