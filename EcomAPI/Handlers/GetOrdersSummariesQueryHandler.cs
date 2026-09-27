using System.Diagnostics;
using EcomAPI.Caching;
using EcomAPI.Data;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.Options;

namespace EcomAPI.Handlers
{
    public class GetOrdersSummariesQueryHandler : IRequestHandler<GetOrdersSummariesQuery, List<OrderSummaryDto>>
    {
        public readonly ReadDbContext _context;
        private readonly HybridCache _cache;
        private readonly HybridCacheEntryOptions _entryOptions;

        public GetOrdersSummariesQueryHandler(ReadDbContext context, HybridCache cache, IOptions<CacheOptions> cacheOptions)
        {
            _context = context;
            _cache = cache;
            _entryOptions = new HybridCacheEntryOptions
            {
                Expiration = TimeSpan.FromSeconds(cacheOptions.Value.SummariesTtlSeconds),
                LocalCacheExpiration = TimeSpan.FromSeconds(cacheOptions.Value.LocalTtlSeconds)
            };
        }

        public async Task<List<OrderSummaryDto>> Handle(GetOrdersSummariesQuery request, CancellationToken cancellationToken)
        {
            var cacheHit = true;
            var summaries = await _cache.GetOrCreateAsync(
                CacheKeys.OrderSummaries,
                async ct =>
                {
                    cacheHit = false;
                    return await _context.Orders
                        .AsNoTracking()
                        .Select(o => new OrderSummaryDto(
                            o.Id,
                            o.FirstName + " " + o.LastName,
                            o.Status,
                            o.TotalCost
                        )).ToListAsync(ct);
                },
                _entryOptions,
                cancellationToken: cancellationToken);
            Activity.Current?.SetTag("cache.hit", cacheHit);

            return summaries;
        }
    }
}
