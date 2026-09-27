using System.Diagnostics;
using EcomAPI.Caching;
using EcomAPI.Data;
using EcomAPI.Handlers;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.Options;

public class GetOrderByIdQueryHandler : IRequestHandler<GetOrderByIdQuery, OrderDto?>
{
    private readonly ReadDbContext _context;
    private readonly HybridCache _cache;
    private readonly HybridCacheEntryOptions _entryOptions;

    public GetOrderByIdQueryHandler(ReadDbContext context, HybridCache cache, IOptions<CacheOptions> cacheOptions)
    {
        _context = context;
        _cache = cache;
        _entryOptions = new HybridCacheEntryOptions
        {
            Expiration = TimeSpan.FromSeconds(cacheOptions.Value.OrderTtlSeconds),
            LocalCacheExpiration = TimeSpan.FromSeconds(cacheOptions.Value.LocalTtlSeconds)
        };
    }

    public async Task<OrderDto?> Handle(GetOrderByIdQuery request, CancellationToken cancellationToken)
    {
        var key = CacheKeys.Order(request.OrderId);

        var cacheHit = true;
        var order = await _cache.GetOrCreateAsync(
            key,
            async ct =>
            {
                cacheHit = false;
                return await LoadOrderAsync(request.OrderId, ct);
            },
            _entryOptions,
            cancellationToken: cancellationToken);
        Activity.Current?.SetTag("cache.hit", cacheHit);

        // Don't keep "not found": the read model is eventually consistent, so the order may be projected any moment.
        if (order == null)
            await _cache.RemoveAsync(key, cancellationToken);

        return order;
    }

    private async Task<OrderDto?> LoadOrderAsync(int orderId, CancellationToken cancellationToken)
    {
        var order = await _context.Orders
            .AsNoTracking()
            .FirstOrDefaultAsync(o => o.Id == orderId, cancellationToken);

        if (order == null)
            return null;

        return new OrderDto(
            order.Id,
            order.FirstName,
            order.LastName,
            order.Status,
            order.CreatedAt,
            order.TotalCost
        );
    }
}
