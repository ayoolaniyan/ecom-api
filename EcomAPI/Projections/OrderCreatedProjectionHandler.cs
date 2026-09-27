using System.Diagnostics;
using EcomAPI.Caching;
using EcomAPI.Data;
using EcomAPI.Events;
using EcomAPI.Models;
using EcomAPI.Tracing;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Hybrid;

namespace EcomAPI.Projections
{
    public class OrderCreatedProjectionHandler : INotificationHandler<OrderCreatedEvent>
    {
        private readonly ReadDbContext _context;
        private readonly HybridCache _cache;
        private readonly ILogger<OrderCreatedProjectionHandler> _logger;

        public OrderCreatedProjectionHandler(ReadDbContext context, HybridCache cache, ILogger<OrderCreatedProjectionHandler> logger)
        {
            _context = context;
            _cache = cache;
            _logger = logger;
        }

        public async Task Handle(OrderCreatedEvent notification, CancellationToken cancellationToken)
        {
            using var activity = Telemetry.Source.StartActivity("project OrderCreatedEvent");
            activity?.SetTag("order.id", notification.OrderId);

            // Events are delivered at least once; ignore a redelivery of an order already projected.
            var alreadyProjected = await _context.Orders.AnyAsync(o => o.Id == notification.OrderId, cancellationToken);
            activity?.SetTag("projection.duplicate", alreadyProjected);

            if (!alreadyProjected)
            {
                var order = new Order
                {
                    Id = notification.OrderId,
                    FirstName = notification.FirstName,
                    LastName = notification.LastName,
                    Status = notification.Status,
                    CreatedAt = notification.CreatedAt,
                    TotalCost = notification.TotalCost
                };

                await _context.Orders.AddAsync(order, cancellationToken);
                await _context.SaveChangesAsync(cancellationToken);
            }

            // Also runs on a redelivery, in case a previous attempt stopped before invalidating.
            await InvalidateCacheAsync(notification.OrderId, cancellationToken);
        }

        private async Task InvalidateCacheAsync(int orderId, CancellationToken cancellationToken)
        {
            try
            {
                await _cache.RemoveAsync(CacheKeys.OrderSummaries, cancellationToken);
                await _cache.RemoveAsync(CacheKeys.Order(orderId), cancellationToken);
            }
            catch (Exception ex)
            {
                Activity.Current?.AddException(ex);
                // The read model is already updated; a stale cache entry expires on its own TTL.
                _logger.LogWarning(ex, "Failed to invalidate cache for order {OrderId}", orderId);
            }
        }
    }
}
