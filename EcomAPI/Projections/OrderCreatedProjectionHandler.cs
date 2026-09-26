using EcomAPI.Data;
using EcomAPI.Events;
using EcomAPI.Models;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace EcomAPI.Projections
{
    public class OrderCreatedProjectionHandler : INotificationHandler<OrderCreatedEvent>
    {
        private readonly ReadDbContext _context;
        public OrderCreatedProjectionHandler(ReadDbContext context)
        {
            _context = context;
        }

        public async Task Handle(OrderCreatedEvent notification, CancellationToken cancellationToken)
        {
            // Events are delivered at least once; ignore a redelivery of an order already projected.
            if (await _context.Orders.AnyAsync(o => o.Id == notification.OrderId, cancellationToken))
                return;

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
    }
}
