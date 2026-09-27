using EcomAPI.Data;
using EcomAPI.Events;
using EcomAPI.Handlers;
using EcomAPI.Models;
using EcomAPI.Observability;
using EcomAPI.Outbox;
using FluentValidation;
using MediatR;

public class CreateOrderCommandHandler : IRequestHandler<CreateOrderCommand, OrderDto>
{
    private readonly WriteDbContext _context;
    private readonly IValidator<CreateOrderCommand> _validator;
    public CreateOrderCommandHandler(
        WriteDbContext context, 
        IValidator<CreateOrderCommand> validator)
    {
        _context = context;
        _validator = validator;
    }

    public async Task<OrderDto> Handle(CreateOrderCommand request, CancellationToken cancellationToken)
    {
        var validationResult = await _validator.ValidateAsync(request, cancellationToken);

        if (!validationResult.IsValid)
        {
            throw new ValidationException(validationResult.Errors);
        }

            var order = new Order
            {
                FirstName = request.FirstName,
                LastName =  request.LastName,
                Status = request.Status,
                CreatedAt = DateTime.Now,
                TotalCost = request.TotalCost
            };

        // Save the order and its event atomically; OutboxDispatcher publishes the event afterwards.
        await using var transaction = await _context.Database.BeginTransactionAsync(cancellationToken);

        await _context.Orders.AddAsync(order, cancellationToken);
        await _context.SaveChangesAsync(cancellationToken);

        var orderCreatedEvent = new OrderCreatedEvent
        (
            order.Id,
            order.FirstName,
            order.LastName,
            order.Status,
            order.CreatedAt,
            order.TotalCost
        );

        await _context.OutboxMessages.AddAsync(OutboxMessage.Create(orderCreatedEvent, key: order.Id.ToString()), cancellationToken);
        await _context.SaveChangesAsync(cancellationToken);

        await transaction.CommitAsync(cancellationToken);
        AppMetrics.OrderCreated();

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
