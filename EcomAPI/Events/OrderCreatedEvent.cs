using MediatR;

public record OrderCreatedEvent
(
    int OrderId,
    string FirstName,
    string LastName,
    string Status,
    DateTime CreatedAt,
    decimal TotalCost
) : INotification;
