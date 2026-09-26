using EcomAPI.Commands;
using EcomAPI.Data;
using EcomAPI.Events;
using EcomAPI.Handlers;
using EcomAPI.Outbox;
using EcomAPI.Projections;
using FluentValidation;
using MediatR;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// builder.Services.AddDbContext<AppDbContext>(opt => opt.UseSqlite(builder.Configuration.GetConnectionString("BaseConnection")));
builder.Services.AddDbContext<WriteDbContext>(opt => opt.UseSqlite(builder.Configuration.GetConnectionString("WriteDbConnection")));
builder.Services.AddDbContext<ReadDbContext>(opt => opt.UseSqlite(builder.Configuration.GetConnectionString("ReadDbConnection")));

// builder.Services.AddScoped<ICommandHandler<CreateOrderCommand, OrderDto>, CreateOrderCommandHandler>();
// builder.Services.AddScoped<IQueryHandler<GetOrderByIdQuery, OrderDto>, GetOrderByIdQueryHandler>();
// builder.Services.AddScoped<IQueryHandler<GetOrdersSummariesQuery, List<OrderSummaryDto>>, GetOrdersSummariesQueryHandler>();
builder.Services.AddScoped<IValidator<CreateOrderCommand>, CreateOrderCommandValidator>();

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.SectionName));
builder.Services.Configure<OutboxOptions>(builder.Configuration.GetSection(OutboxOptions.SectionName));

// EventBus:Provider selects how outbox events reach the projections: "Kafka" or "InProcess" (no broker).
var eventBusProvider = builder.Configuration["EventBus:Provider"] ?? "InProcess";
if (eventBusProvider.Equals("Kafka", StringComparison.OrdinalIgnoreCase))
{
    builder.Services.AddSingleton<IEventPublisher, KafkaEventPublisher>();
    builder.Services.AddHostedService<KafkaEventConsumer>();
}
else
{
    builder.Services.AddSingleton<IEventPublisher, InProcessEventPublisher>();
}
builder.Services.AddHostedService<OutboxDispatcher>();
// builder.Services.AddScoped<IEventHandler<OrderCreatedEvent>, OrderCreatedProjectionHandler>();

builder.Services.AddMediatR(cfg => cfg.RegisterServicesFromAssembly(typeof(Program).Assembly));

var app = builder.Build();

// Apply pending migrations so a fresh database (e.g. an empty Docker volume) gets its schema.
using (var scope = app.Services.CreateScope())
{
    scope.ServiceProvider.GetRequiredService<WriteDbContext>().Database.Migrate();
    scope.ServiceProvider.GetRequiredService<ReadDbContext>().Database.Migrate();
}

app.MapPost("/api/orders", async (IMediator mediator, CreateOrderCommand command) =>
{
    try
    {
        var createdOrder = await mediator.Send(command);

        if (createdOrder == null)
            return Results.BadRequest("Failed to create an order");

        return Results.Created($"/api/orders{createdOrder.Id}", createdOrder);
        
    }
    catch (ValidationException ex)
    {
        var errors = ex.Errors.Select(e => new { e.PropertyName, e.ErrorMessage });
        return Results.BadRequest(errors);
    }
    
});

app.MapGet("/api/orders/{id}", async (IMediator mediator, int id) =>
{
    var order = await mediator.Send(new GetOrderByIdQuery(id));
    if (order == null)
        return Results.NotFound();

    return Results.Ok(order);
});

// TODOS: Pagination
app.MapGet("/api/orders", async (IMediator mediator) =>
{
    var summaries = await mediator.Send(new GetOrdersSummariesQuery());
    return Results.Ok(summaries);
});

app.Run();
