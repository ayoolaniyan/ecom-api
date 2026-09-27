using EcomAPI.Caching;
using EcomAPI.Commands;
using EcomAPI.Data;
using EcomAPI.Events;
using EcomAPI.Handlers;
using EcomAPI.Health;
using EcomAPI.Outbox;
using EcomAPI.Projections;
using FluentValidation;
using MediatR;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// builder.Services.AddDbContext<AppDbContext>(opt => opt.UseSqlite(builder.Configuration.GetConnectionString("BaseConnection")));
builder.Services.AddDbContext<WriteDbContext>(opt => opt.UseSqlite(builder.Configuration.GetConnectionString("WriteDbConnection")));
builder.Services.AddDbContext<ReadDbContext>(opt => opt.UseSqlite(builder.Configuration.GetConnectionString("ReadDbConnection")));

// Checks tagged "ready" back /healthz/ready. The databases are required; Kafka and Redis only report
// Degraded, since the outbox buffers writes and reads fall back to the database while they are down.
var healthChecks = builder.Services.AddHealthChecks()
    .AddDbContextCheck<WriteDbContext>("write-db", tags: ["ready"])
    .AddDbContextCheck<ReadDbContext>("read-db", tags: ["ready"]);

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
    builder.Services.AddSingleton<KafkaHealthCheck>();
    healthChecks.AddCheck<KafkaHealthCheck>("kafka", HealthStatus.Degraded, ["ready"]);
}
else
{
    builder.Services.AddSingleton<IEventPublisher, InProcessEventPublisher>();
}
builder.Services.AddHostedService<OutboxDispatcher>();

// Cache:Provider selects the HybridCache L2: "Redis" (shared across instances) or "Memory" (in-process L1 only).
var cacheSection = builder.Configuration.GetSection(CacheOptions.SectionName);
builder.Services.Configure<CacheOptions>(cacheSection);
var cacheOptions = cacheSection.Get<CacheOptions>() ?? new CacheOptions();
if (cacheOptions.Provider.Equals("Redis", StringComparison.OrdinalIgnoreCase))
{
    // HybridCache picks up the registered IDistributedCache as its L2.
    builder.Services.AddStackExchangeRedisCache(opt =>
    {
        var redis = ConfigurationOptions.Parse(cacheOptions.RedisConnection);
        // Start and keep serving from the database if Redis is unreachable; fail fast instead of blocking requests.
        redis.AbortOnConnectFail = false;
        redis.ConnectTimeout = 2000;
        redis.AsyncTimeout = 1000;
        redis.SyncTimeout = 1000;
        opt.ConfigurationOptions = redis;
        opt.InstanceName = cacheOptions.InstanceName;
    });
    healthChecks.AddCheck<DistributedCacheHealthCheck>("redis", HealthStatus.Degraded, ["ready"]);
}
builder.Services.AddHybridCache(opt =>
{
    opt.DefaultEntryOptions = new HybridCacheEntryOptions
    {
        Expiration = TimeSpan.FromSeconds(cacheOptions.OrderTtlSeconds),
        LocalCacheExpiration = TimeSpan.FromSeconds(cacheOptions.LocalTtlSeconds)
    };
});
// builder.Services.AddScoped<IEventHandler<OrderCreatedEvent>, OrderCreatedProjectionHandler>();

builder.Services.AddMediatR(cfg => cfg.RegisterServicesFromAssembly(typeof(Program).Assembly));

var app = builder.Build();

// Apply pending migrations so a fresh database (e.g. an empty Docker volume) gets its schema.
using (var scope = app.Services.CreateScope())
{
    scope.ServiceProvider.GetRequiredService<WriteDbContext>().Database.Migrate();
    scope.ServiceProvider.GetRequiredService<ReadDbContext>().Database.Migrate();
}

// Liveness only proves the process is serving requests; readiness also checks the dependencies.
app.MapHealthChecks("/healthz/live", new HealthCheckOptions { Predicate = _ => false });
app.MapHealthChecks("/healthz/ready", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("ready"),
    ResponseWriter = HealthCheckResponseWriter.WriteAsync
});

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
