# eComAPI --- Event‑Driven E‑Commerce API with CQRS

A modern **E‑Commerce backend API** demonstrating **CQRS (Command Query
Responsibility Segregation)** and **Event‑Driven Architecture** built
with **.NET / ASP.NET Core**.

This project showcases how to design scalable backend systems by
separating **read and write workloads**, emitting **domain events**, and
projecting optimized **read models**.

------------------------------------------------------------------------

# Key Features

-   CQRS architecture
-   Event‑driven design
-   Kafka event streaming with a transactional outbox
-   Separate read and write databases
-   Event projections
-   Clean architecture principles
-   RESTful API
-   Entity Framework migrations
-   Lightweight SQLite database
-   Docker containerization

------------------------------------------------------------------------

# System Architecture

Below is the high‑level architecture of the system.

``` mermaid
flowchart LR
    Client[Client Application]
    API[ASP.NET Core API]
    Commands[Command Handlers]
    Queries[Query Handlers]
    WriteDB[(Write Database<br/>Orders + Outbox)]
    Dispatcher[Outbox Dispatcher]
    Kafka[(Kafka topic<br/>orders.order-created)]
    Consumer[Kafka Consumer]
    Projections[Projection Handlers]
    ReadDB[(Read Database)]

    Client --> API
    API --> Commands
    API --> Queries

    Commands --> WriteDB
    WriteDB --> Dispatcher
    Dispatcher --> Kafka

    Kafka --> Consumer
    Consumer --> Projections
    Projections --> ReadDB

    Queries --> ReadDB
```

### Architecture Highlights

-   **Commands** modify system state
-   **Events** are stored in an outbox in the same transaction as the write,
    then streamed through **Kafka**
-   **Projections** update read models
-   **Queries** read optimized data models

This separation improves:

-   scalability
-   performance
-   maintainability

------------------------------------------------------------------------

# CQRS Request Flow (Sequence Diagram)

This diagram shows how a typical command flows through the system.

``` mermaid
sequenceDiagram
    participant Client
    participant API
    participant CommandHandler
    participant WriteDB
    participant Dispatcher as OutboxDispatcher
    participant Kafka
    participant Consumer as KafkaEventConsumer
    participant Projection
    participant ReadDB

    Client->>API: Create Order Request
    API->>CommandHandler: Execute Command
    CommandHandler->>WriteDB: Save Order + OutboxMessage (one transaction)
    WriteDB-->>CommandHandler: Success
    CommandHandler-->>Client: 201 Created
    Dispatcher->>WriteDB: Poll unpublished messages
    Dispatcher->>Kafka: Produce OrderCreatedEvent (key = orderId)
    Dispatcher->>WriteDB: Mark message processed
    Kafka->>Consumer: Deliver event
    Consumer->>Projection: Trigger Projection
    Projection->>ReadDB: Update Read Model (idempotent)
    Consumer->>Kafka: Commit offset
    Client->>API: Query Products
    API->>ReadDB: Fetch Data
    ReadDB-->>Client: Return Product List
```

------------------------------------------------------------------------

# Event Lifecycle

The lifecycle of a domain event in the system.

``` mermaid
flowchart TD
    Command[Command Received]
    Handler[Command Handler]
    WriteDB[(Write Database)]
    Event[Domain Event Created]
    Outbox[(Outbox Table)]
    Dispatcher[Outbox Dispatcher]
    Kafka[(Kafka)]
    Consumer[Kafka Consumer]
    Projection[Projection Handler]
    ReadDB[(Read Database)]
    Query[Query Request]

    Command --> Handler
    Handler --> WriteDB
    Handler --> Event
    Event --> Outbox
    Outbox --> Dispatcher
    Dispatcher --> Kafka
    Kafka --> Consumer
    Consumer --> Projection
    Projection --> ReadDB
    Query --> ReadDB
```

### Event Flow Explained

1.  A **command** changes application state.
2.  The **write database** is updated and a **domain event** is written to
    the **outbox table** in the same transaction.
3.  The **outbox dispatcher** publishes pending events to **Kafka**.
4.  The **Kafka consumer** receives the event and runs the **projections**,
    which update read models.
5.  Clients query the **read database**.

Reads are **eventually consistent**: a new order appears in the read
model shortly after the `POST` returns (typically within about a second).

------------------------------------------------------------------------

# Project Structure

    eComAPI
    │
    ├── Commands
    ├── Queries
    ├── Events
    ├── Handlers
    ├── Projections
    ├── Outbox
    ├── Models
    ├── DTOs
    ├── Data
    ├── Migrations
    │
    ├── Dockerfile
    ├── Write.db
    ├── Read.db
    └── NoCQRS.db

    docker-compose.yml

------------------------------------------------------------------------

# Technology Stack

## Backend

-   C#
-   .NET
-   ASP.NET Core

## Architecture

-   CQRS
-   Event‑Driven Architecture
-   Clean Architecture

## Data

-   SQLite
-   Entity Framework Core

## Messaging

-   Apache Kafka (KRaft mode)
-   Confluent.Kafka client

## DevOps

-   Docker
-   Docker Compose

------------------------------------------------------------------------

# Running the Application

## Clone the repository

``` bash
git clone https://github.com/yourusername/eComAPI.git
cd eComAPI
```

## Run the API

``` bash
cd EcomAPI
dotnet run
```

Pending EF Core migrations are applied automatically at startup.

By default `dotnet run` uses `EventBus:Provider = InProcess`, so no
broker is needed: outbox events are dispatched to the projections
in-process. To run locally against Kafka, start only the broker and
switch the provider:

``` bash
docker compose up -d kafka
cd EcomAPI
EventBus__Provider=Kafka dotnet run
```

The broker is reachable from the host at `localhost:9094`.

------------------------------------------------------------------------

# Running with Docker

The API ships with a multi-stage `Dockerfile` and a `docker-compose.yml`
that runs the API together with a single-node Kafka broker. SQLite
databases are stored on a named volume (`ecom-data`) and Kafka data on
`kafka-data`, so both survive container restarts.

## Start the container

``` bash
docker compose up --build -d
```

The API is available at `http://localhost:5025`. It waits for the Kafka
healthcheck before starting. On first start the schema is created in the
empty volume via EF Core migrations, and the `orders.order-created` topic
is created by the API.

## Try it out

``` bash
# Create an order
curl -X POST http://localhost:5025/api/orders \
  -H "Content-Type: application/json" \
  -d '{"firstName":"Jane","lastName":"Doe","status":"Pending","totalCost":49.99}'

# Get an order by id
curl http://localhost:5025/api/orders/1

# List order summaries
curl http://localhost:5025/api/orders
```

## Logs, stop and reset

``` bash
docker compose logs -f        # follow logs
docker compose down           # stop (data is kept)
docker compose down -v        # stop and delete the database and Kafka volumes
```

## Inspecting events

``` bash
# Read every event on the topic (with key and headers)
docker exec ecom-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.order-created \
  --from-beginning --property print.key=true --property print.headers=true

# Show the projection consumer group's offsets and lag
docker exec ecom-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group ecomapi-read-projection
```

## Configuration

| Variable | Default (in container) |
|----------|------------------------|
| `ASPNETCORE_ENVIRONMENT` | `Production` |
| `ConnectionStrings__WriteDbConnection` | `Data Source=/app/data/Write.db` |
| `ConnectionStrings__ReadDbConnection` | `Data Source=/app/data/Read.db` |
| `EventBus__Provider` | `Kafka` (`InProcess` outside Docker) |
| `Kafka__BootstrapServers` | `kafka:9092` (`localhost:9094` outside Docker) |
| `Kafka__Topic` | `orders.order-created` |
| `Kafka__ConsumerGroupId` | `ecomapi-read-projection` |
| `Kafka__TopicPartitions` | `3` |
| `Kafka__TopicReplicationFactor` | `1` |
| `Outbox__PollingIntervalMs` | `1000` |
| `Outbox__BatchSize` | `50` |

The container listens on port `8080` and runs as a non-root user.

------------------------------------------------------------------------

# Event Streaming with Kafka

Events travel from the write side to the read side through Kafka, using
the **transactional outbox** pattern:

-   **Atomic write + event.** `CreateOrderCommandHandler` saves the order
    and an `OutboxMessage` in one database transaction. An event is never
    lost because the broker was down, and never published for a write
    that rolled back.
-   **Outbox dispatcher.** `OutboxDispatcher` polls unpublished messages
    in order and produces them to Kafka (idempotent producer,
    `acks=all`). Failed publishes are retried on the next poll, with the
    attempt count and last error stored on the row.
-   **Partitioning.** Messages are keyed by order id, so all events for an
    order land on the same partition and are consumed in order. The
    event type is carried in an `event-type` header.
-   **Consumer.** `KafkaEventConsumer` dispatches each event to the
    MediatR projection handlers and commits the offset only after they
    succeed. A failing message is retried; an unknown or malformed one is
    logged and skipped.
-   **At-least-once delivery.** An event may be delivered more than once,
    so projections are idempotent (`OrderCreatedProjectionHandler` skips
    orders already in the read database).

New event types must be registered in `Events/EventSerializer.cs`.

The dispatcher assumes a single API instance. Running several replicas
would need row locking or leader election on the outbox.

------------------------------------------------------------------------

# Learning Goals

This project demonstrates:

-   CQRS implementation in .NET
-   Event‑driven design
-   Read/write model separation
-   Event projections
-   scalable backend architecture patterns

------------------------------------------------------------------------

# Possible Improvements

Future improvements may include:

-   ~~Docker containerization~~ ✅ (see [Running with Docker](#running-with-docker))
-   ~~Kafka or RabbitMQ event streaming~~ ✅ (see [Event Streaming with Kafka](#event-streaming-with-kafka))
-   Redis distributed caching
-   Kubernetes deployment
-   distributed tracing
-   observability stack
