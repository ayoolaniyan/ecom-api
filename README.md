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
    WriteDB[(Write Database)]
    EventBus[(Event Dispatcher)]
    Projections[Projection Handlers]
    ReadDB[(Read Database)]

    Client --> API
    API --> Commands
    API --> Queries

    Commands --> WriteDB
    Commands --> EventBus

    EventBus --> Projections
    Projections --> ReadDB

    Queries --> ReadDB
```

### Architecture Highlights

-   **Commands** modify system state
-   **Events** are emitted after successful writes
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
    participant Event
    participant Projection
    participant ReadDB

    Client->>API: Create Product Request
    API->>CommandHandler: Execute Command
    CommandHandler->>WriteDB: Save Product
    WriteDB-->>CommandHandler: Success
    CommandHandler->>Event: Publish ProductCreatedEvent
    Event->>Projection: Trigger Projection
    Projection->>ReadDB: Update Read Model
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
    Dispatcher[Event Dispatcher]
    Projection[Projection Handler]
    ReadDB[(Read Database)]
    Query[Query Request]

    Command --> Handler
    Handler --> WriteDB
    Handler --> Event
    Event --> Dispatcher
    Dispatcher --> Projection
    Projection --> ReadDB
    Query --> ReadDB
```

### Event Flow Explained

1.  A **command** changes application state.
2.  The **write database** is updated.
3.  A **domain event** is created.
4.  The **event dispatcher** publishes the event.
5.  **Projections** update read models.
6.  Clients query the **read database**.

------------------------------------------------------------------------

# Project Structure

    eComAPI
    │
    ├── Commands
    ├── Queries
    ├── Events
    ├── Handlers
    ├── Projections
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

------------------------------------------------------------------------

# Running with Docker

The API ships with a multi-stage `Dockerfile` and a `docker-compose.yml`.
SQLite databases are stored on a named volume (`ecom-data`) so data
survives container restarts.

## Start the container

``` bash
docker compose up --build -d
```

The API is available at `http://localhost:5025`. On first start the
schema is created in the empty volume via EF Core migrations.

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
docker compose down -v        # stop and delete the database volume
```

## Configuration

| Variable | Default (in container) |
|----------|------------------------|
| `ASPNETCORE_ENVIRONMENT` | `Production` |
| `ConnectionStrings__WriteDbConnection` | `Data Source=/app/data/Write.db` |
| `ConnectionStrings__ReadDbConnection` | `Data Source=/app/data/Read.db` |

The container listens on port `8080` and runs as a non-root user.

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
-   Kafka or RabbitMQ event streaming
-   Redis distributed caching
-   Kubernetes deployment
-   distributed tracing
-   observability stack
