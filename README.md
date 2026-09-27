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
-   Redis distributed caching of read queries (HybridCache)
-   Distributed tracing with OpenTelemetry, across the outbox and Kafka
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
    Cache[(HybridCache<br/>memory L1 + Redis L2)]
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

    Queries --> Cache
    Cache -->|miss| ReadDB
    Projections -.->|invalidate| Cache
```

### Architecture Highlights

-   **Commands** modify system state
-   **Events** are stored in an outbox in the same transaction as the write,
    then streamed through **Kafka**
-   **Projections** update read models
-   **Queries** read optimized data models, cached in **Redis**

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
    participant Cache as HybridCache (Redis)

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
    Projection->>Cache: Invalidate order + summaries
    Consumer->>Kafka: Commit offset
    Client->>API: Query Orders
    API->>Cache: Get (memory, then Redis)
    alt cache miss
        API->>ReadDB: Fetch Data
        API->>Cache: Store with TTL
    end
    API-->>Client: Return Order List
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
    ├── Caching
    ├── Tracing
    ├── Health
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

    deploy
    ├── helm/ecom-api      Helm chart (API, Strimzi Kafka, Redis)
    └── kind               Local kind cluster config and scripts

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

## Caching

-   Redis
-   HybridCache (`Microsoft.Extensions.Caching.Hybrid`)

## Observability

-   OpenTelemetry (.NET SDK, OTLP exporter)
-   Jaeger

## Messaging

-   Apache Kafka (KRaft mode)
-   Confluent.Kafka client

## DevOps

-   Docker
-   Docker Compose
-   Kubernetes
-   Helm
-   Strimzi (Kafka operator)

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

Likewise, `Cache:Provider = Memory` by default, so queries are cached
in-process only. To use Redis locally:

``` bash
docker compose up -d redis
cd EcomAPI
Cache__Provider=Redis dotnet run
```

Redis is reachable from the host at `localhost:6379`.

Tracing is off by default (`Tracing:Enabled = false`). To send traces
to Jaeger from a local run:

``` bash
docker compose up -d jaeger
cd EcomAPI
Tracing__Enabled=true dotnet run
```

Traces are exported to `localhost:4317` and shown at
`http://localhost:16686`.

------------------------------------------------------------------------

# Running with Docker

The API ships with a multi-stage `Dockerfile` and a `docker-compose.yml`
that runs the API together with a single-node Kafka broker, Redis and
Jaeger.
SQLite databases are stored on a named volume (`ecom-data`), Kafka data on
`kafka-data` and Redis data on `redis-data`, so all survive container
restarts.

## Start the container

``` bash
docker compose up --build -d
```

The API is available at `http://localhost:5025` and the Jaeger UI at
`http://localhost:16686`. It waits for the Kafka
and Redis healthchecks before starting. On first start the schema is created in the
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
docker compose down -v        # stop and delete the database, Kafka and Redis volumes (Jaeger keeps traces in memory only)
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
| `Cache__Provider` | `Redis` (`Memory` outside Docker) |
| `Cache__RedisConnection` | `redis:6379` (`localhost:6379` outside Docker) |
| `Cache__InstanceName` | `ecomapi:` (Redis key prefix) |
| `Cache__OrderTtlSeconds` | `300` |
| `Cache__SummariesTtlSeconds` | `30` |
| `Cache__LocalTtlSeconds` | `10` |
| `Tracing__Enabled` | `true` (`false` outside Docker) |
| `Tracing__OtlpEndpoint` | `http://jaeger:4317` (`http://localhost:4317` outside Docker) |
| `Tracing__ServiceName` | `ecomapi` |
| `Tracing__SamplingRatio` | `1.0` |

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

# Distributed Caching with Redis

Read queries are cached with .NET's **HybridCache**, a two-level cache:

-   **L1** is an in-memory cache inside each API instance.
-   **L2** is **Redis**, shared by all instances (`Cache:Provider = Redis`).
    With `Memory`, only L1 is used and no Redis is needed.

A lookup checks L1, then Redis, then the read database, and stores the
result in both levels. Concurrent misses for the same key are collapsed
into a single database query (stampede protection).

| Key | Query | TTL (Redis / memory) |
|-----|-------|----------------------|
| `order:{id}` | `GET /api/orders/{id}` | 300s / 10s |
| `orders:summaries` | `GET /api/orders` | 30s / 10s |

Keys are stored in Redis with the `ecomapi:` prefix.

-   **Invalidation.** When `OrderCreatedProjectionHandler` projects an
    order, it removes `orders:summaries` and `order:{id}` from the cache,
    so the list shows the new order right away. It also does this on a
    redelivered event, in case an earlier attempt stopped before
    invalidating.
-   **Not-found results are not cached.** Reads are eventually
    consistent, so an order that is not projected yet must show up as soon
    as it is.
-   **Multiple instances.** Removing a key clears Redis for everyone, but
    only the local memory copy of the instance doing the removal. Other
    instances can serve their memory copy for up to `LocalTtlSeconds`
    (10s), so this is kept short.
-   **Redis outages.** If Redis is unreachable, HybridCache logs the
    failure and falls back to memory and the read database; requests keep
    working, only slower (up to about 1–2s on a memory miss because of the
    Redis timeout). An invalidation that fails during an outage is logged,
    and the stale entry expires on its TTL.

## Inspecting the cache

``` bash
# List cached keys
docker exec ecom-redis redis-cli --scan --pattern 'ecomapi:*'

# Remaining lifetime of an entry (seconds)
docker exec ecom-redis redis-cli TTL ecomapi:orders:summaries

# Clear the cache
docker exec ecom-redis redis-cli FLUSHALL
```

------------------------------------------------------------------------

# Distributed Tracing

The API is instrumented with **OpenTelemetry** and exports traces over
OTLP/gRPC. With Docker Compose they go to **Jaeger**. Any OTLP backend
works (OpenTelemetry Collector, Grafana Tempo, a vendor) by changing
`Tracing:OtlpEndpoint`.

A `POST /api/orders` produces **one trace**, even though the event is
published and projected after the response has been sent:

``` mermaid
flowchart TD
    HTTP["POST /api/orders<br/>(ASP.NET Core)"]
    Cmd["CreateOrderCommand<br/>(MediatR)"]
    Insert["INSERT Orders + OutboxMessages<br/>(EF Core)"]
    Dispatch["outbox dispatch"]
    Send["send orders.order-created<br/>(Kafka producer)"]
    Process["process orders.order-created<br/>(Kafka consumer)"]
    Project["project OrderCreatedEvent"]
    ReadDB["SELECT / INSERT read model<br/>(EF Core)"]
    Redis["UNLINK cache keys<br/>(Redis)"]

    HTTP --> Cmd --> Insert
    Cmd -.->|traceparent stored<br/>on the outbox row| Dispatch
    Dispatch --> Send
    Send -.->|traceparent in<br/>Kafka headers| Process
    Process --> Project
    Project --> ReadDB
    Project --> Redis
```

-   **Automatic spans.** ASP.NET Core requests, EF Core commands (write
    and read databases) and Redis commands (when `Cache:Provider = Redis`)
    are instrumented by the OpenTelemetry libraries.
-   **Across the outbox.** `OutboxMessage.Create` stores the current W3C
    `traceparent`/`tracestate` on the outbox row. `OutboxDispatcher`
    starts its `outbox dispatch` span from it, so publishing continues the
    request's trace. A failed publish is recorded on its span, and each
    retry appears in the same trace.
-   **Across Kafka.** `KafkaEventPublisher` creates a producer span and
    injects its context into the message headers (`traceparent`, next to
    `event-type` and `message-id`). `KafkaEventConsumer` extracts it and
    starts a consumer span, tagged with the partition, offset and key.
    Skipped and retried messages are marked on the span. In `InProcess`
    mode the dispatcher calls the projection directly, inside the same
    trace.
-   **Commands and queries.** A MediatR pipeline behavior
    (`TracingBehavior`) wraps each request in a span named after it
    (`CreateOrderCommand`, `GetOrderByIdQuery`, ...). Query spans carry a
    `cache.hit` tag, so you can tell cached reads from database reads.
-   **Noise.** `/healthz/*` requests (and the checks they run) and the
    outbox dispatcher's once-a-second polling queries are not traced.
-   **Logs.** Every log entry includes the `TraceId` and `SpanId` of the
    span it was written in, so a log line can be looked up in Jaeger.
-   **Sampling.** `Tracing:SamplingRatio` samples new traces. Spans that
    continue a trace (from a caller's `traceparent`, an outbox row or a
    Kafka message) follow the decision already made.

With `Tracing:Enabled = false` (the default outside Docker), nothing is
exported and no collector is needed.

## Viewing traces

1.  Create an order (see [Try it out](#try-it-out)).
2.  Open `http://localhost:16686`, pick the service `ecomapi` and click
    **Find Traces**.
3.  Open the `POST /api/orders` trace. Its spans run from the HTTP request
    through the outbox and Kafka to the projection and the cache
    invalidation.

To trace a log line, copy the `TraceId` from the log entry and paste it
into Jaeger's search box.

``` bash
docker compose logs ecomapi | grep TraceId
```

Jaeger keeps traces in memory, so they are lost when its container
restarts.

------------------------------------------------------------------------

# Deploying to Kubernetes

The `deploy/helm/ecom-api` Helm chart deploys the API with Redis and a
Kafka cluster managed by the [Strimzi](https://strimzi.io) operator.

| Component | Kubernetes resources | Notes |
|-----------|----------------------|-------|
| API | Deployment, Service, ConfigMap, Secret, PVC | 1 replica, `Recreate` rollout, SQLite on a persistent volume |
| Kafka | Strimzi `Kafka`, `KafkaNodePool`, `KafkaTopic` | KRaft mode, 3 controllers + 3 brokers, RF 3, `min.insync.replicas` 2 |
| Redis | StatefulSet, Service | AOF persistence, 256 MB LRU |

## Production Kafka with Strimzi

The Strimzi Cluster Operator runs the Kafka cluster from the chart's custom
resources:

-   **Separate node pools.** Three `controller` nodes form the KRaft
    metadata quorum and tolerate one failure. Three `broker` nodes store the
    data. Broker load cannot destabilise the quorum.
-   **Replication.** The `orders.order-created` topic has 3 partitions ×
    3 replicas with `min.insync.replicas=2`. The API's producer uses
    `acks=all` with idempotence, so writes stay durable and available when
    one broker is lost. The chart refuses to render settings that would
    break this, such as a replication factor above the broker count, or
    `min.insync.replicas` ≥ the replication factor.
-   **Topic as code.** The topic is a `KafkaTopic` resource reconciled by
    the Topic Operator. Automatic topic creation is disabled.
-   **Scheduling and storage.** Pods of each pool prefer different nodes
    (`kafka.podAntiAffinity`, can be `required`). Rack awareness across zones
    is available with `kafka.rack.enabled`. Storage is JBOD persistent
    volumes, which are kept when the cluster is deleted.
-   **Listeners.** A plaintext listener (`9092`) is used by the API, and a
    TLS listener (`9093`) is available for other clients.
-   **Rolling updates.** Strimzi handles rolling restarts, upgrades and
    `PodDisruptionBudget`s.

## Health checks

The API exposes two endpoints, used by the pod's probes:

| Endpoint | Probe | Checks |
|----------|-------|--------|
| `/healthz/live` | startup, liveness | Process is serving requests |
| `/healthz/ready` | readiness | Write/read databases (required), Kafka and Redis (`Degraded` only) |

Kafka and Redis only degrade readiness. Writes are buffered in the outbox
and reads fall back to the database, so a broker or cache outage doesn't
take the API out of its Service. `/healthz/ready` returns a JSON report
per check.

## Run locally with kind

Prerequisites: Docker, [kind](https://kind.sigs.k8s.io), `kubectl` and
Helm 3+. Give Docker at least 8 GB of memory.

``` bash
./deploy/kind/up.sh
```

The script:

1.  Creates a kind cluster `ecom` with 1 control plane and 3 workers.
2.  Builds the image as `ecomapi:local` and loads it into the cluster.
3.  Installs the Strimzi operator (namespace `strimzi`, watching `ecom`).
4.  Installs the chart into `ecom` with `values-local.yaml`.
5.  Waits for Kafka and runs `helm test`, which creates an order and reads
    it back from the read model.

`values-local.yaml` still runs 3 Kafka nodes with the same replication
settings. Each node is both controller and broker, and resources are
smaller.

The API is published on `http://localhost:5026` (NodePort `30025`). This
is a different port from Docker Compose's `5025`, so both can run at once.

``` bash
curl http://localhost:5026/healthz/ready
curl -X POST http://localhost:5026/api/orders \
  -H "Content-Type: application/json" \
  -d '{"firstName":"Jane","lastName":"Doe","status":"Pending","totalCost":49.99}'
curl http://localhost:5026/api/orders

kubectl -n ecom get kafka,kafkanodepool,kafkatopic,pods
kubectl -n ecom logs deploy/ecom-api -f

./deploy/kind/down.sh   # delete the cluster and all its data
```

## Deploy to a cluster

``` bash
# 1. Strimzi Cluster Operator (once per cluster)
helm repo add strimzi https://strimzi.io/charts/
helm install strimzi-operator strimzi/strimzi-kafka-operator \
  --version 1.2.0 --namespace strimzi --create-namespace \
  --set 'watchNamespaces={ecom}'

# 2. Push the API image to a registry the cluster can pull from
docker build -t <registry>/ecomapi:1.0.0 EcomAPI
docker push <registry>/ecomapi:1.0.0

# 3. The application
helm install ecom-api deploy/helm/ecom-api \
  --namespace ecom --create-namespace \
  --set api.image.repository=<registry>/ecomapi

helm test ecom-api -n ecom --logs
```

The `ecom` namespace must exist before the operator is installed, because
the operator creates its RoleBindings there. Create it with
`kubectl create namespace ecom`.

Useful values (see `values.yaml` for all of them):

| Value | Default | Purpose |
|-------|---------|---------|
| `api.image.repository` / `tag` | `ecomapi` / appVersion | API image |
| `api.persistence.size` / `storageClass` | `1Gi` / default | SQLite volume |
| `kafka.nodePools` | 3 controllers, 3 brokers | KRaft node pools, sizes, storage, JVM heap |
| `kafka.topic.*` | 3 partitions, RF 3 | Events topic |
| `kafka.enabled` / `kafka.externalBootstrapServers` | `true` / empty | Use an existing Kafka instead of Strimzi |
| `redis.enabled` / `redis.externalConnection` | `true` / empty | Use an existing Redis (stored in a Secret) |
| `tracing.enabled` / `tracing.otlpEndpoint` | `false` / empty | Export traces to an existing OTLP/gRPC collector |
| `tracing.samplingRatio` | `1.0` | Fraction of new traces recorded |

With both Kafka options off, the API falls back to the in-process event
bus. With both Redis options off, it uses the in-memory cache only. The
chart doesn't deploy a tracing backend. Point `tracing.otlpEndpoint` at
one that runs in or outside the cluster, e.g.
`--set tracing.enabled=true --set tracing.otlpEndpoint=http://otel-collector.observability:4317`.

## Limitations

-   **The API runs a single replica.** Its write and read stores are SQLite
    files on a `ReadWriteOnce` volume, and the outbox dispatcher assumes a
    single instance. The chart rejects `api.replicaCount > 1` and rolls out
    with `Recreate`. Scaling out would need a server database (e.g.
    PostgreSQL) and a lock or leader election for the outbox dispatcher.
    Kafka and Redis already support multiple instances.
-   **Redis is a single instance.** This is acceptable for a cache, since
    the API keeps working without it. Use Redis Sentinel/Cluster or a
    managed service through `redis.externalConnection` for HA.
-   **The Kafka listener the API uses is plaintext and unauthenticated.** For
    untrusted networks, move the API to the TLS listener with SCRAM or mTLS
    `KafkaUser`s. This needs security settings in the Kafka client
    configuration.

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
-   ~~Redis distributed caching~~ ✅ (see [Distributed Caching with Redis](#distributed-caching-with-redis))
-   ~~Kubernetes deployment~~ ✅ (see [Deploying to Kubernetes](#deploying-to-kubernetes))
-   ~~distributed tracing~~ ✅ (see [Distributed Tracing](#distributed-tracing))
-   observability stack
