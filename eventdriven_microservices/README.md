# Event-Driven Microservices with Kafka (Confluent)

An e-commerce order saga built with Spring Boot and Confluent Platform Kafka (KRaft, no ZooKeeper), using choreography and JSON events.

```
POST /orders ──► order-service ──orders.created──► payment-service
                      ▲                               │ payments.completed / payments.failed
                      │                               ▼
                      │                         inventory-service ── inventory.reserved / inventory.rejected
                      │                               │
   status updates ◄───┴───────────────────────────────┴──► notification-service (email log)
```

| Service | Role |
|---|---|
| order-service (:8080) | REST API; publishes `orders.created`; tracks status from downstream events |
| payment-service | Declines amounts > 1000 (`payment.limit`); refunds on `inventory.rejected` (compensation) |
| inventory-service | In-memory stock: `BOOK-1`=10, `PHONE-1`=2, `LAPTOP-1`=0 |
| notification-service | Logs customer notifications for final outcomes |
| common | Event records, topic names, shared Kafka config (topic creation, retry, dead-letter) |

Design notes: events are keyed by `orderId` (per-order ordering), producers use `acks=all` + idempotence, failing consumers retry 3x then publish to `<topic>.DLT`, deserialization errors are handled via `ErrorHandlingDeserializer`. State is in memory for simplicity.

## Run

Prereqs: Docker, Java 17+, Maven.

```bash
docker compose up -d                 # Kafka + Control Center (http://localhost:9021)
mvn clean package -DskipTests
# in four terminals (or background them):
java -jar order-service/target/order-service-1.0.0.jar
java -jar payment-service/target/payment-service-1.0.0.jar
java -jar inventory-service/target/inventory-service-1.0.0.jar
java -jar notification-service/target/notification-service-1.0.0.jar
./demo.sh
```

Or run everything in containers: `docker compose --profile apps up -d --build`.

## Try it manually with curl

```bash
# 1. Success (in stock, amount <= 1000)
curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"c1","sku":"BOOK-1","quantity":2,"amount":40}'

# 2. Payment failure (amount > 1000)
curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"c2","sku":"BOOK-1","quantity":1,"amount":5000}'

# 3. Inventory failure (LAPTOP-1 has no stock -> refund)
curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"c3","sku":"LAPTOP-1","quantity":1,"amount":900}'

# Check an order's status using the orderId returned above
curl -s localhost:8080/orders/<orderId>
```

The POST returns `202` with status `PENDING`; a second later the GET shows `CONFIRMED` or `CANCELLED`. Invalid input (quantity/amount <= 0) returns `400`. Watch the notification-service console for the simulated emails, and browse topics/messages in Control Center at http://localhost:9021.

Expected demo results: order 1 `CONFIRMED`, order 2 `CANCELLED` (payment failed), order 3 `CANCELLED` (inventory rejected, refunded).

Stop: `docker compose --profile apps down`.
