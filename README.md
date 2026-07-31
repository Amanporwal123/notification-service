# Event-Driven Notification Service

A scalable, event-driven notification microservice written in Go. This service decouples HTTP API requests from the slow process of delivering notifications (Email/SMS) by utilizing **Apache Kafka** for asynchronous processing and background workers.

## Architecture Overview

The application runs as a unified binary containing two primary components running concurrently:

1. **HTTP API Server (Producer):** Receives incoming HTTP POST requests to create notifications. It saves the initial `PENDING` state to PostgreSQL and publishes the event to a Kafka topic. It responds instantly to the client.
2. **Background Worker (Consumer):** A concurrent goroutine that continuously listens to the Kafka topic. When it receives a message, it processes the notification via third-party providers (SendGrid, Twilio) and updates the final status (`SENT` or `FAILED`) in the database.

### Tech Stack
*   **Language:** Go (Golang)
*   **Database:** PostgreSQL (with GORM)
*   **Message Broker:** Apache Kafka (with Zookeeper)
*   **Web Framework:** Gin
*   **Providers:** SendGrid (Email), Twilio (SMS)

---

## Prerequisites

*   [Docker & Docker Compose](https://docs.docker.com/get-docker/) (for running Postgres and Kafka)
*   [Go 1.20+](https://golang.org/doc/install)
*   A SendGrid API Key (for sending emails)
*   A Twilio Account SID & Auth Token (for sending SMS)

---

## Getting Started

### 1. Configure the Application
First, ensure you have your API keys ready. Create a local copy of your configuration to avoid committing secrets:
```bash
cp config.yaml config.local.yaml
```
Update your `config.yaml` (or `config.local.yaml`) with your actual SendGrid and Twilio credentials.

### 2. Start Infrastructure (Docker)
Start the PostgreSQL database and Kafka/Zookeeper message brokers using Docker Compose:
```bash
docker-compose up -d
```
*Note: Kafka takes about 15-20 seconds to fully initialize.*

### 3. Run the Application
Start the Go application. This single command boots up both the HTTP API and the Kafka Background Worker:
```bash
go run cmd/api/main.go
```

---

## Testing the API

You can test the notification flow by sending a `POST` request to the API. The API will respond almost instantly, and the background worker will process the email asynchronously.

```bash
curl -X POST http://localhost:8080/api/v1/notifications \
-H "Content-Type: application/json" \
-d '{
    "type": "EMAIL",
    "recipient": "your_email@gmail.com",
    "content": "Hello from the Kafka Background Worker!"
}'
```

### Expected Flow:
1. You receive a `201 Created` HTTP response within milliseconds.
2. The terminal logs `Successfully published event to Kafka`.
3. The terminal logs `Processing notification...` as the worker picks it up.
4. The worker formats the email with a beautiful HTML template and sends it via SendGrid.
5. The database record is updated to `SENT`.

---

## Project Structure

```text
├── cmd/
│   └── api/                # Main entrypoint (Boots API & Worker)
├── internal/
│   ├── api/                # Gin Router setup
│   ├── config/             # YAML Configuration loading
│   ├── constants/          # System-wide constants (Statuses, Types)
│   ├── handler/            # HTTP Handlers / Controllers
│   ├── model/              # GORM Database Models
│   ├── provider/           # Third-party integrations (SendGrid/Twilio)
│   ├── repository/         # Database access layer
│   ├── service/            # Core business logic
│   └── worker/             # Kafka background consumer processor
├── pkg/
│   ├── kafka/              # Kafka Producer & Consumer implementations
│   └── logger/             # Zap structured logger
├── config.yaml             # Application configuration
└── docker-compose.yml      # Infrastructure (Postgres, Kafka, Zookeeper)
```

## Graceful Shutdown
The service implements a robust graceful shutdown mechanism. Upon receiving an interrupt signal (`CTRL+C`), the HTTP server stops accepting new connections, and the Kafka consumer safely breaks its listening loop and commits its final offsets before the process exits, ensuring zero data loss.
