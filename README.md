# Go Kafka Producer Consumer

A Go implementation of Kafka producer and consumer using the Confluent Kafka Go client with structured logging and modern Go practices.

## Prerequisites

- Go 1.24.5 or later
- Apache Kafka cluster running (default: localhost:9092)
- librdkafka C library (required by confluent-kafka-go)

### Installing librdkafka

**macOS:**
```bash
brew install librdkafka
```

**Ubuntu/Debian:**
```bash
sudo apt-get install librdkafka-dev
```

**CentOS/RHEL:**
```bash
sudo yum install librdkafka-devel
```

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd go-kafka-producer-consumer
```

2. Install dependencies:
```bash
go mod tidy
```

## Project Structure

```
├── producer.go              # Producer implementation
├── consumer.go              # Consumer implementation  
├── main.go                  # Usage example
├── docker-compose.yml       # Kafka cluster setup
└── README.md               # This file
```

## Usage

### Quick Start

Run the producer and consumer example:

```bash
go run .
```

This will:
- Create a producer and consumer
- Subscribe to the "test-topic" 
- Produce 5 sample messages
- Consume and process the messages

## Implementation

### Producer

```go
// Create producer
producer, err := NewKafkaProducer(ProducerConfig{
    Brokers:  "localhost:9092",
    ClientId: "my-producer",
})
if err != nil {
    log.Fatalf("Failed to create producer: %v", err)
}
defer producer.Close()

// Produce a message
message := map[string]interface{}{
    "id": "msg-1",
    "content": "Hello Kafka!",
}

err = producer.Produce("my-topic", "msg-key", message)
if err != nil {
    log.Printf("Failed to produce message: %v", err)
}
```

### Consumer

```go
// Create consumer
consumer, err := NewKafkaConsumer(ConsumerConfig{
    Brokers:            "localhost:9092",
    GroupID:            "my-consumer-group",
    ClientID:           "my-consumer",
    EnableAutoCommit:   false,
    AutoCommitInterval: 5000,
    BatchSize:          10,
    BatchTimeOut:       5 * time.Second,
})
if err != nil {
    log.Fatalf("Failed to create consumer: %v", err)
}
defer consumer.Close()

// Consume messages individually
err = consumer.Consume("my-topic", func(key, value []byte) error {
    log.Printf("Received message: key=%s, value=%s", string(key), string(value))
    return nil
})

// Or consume messages in batches
err = consumer.ConsumeBatch("my-topic", func(messages []Message) error {
    log.Printf("Processing batch of %d messages", len(messages))
    for _, msg := range messages {
        // Process each message
        log.Printf("Message from topic %s: %s", msg.Topic, string(msg.Value))
    }
    return nil
})
```

## Configuration

### Producer Configuration

```go
type ProducerConfig struct {
    Brokers  string // Kafka brokers (e.g., "localhost:9092")
    ClientId string // Client identifier
}
```

### Consumer Configuration

```go
type ConsumerConfig struct {
    Brokers            string        // Kafka brokers
    GroupID            string        // Consumer group ID
    ClientID           string        // Client identifier
    EnableAutoCommit   bool          // Enable/disable auto commit
    AutoCommitInterval int           // Auto commit interval in milliseconds
    BatchSize          int           // Batch size for batch processing
    BatchTimeOut       time.Duration // Batch timeout
}
```

## Running Kafka Locally

### Using Docker Compose

A `docker-compose.yml` file is included for easy local development:

```bash
# Start Kafka cluster with Zookeeper and Kafka UI
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f kafka

# Stop services
docker-compose down
```

Services included:
- **Kafka**: Main broker (port 9092)
- **Zookeeper**: Coordination service (port 2181)
- **Kafka UI**: Web interface (http://localhost:8080)

### Create Topics

```bash
# Using Docker
docker exec -it kafka kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# List topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe topic
docker exec -it kafka kafka-topics --describe --topic test-topic --bootstrap-server localhost:9092
```

## Features

### Producer Features
- **Structured Logging**: Uses slog for JSON logging
- **Message Serialization**: Automatic JSON marshaling for complex types
- **Delivery Reports**: Tracks message delivery success/failure
- **Configurable Settings**: Compression, retries, idempotence
- **Error Handling**: Comprehensive error reporting
- **Graceful Shutdown**: Proper message flushing on close

### Consumer Features
- **Individual Processing**: Process messages one by one
- **Batch Processing**: Process messages in configurable batches
- **Manual Commit**: Fine-grained offset control
- **Message Headers**: Access to Kafka message headers
- **Error Handling**: Robust error handling and logging
- **Graceful Shutdown**: Clean consumer shutdown
- **Timeout Handling**: Configurable polling and batch timeouts

## Dependencies

- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) - Confluent's Apache Kafka Go client
- Go standard library packages for concurrency, logging, and JSON handling

## Example

See `main.go` for a complete example that demonstrates:
- Producer and consumer creation
- Message production with different data types
- Individual and batch message consumption
- Proper error handling and logging
- Graceful shutdown

## License

This project is open source and available under the [MIT License](LICENSE).