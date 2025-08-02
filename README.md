# Go Kafka Producer Consumer

A simple Go implementation of Kafka producer and consumer using the Confluent Kafka Go client.

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

## Usage

### Running the Example

The main.go file demonstrates both producer and consumer functionality:

```bash
go run .
```

This will:
- Create a producer and consumer
- Subscribe to the "test-topic"
- Produce 5 sample messages
- Consume and process the messages

### Producer

```go
// Create producer
producer, err := NewKafkaProducer(ProducerConfig{
    Brokers: []string{"localhost:9092"},
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

err = producer.Produce("my-topic", message)
if err != nil {
    log.Printf("Failed to produce message: %v", err)
}
```

### Consumer

```go
// Create consumer
consumer, err := NewKafkaConsumer(ConsumerConfig{
    Brokers:    []string{"localhost:9092"},
    GroupID:    "my-consumer-group",
    AutoCommit: false,
})
if err != nil {
    log.Fatalf("Failed to create consumer: %v", err)
}
defer consumer.Close()

// Subscribe to topics
err = consumer.Subscribe([]string{"my-topic"})
if err != nil {
    log.Fatalf("Failed to subscribe: %v", err)
}

// Consume messages
err = consumer.Consume(func(message []byte) error {
    log.Printf("Received message: %s", string(message))
    return nil
})
```

## Configuration

### Producer Configuration

```go
type ProducerConfig struct {
    Brokers []string // List of Kafka brokers
}
```

### Consumer Configuration

```go
type ConsumerConfig struct {
    Brokers    []string // List of Kafka brokers
    GroupID    string   // Consumer group ID
    AutoCommit bool     // Enable/disable auto commit
}
```

## Features

- **Producer:**
  - JSON message serialization
  - Delivery report handling
  - Error handling and logging
  - Configurable broker list

- **Consumer:**
  - Topic subscription
  - Configurable consumer groups
  - Manual and auto commit options
  - Message processing with error handling
  - Graceful shutdown

## Running Kafka Locally

If you need to run Kafka locally for testing:

### Using Docker Compose

Create a `docker-compose.yml` file:

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - 9092:9092
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

Start Kafka:
```bash
docker-compose up -d
```

### Create a Test Topic

```bash
# Using Kafka CLI tools
kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Or using Docker
docker exec -it <kafka-container-id> kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Dependencies

- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) - Confluent's Apache Kafka Go client

## License

This project is open source and available under the [MIT License](LICENSE).