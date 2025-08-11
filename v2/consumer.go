package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ack struct {
	tp  kafka.TopicPartition
	off kafka.Offset
}

// Consumer wraps the Kafka consumer with additional state management
type Consumer struct {
	consumer        *kafka.Consumer
	tasks           chan *kafka.Message
	acks            chan ack
	parallelWorkers int
	wg              *sync.WaitGroup
}

// ConsumerConfig holds the configuration for creating a consumer
type ConsumerConfig struct {
	BootstrapServers string
	GroupID          string
	Topics           []string
	ParallelWorkers  int
}

// NewConsumer creates a new Consumer instance
func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":             config.BootstrapServers,
		"group.id":                      config.GroupID,
		"auto.offset.reset":             "earliest",
		"enable.auto.commit":            false, // Manual offset control
		"enable.auto.offset.store":      false, // Store after success only
		"partition.assignment.strategy": "cooperative-sticky",
		"session.timeout.ms":            30000,
		"max.poll.interval.ms":          300000, // 5 minutes
	}

	consumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	err = consumer.SubscribeTopics(config.Topics, nil)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}

	return &Consumer{
		consumer:        consumer,
		tasks:           make(chan *kafka.Message, 1000),
		acks:            make(chan ack, 1000),
		parallelWorkers: config.ParallelWorkers,
		wg:              &sync.WaitGroup{},
	}, nil
}

// Close closes the consumer and its channels
func (c *Consumer) Close() error {
	return c.consumer.Close()
}

// startWorkerPool creates and starts the worker goroutines
func (c *Consumer) startWorkerPool(ctx context.Context) {
	for i := 0; i < c.parallelWorkers; i++ {
		c.wg.Add(1)
		go func(workerID int) {
			defer c.wg.Done()
			log.Printf("Worker %d started", workerID)

			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-c.tasks:
					if !ok {
						return // Channel closed
					}

					// Process the message
					if err := processMessage(msg); err != nil {
						log.Printf("Failed to process message: %v", err)
						// Decide: retry, DLQ, or skip
						// Don't send ack on failure for at-least-once delivery
						continue
					}

					// Signal successful processing
					c.acks <- ack{
						tp:  msg.TopicPartition,
						off: msg.TopicPartition.Offset,
					}
				}
			}
		}(i)
	}
}

// startCommitCoordinator handles offset management and periodic commits
func (c *Consumer) startCommitCoordinator(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		pending := make(map[kafka.TopicPartition]kafka.Offset)

		for {
			select {
			case <-ctx.Done():
				return
			case ack := <-c.acks:
				// Store offset for successful message (next offset to read)
				nextOffset := ack.off + 1
				tp := kafka.TopicPartition{
					Topic:     ack.tp.Topic,
					Partition: ack.tp.Partition,
					Offset:    nextOffset,
				}

				pending[ack.tp] = nextOffset

				// Store offset locally
				_, err := c.consumer.StoreOffsets([]kafka.TopicPartition{tp})
				if err != nil {
					log.Printf("Failed to store offset: %v", err)
				}

			case <-ticker.C:
				// Periodically commit stored offsets
				if len(pending) > 0 {
					offsets, err := c.consumer.Commit()
					if err != nil {
						log.Printf("Commit failed: %v", err)
					} else {
						log.Printf("Committed %d partitions", len(offsets))
					}
					pending = make(map[kafka.TopicPartition]kafka.Offset)
				}
			}
		}
	}()
}

// handleKafkaEvent processes different types of Kafka events
func (c *Consumer) handleKafkaEvent(ev kafka.Event, ctx context.Context) error {
	switch e := ev.(type) {
	case *kafka.Message:
		// Send to worker pool (blocks if queue full)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case c.tasks <- e:
			// Message queued for processing
		}

	case kafka.Error:
		log.Printf("Consumer error: %v", e)
		if e.Code() == kafka.ErrAllBrokersDown {
			return fmt.Errorf("all brokers down")
		}

	case kafka.AssignedPartitions:
		log.Printf("Partitions assigned: %v", e)
		err := c.consumer.Assign(e.Partitions)
		if err != nil {
			log.Printf("Failed to assign partitions: %v", err)
		}

	case kafka.RevokedPartitions:
		log.Printf("Partitions revoked: %v", e)
		err := c.consumer.Unassign()
		if err != nil {
			log.Printf("Failed to unassign partitions: %v", err)
		}

	default:
		log.Printf("Ignored event: %v", e)
	}

	return nil
}

// gracefulShutdown handles the shutdown process
func (c *Consumer) gracefulShutdown() error {
	log.Println("Shutting down consumer...")

	// Stop accepting new work
	close(c.tasks)

	// Wait for workers to finish
	c.wg.Wait()

	// Final commit
	_, err := c.consumer.Commit()
	if err != nil {
		log.Printf("Final commit failed: %v", err)
	}

	return c.consumer.Close()
}

// Run starts the consumer and blocks until context is cancelled
func (c *Consumer) Run(ctx context.Context) error {
	// Start worker pool
	c.startWorkerPool(ctx)

	// Start commit coordinator
	c.startCommitCoordinator(ctx)

	// Main polling loop (single-threaded!)
	log.Println("Starting consumer polling loop")

	for {
		select {
		case <-ctx.Done():
			return c.gracefulShutdown()

		default:
			// Poll for events (100ms timeout)
			ev := c.consumer.Poll(100)
			if ev == nil {
				continue
			}

			if err := c.handleKafkaEvent(ev, ctx); err != nil {
				return err
			}
		}
	}
}

func processMessage(msg *kafka.Message) error {
	// Your business logic here
	log.Printf("Processing message from %s[%d]@%d: %s",
		*msg.TopicPartition.Topic,
		msg.TopicPartition.Partition,
		msg.TopicPartition.Offset,
		string(msg.Value))

	// Simulate processing time
	time.Sleep(10 * time.Millisecond)

	return nil
}

func main() {
	// Parse command line arguments or use defaults
	bootstrapServers := getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	groupID := getEnvOrDefault("KAFKA_GROUP_ID", "my-consumer-group")
	topicName := getEnvOrDefault("KAFKA_TOPIC", "my-topic")

	// Create consumer
	consumer, err := NewConsumer(ConsumerConfig{
		BootstrapServers: bootstrapServers,
		GroupID:          groupID,
		Topics:           []string{topicName},
		ParallelWorkers:  5,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	// Start consumer
	log.Printf("Starting Kafka consumer...")
	log.Printf("Bootstrap servers: %s", bootstrapServers)
	log.Printf("Group ID: %s", groupID)
	log.Printf("Topic: %s", topicName)

	if err := consumer.Run(ctx); err != nil {
		if err != context.Canceled {
			log.Printf("Consumer error: %v", err)
		}
	}

	log.Println("Consumer shutdown complete")
}

// getEnvOrDefault returns environment variable value or default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
