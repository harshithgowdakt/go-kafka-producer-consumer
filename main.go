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
)

func main() {
	var wg sync.WaitGroup

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		runProducer(ctx)
	}()

	// Consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		runConsumer(ctx)
	}()

	// Wait for interrupt signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Println("Shutting down...")
	cancel()
	wg.Wait()
	log.Println("Shutdown complete")
}

func runConsumer(ctx context.Context) {
	config := ConsumerConfig{
		Brokers:              "localhost:29092",
		GroupID:              "test-group",
		ClientID:             "portal-backend-v3-consumer",
		EnableAutoCommit:     true,
		AutoCommitInterval: 5000,
		BatchSize:            100,
		BatchTimeOut:         5 * time.Second,
	}

	consumer, err := NewKafkaConsumer(config)
	if err != nil {
		log.Fatal("Failed to create consumer:", err)
	}
	defer consumer.Close()

	handler := func(messages []Message) error {
		for _, message := range messages {
			log.Printf("Consumed - Key: %s, Value: %s, Partition: %d", string(message.Key), string(message.Value), message.Partition)
		}
		return nil
	}

	err = consumer.ConsumeBatch("test-topic", handler)
	if err != nil {
		log.Fatal("Failed to start consuming:", err)
	}

	<-ctx.Done()
}

func runProducer(ctx context.Context) {
	config := ProducerConfig{
		Brokers:  "localhost:29092",
		ClientId: "portal-backend-v3-producer",
	}
	producer, err := NewKafkaProducer(config)
	if err != nil {
		log.Fatal("Failed to create producer:", err)
	}
	defer producer.Close()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	counter := 0
	for {
		select {
		case <-ctx.Done():

			return
		case <-ticker.C:
			counter++
			message := map[string]interface{}{
				"id":        counter,
				"message":   "Hello from producer",
				"timestamp": time.Now(),
			}

			err := producer.Produce("test-topic", fmt.Sprintf("key-%d", counter), message)
			if err != nil {
				log.Printf("Failed to publish: %v", err)
			}
		}
	}
}
