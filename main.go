package main

import (
	"fmt"
	"log"
	"time"
)

type ExampleMessage struct {
	ID        string    `json:"id"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}


func main() {
	// Configuration
	brokers := "localhost:9092"
	topic := "test-topic"
	groupID := "test-consumer-group"

	// Create producer
	producer, err := NewKafkaProducer(ProducerConfig{
		Brokers:  brokers,
		ClientId: "example-producer",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create consumer
	consumer, err := NewKafkaConsumer(ConsumerConfig{
		Brokers:            brokers,
		GroupID:            groupID,
		ClientID:           "example-consumer",
		EnableAutoCommit:   false,
		AutoCommitInterval: 5000,
		BatchSize:          10,
		BatchTimeOut:       time.Second * 5,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Start consumer in a goroutine
	go func() {
		log.Println("Starting consumer...")
		err := consumer.Consume(topic, func(key, value []byte) error {
			var msg ExampleMessage
			if err := UnmarshalJSON(value, &msg); err != nil {
				log.Printf("Failed to unmarshal message: %v", err)
				return err
			}
			log.Printf("Processed message: Key=%s, ID=%s, Content=%s, Timestamp=%v", 
				string(key), msg.ID, msg.Content, msg.Timestamp)
			return nil
		})
		if err != nil {
			log.Printf("Consumer error: %v", err)
		}
	}()

	// Produce some messages
	log.Println("Starting producer...")
	for i := 0; i < 5; i++ {
		message := ExampleMessage{
			ID:        fmt.Sprintf("msg-%d", i+1),
			Content:   fmt.Sprintf("Hello Kafka! Message %d", i+1),
			Timestamp: time.Now(),
		}

		err := producer.Produce(topic, fmt.Sprintf("msg-%d", i+1), message)
		if err != nil {
			log.Printf("Failed to produce message: %v", err)
			continue
		}

		log.Printf("Produced message: %+v", message)
		time.Sleep(2 * time.Second)
	}

	// Wait for messages to be consumed
	time.Sleep(10 * time.Second)
	log.Println("Done!")
}