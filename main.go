package main

import (
	"fmt"
	"log"
	"time"
)

type Message struct {
	ID        string    `json:"id"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	// Configuration
	brokers := []string{"localhost:9092"}
	topic := "test-topic"
	groupID := "test-consumer-group"

	// Create producer
	producer, err := NewKafkaProducer(ProducerConfig{
		Brokers: brokers,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create consumer
	consumer, err := NewKafkaConsumer(ConsumerConfig{
		Brokers:    brokers,
		GroupID:    groupID,
		AutoCommit: false,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Subscribe to topic
	err = consumer.Subscribe([]string{topic})
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %v", err)
	}

	// Start consumer in a goroutine
	go func() {
		log.Println("Starting consumer...")
		err := consumer.Consume(func(message []byte) error {
			var msg Message
			if err := UnmarshalJSON(message, &msg); err != nil {
				log.Printf("Failed to unmarshal message: %v", err)
				return err
			}
			log.Printf("Processed message: ID=%s, Content=%s, Timestamp=%v", 
				msg.ID, msg.Content, msg.Timestamp)
			return nil
		})
		if err != nil {
			log.Printf("Consumer error: %v", err)
		}
	}()

	// Produce some messages
	log.Println("Starting producer...")
	for i := 0; i < 5; i++ {
		message := Message{
			ID:        fmt.Sprintf("msg-%d", i+1),
			Content:   fmt.Sprintf("Hello Kafka! Message %d", i+1),
			Timestamp: time.Now(),
		}

		err := producer.Produce(topic, message)
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