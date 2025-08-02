package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Producer interface {
	Produce(topic string, message interface{}) error
	Close() error
}

type KafkaProducer struct {
	producer *kafka.Producer
	done     chan bool
}

type ProducerConfig struct {
	Brokers []string
}

func NewKafkaProducer(config ProducerConfig) (*KafkaProducer, error) {
	brokers := ""
	for i, broker := range config.Brokers {
		if i > 0 {
			brokers += ","
		}
		brokers += broker
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	kp := &KafkaProducer{
		producer: p,
		done:     make(chan bool),
	}

	// Start delivery report handler
	go kp.handleDeliveryReports()

	return kp, nil
}

func (kp *KafkaProducer) handleDeliveryReports() {
	for e := range kp.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v\n", ev.TopicPartition)
			} else {
				log.Printf("Delivered message to %v\n", ev.TopicPartition)
			}
		}
	}
}

func (kp *KafkaProducer) Produce(topic string, message interface{}) error {
	// Convert message to JSON
	msgBytes, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Produce message
	err = kp.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          msgBytes,
	}, nil)

	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	return nil
}

func (kp *KafkaProducer) Close() error {
	close(kp.done)
	kp.producer.Close()
	return nil
}
