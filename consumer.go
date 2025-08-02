package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Consumer interface {
	Subscribe(topics []string) error
	Consume(handler func(message []byte) error) error
	Close() error
}

type KafkaConsumer struct {
	consumer *kafka.Consumer
	done     chan bool
}

type ConsumerConfig struct {
	Brokers  []string
	GroupID  string
	AutoCommit bool
}

func NewKafkaConsumer(config ConsumerConfig) (*KafkaConsumer, error) {
	brokers := ""
	for i, broker := range config.Brokers {
		if i > 0 {
			brokers += ","
		}
		brokers += broker
	}

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          config.GroupID,
		"auto.offset.reset": "earliest",
	}

	if config.AutoCommit {
		configMap.SetKey("enable.auto.commit", true)
	} else {
		configMap.SetKey("enable.auto.commit", false)
	}

	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	kc := &KafkaConsumer{
		consumer: c,
		done:     make(chan bool),
	}

	return kc, nil
}

func (kc *KafkaConsumer) Subscribe(topics []string) error {
	err := kc.consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}
	return nil
}

func (kc *KafkaConsumer) Consume(handler func(message []byte) error) error {
	for {
		select {
		case <-kc.done:
			return nil
		default:
			msg, err := kc.consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Check if it's a timeout error (expected when no messages)
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Consumer error: %v\n", err)
				continue
			}

			// Process the message
			if err := handler(msg.Value); err != nil {
				log.Printf("Error processing message: %v\n", err)
				continue
			}

			// Manually commit if auto-commit is disabled
			_, err = kc.consumer.CommitMessage(msg)
			if err != nil {
				log.Printf("Error committing message: %v\n", err)
			}

			log.Printf("Consumed message from %s[%d] at offset %v: %s\n",
				*msg.TopicPartition.Topic, msg.TopicPartition.Partition,
				msg.TopicPartition.Offset, string(msg.Value))
		}
	}
}

func (kc *KafkaConsumer) Close() error {
	close(kc.done)
	return kc.consumer.Close()
}

// Helper function to unmarshal JSON messages
func UnmarshalJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}