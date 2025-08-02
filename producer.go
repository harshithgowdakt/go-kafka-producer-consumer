package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaProducer struct {
	producer *kafka.Producer
	done     chan bool
	wg       sync.WaitGroup
}

type ProducerConfig struct {
	Brokers  string
	ClientId string
}

type Producer interface {
	Produce(topic string, key string, message any) error
	Close()
}

// NewKafkaProducer creates a new Kafka producer instance with the specified configuration
func NewKafkaProducer(config ProducerConfig) (*KafkaProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":     config.Brokers,
		"client.id":             config.ClientId,
		"acks":                  "all",
		"retries":               10,
		"compression.type":      "gzip",
		"enable.idempotence":    true,
		"broker.address.family": "v4",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	kp := &KafkaProducer{
		producer: p,
		done:     make(chan bool),
	}

	kp.start()

	return kp, nil
}

// start initializes the producer by starting the delivery report handler goroutine
func (kp *KafkaProducer) start() {
	kp.wg.Add(1)
	go kp.handleDeliveryReports()
}

// handleDeliveryReports processes delivery reports and errors from the Kafka producer in a background goroutine
func (kp *KafkaProducer) handleDeliveryReports() {
	defer kp.wg.Done()

	for {
		select {
		case <-kp.done:
			slog.Info("Shutting down the handleDeliveryReports")
			return
		case e := <-kp.producer.Events():
			if e == nil {
				slog.Info("Producer channel close")
				return
			}

			switch ev := e.(type) {
			case *kafka.Message:
				kp.processDeliveryReport(ev)
			case kafka.Error:
				kp.handleKafkaError(ev)

			default:
				//ingnore other message
			}
		}
	}
}

// processDeliveryReport logs the delivery status of a message (success or failure)
func (kp *KafkaProducer) processDeliveryReport(msg *kafka.Message) {
	if msg.TopicPartition.Error != nil {
		slog.Error(fmt.Sprintf("Message delvery failed to topic %s [%d]: %v",
			*msg.TopicPartition.Topic,
			msg.TopicPartition.Partition,
			msg.TopicPartition.Error,
		))
	} else {
		slog.Info(fmt.Sprintf("Message delivered to topic %s [%d] at offset %v",
			*msg.TopicPartition.Topic,
			msg.TopicPartition.Partition,
			msg.TopicPartition.Offset))
	}
}

// handleKafkaError logs Kafka errors and checks for fatal errors that may require restart
func (kp *KafkaProducer) handleKafkaError(err kafka.Error) {
	slog.Error("Kafka Error", "error", err, "code", err.Code())

	if err.IsFatal() {
		slog.Error("FATAL kafka error: producer may need to restart", "error", err)
	}
}

// Produce sends a message to the specified Kafka topic with the given key and value
func (kp *KafkaProducer) Produce(topic, key string, message any) error {
	// Convert message to JSON
	var valueByte []byte
	var err error
	switch m := message.(type) {
	case []byte:
		valueByte = m
	case string:
		valueByte = []byte(m)
	default:
		valueByte, err = json.Marshal(m)
		if err != nil {
			return fmt.Errorf("failed to marshal value: %v", err)
		}

	}

	// Produce message
	err = kp.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          valueByte,
		Key:            []byte(key),
		Timestamp:      time.Now(),
	}, nil)

	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	return nil
}

// Close gracefully shuts down the producer by flushing remaining messages and closing connections
func (kp *KafkaProducer) Close() {
	slog.Info("Flushing the remaining messages from kafka producer...")
	unflushed := kp.producer.Flush(300000)
	if unflushed > 0 {
		slog.Error("Failed to flush messages", "count", unflushed)
	}

	close(kp.done)
	kp.producer.Close()
	kp.wg.Wait()
}
