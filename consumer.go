package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Message struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
}

type MessageHandler func(key, value []byte) error

type BatchMessageHandler func(message []Message) error

type Consumer interface {
	Consume(topic string, handler MessageHandler) error
	ConsumeBatch(topic string, handler BatchMessageHandler) error
	Close()
}

type KafkaConsumer struct {
	consumer      *kafka.Consumer
	done          chan bool
	wg            sync.WaitGroup
	handler       MessageHandler
	batchHandler  BatchMessageHandler
	topic         string
	batchSize     int
	batchTimeOut  time.Duration
	messageBatch  []Message
	lastBatchTime time.Time
	mu            sync.Mutex
}

type ConsumerConfig struct {
	Brokers            string
	GroupID            string
	ClientID           string
	EnableAutoCommit   bool
	AutoCommitInterval int
	BatchSize          int
	BatchTimeOut       time.Duration
}

func NewKafkaConsumer(config ConsumerConfig) (*KafkaConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       config.Brokers,
		"group.id":                config.GroupID,
		"client.id":               config.ClientID,
		"enable.auto.commit":      config.EnableAutoCommit,
		"auto.commit.interval.ms": config.AutoCommitInterval,
		"broker.adress.family":    "v4",
		"auto.offset.reset":       "earliest",
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	kc := &KafkaConsumer{
		consumer:      c,
		done:          make(chan bool),
		batchSize:     config.BatchSize,
		batchTimeOut:  config.BatchTimeOut,
		messageBatch:  make([]Message, 0, config.BatchSize),
		lastBatchTime: time.Now(),
	}

	slog.Info("Kafka consumer initialized")
	return kc, nil
}

func (kc *KafkaConsumer) Consume(topic string, handler MessageHandler) error {
	if handler == nil {
		return fmt.Errorf("message handler can't be nil")
	}

	kc.topic = topic
	err := kc.consumer.Subscribe(kc.topic, nil)
	if err != nil {
		return fmt.Errorf("Failed to subscribe to topic %s: %w", topic, err)
	}

	kc.start()
	slog.Info("Started consuming from topic:", topic)
	return nil
}

func (kc *KafkaConsumer) start() {
	kc.wg.Add(1)
	go kc.consumeMessages()
}

func (kc *KafkaConsumer) consumeMessages() {
	defer kc.wg.Done()

	slog.Info("Starting the kafka consmer for topic: %s", kc.topic)

	if kc.batchHandler != nil {
		kc.wg.Add(1)
		go kc.batchTimeOutChecker()
	}

	for {
		select {
		case <-kc.done:
			if kc.batchHandler != nil {
				kc.processBatch()
			}
			slog.Info("Kafka consumer shutting down...")
			return
		default:
			ev := kc.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				if kc.batchHandler != nil {
					kc.addToBatch(e)
				} else {
					kc.processMessage(e)
				}
			case kafka.Error:
				kc.handleKafkaError(e)
			default:
				//ignore
			}

		}
	}
}

func (kc *KafkaConsumer) processMessage(msg *kafka.Message) {
	if msg.TopicPartition.Error != nil {
		slog.Error(fmt.Sprintf("Message error from topic %s [%d]: %w",
			*msg.TopicPartition.Topic,
			msg.TopicPartition.Offset,
			msg.TopicPartition.Error))
		return
	}

	if kc.handler != nil {
		kc.handler(msg.Key, msg.Value)
	}
}

func (kc *KafkaConsumer) addToBatch(msg *kafka.Message) {
	if msg.TopicPartition.Error != nil {
		slog.Info(fmt.Sprintf("Error Message from the topic: %s [%d]: %v",
			*msg.TopicPartition.Topic,
			msg.TopicPartition.Partition,
			msg.TopicPartition.Error))
		return
	}

	kc.mu.Lock()
	defer kc.mu.Unlock()

	message := Message{
		Key:       msg.Key,
		Value:     msg.Value,
		Topic:     *msg.TopicPartition.Topic,
		Partition: msg.TopicPartition.Partition,
		Offset:    int64(msg.TopicPartition.Offset),
	}

	kc.messageBatch = append(kc.messageBatch, message)

	if len(kc.messageBatch) >= kc.batchSize {
		kc.processBatch()
	}
}

func (kc *KafkaConsumer) processBatch() {
	if len(kc.messageBatch) == 0 {
		return
	}

	batch := make([]Message, len(kc.messageBatch))
	copy(batch, kc.messageBatch)
	kc.messageBatch = kc.messageBatch[:0]
	kc.lastBatchTime = time.Now()

	if kc.batchHandler != nil {
		kc.batchHandler(batch)
	}
}

func (kc *KafkaConsumer) batchTimeOutChecker() {
	defer kc.wg.Done()

	ticker := time.NewTicker(kc.batchTimeOut / 2)
	defer ticker.Stop()

	for {
		select {
		case <-kc.done:
			return
		case <-ticker.C:
			kc.mu.Lock()
			if len(kc.messageBatch) > 0 && time.Since(kc.lastBatchTime) >= kc.batchTimeOut {
				kc.processBatch()
			}
			kc.mu.Unlock()
		}
	}
}

func (kc *KafkaConsumer) handleKafkaError(err kafka.Error) {
	slog.Error("Kafka consumer error: %v (code : %d)", err, err.Code())

	if err.IsFatal() {
		slog.Error("FATAL kafka consumer error - may need to restart: %v", err)
	}
}

func (kc *KafkaConsumer) Close() {
	slog.Info("Closing kafka consumer....")

	close(kc.done)

	err := kc.consumer.Close()
	if err != nil {
		slog.Error("Error closing the consumer: ", err)
	}

	kc.wg.Wait()
	slog.Info("Kafka consumer shutdown complete")
}

// Helper function to unmarshal JSON messages
func UnmarshalJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
