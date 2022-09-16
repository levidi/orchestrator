package common

import (
	"os"

	"github.com/segmentio/kafka-go"
)

type Topics struct {
	Itens []kafka.TopicConfig
}

func ConfigTopic(name string, partitions int, replication int) kafka.TopicConfig {
	return kafka.TopicConfig{
		Topic:             name,
		NumPartitions:     partitions,
		ReplicationFactor: replication,
	}
}

func GetKafkaWriter(topic string) *kafka.Writer {
	kafkaURL := os.Getenv("KAFKA_URL")

	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}
