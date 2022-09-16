package order

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

func ListeningOrderCreated(reader *kafka.Reader, c chan kafka.Message) {

	m, err := reader.ReadMessage(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	// fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	c <- m

}

func BookStock() {

}

func CancelReservedStock() {

}

func ReleaseStockForDelivery() {

}
