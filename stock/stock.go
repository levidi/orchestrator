package stock

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

func BookStock(w *kafka.Writer, msOrder kafka.Message) (bool, error) {

	m := []byte("Solicitação de reserva de estoque")
	msg := kafka.Message{
		Key:   []byte("CHAVE011"),
		Value: m,
	}
	err := w.WriteMessages(context.Background(), msg)

	if err != nil {
		log.Fatalln(err)
		return false, err
	}
	return true, nil

}

func ListeningStockReserved(reader *kafka.Reader, c chan kafka.Message) {

	m, err := reader.ReadMessage(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	// fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	c <- m

}

func ReleaseStockForDelivery(w *kafka.Writer, msOrder kafka.Message) (err error) {

	m := []byte("sucess")
	msg := kafka.Message{
		Key:   []byte("CHAVE011"),
		Value: m,
	}
	err = w.WriteMessages(context.Background(), msg)

	if err != nil {
		log.Fatalln(err)
	}
	return

}

func CancelReservedStock(w *kafka.Writer, msOrder kafka.Message) (err error) {

	m := []byte("sucess")
	msg := kafka.Message{
		Key:   []byte("CHAVE011"),
		Value: m,
	}
	err = w.WriteMessages(context.Background(), msg)

	if err != nil {
		log.Fatalln(err)
	}
	return

}
