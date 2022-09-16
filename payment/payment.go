package payment

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

func PaymentProcess(w *kafka.Writer, msOrder kafka.Message) (bool, error) {

	m := []byte("sucess")
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

func ListeningPaymentApproved(reader *kafka.Reader, c chan kafka.Message) {

	m, err := reader.ReadMessage(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	c <- m
}

func ListeningPaymentRefused(reader *kafka.Reader, c chan kafka.Message) {

	m, err := reader.ReadMessage(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	c <- m
}
