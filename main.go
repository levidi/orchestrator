package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	common "github.com/levidi/orchestrator/common"
	order "github.com/levidi/orchestrator/order"
	payment "github.com/levidi/orchestrator/payment"
	stock "github.com/levidi/orchestrator/stock"
	kafka "github.com/segmentio/kafka-go"
)

var kafkaURL string = os.Getenv("KAFKA_URL")

func createReader(topicName string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaURL},
		Topic:   topicName,
	})
}

func ListeningPayment(c chan kafka.Message) {

	readerPaymentApproved := createReader("paymentApproved")
	defer readerPaymentApproved.Close()

	payment.ListeningPaymentApproved(readerPaymentApproved, c)

	readerPaymentRefused := createReader("paymentRefused")
	defer readerPaymentRefused.Close()

	payment.ListeningPaymentRefused(readerPaymentRefused, c)

}

func startSagaOrder() {

	orderChannel := make(chan kafka.Message)
	stockChannel := make(chan kafka.Message)
	paymentChannel := make(chan kafka.Message)

	readerOrder := createReader("orderCreated")
	defer readerOrder.Close()

	writerBookStock := common.GetKafkaWriter("bookStock")
	defer writerBookStock.Close()

	writerReleaseStockForDelivery := common.GetKafkaWriter("releaseStockForDelivery")
	defer writerReleaseStockForDelivery.Close()

	writerCancelReservedStock := common.GetKafkaWriter("cancelReservedStock")
	defer writerCancelReservedStock.Close()

	writerPaymentProcess := common.GetKafkaWriter("paymentProcess")
	defer writerPaymentProcess.Close()

	readerStock := createReader("stockReserved")
	defer readerStock.Close()

	for {
		go func() {
			order.ListeningOrderCreated(readerOrder, orderChannel)
			msOrder := <-orderChannel

			okBookStock, err := stock.BookStock(writerBookStock, msOrder)
			if err != nil {
				log.Fatalln("Error on book stock")
			}

			if okBookStock {
				readerOrder.CommitMessages(context.Background())
			}
		}()

		go func() {
			stock.ListeningStockReserved(readerStock, stockChannel)
			msStock := <-stockChannel

			okPaymentProcess, err := payment.PaymentProcess(writerPaymentProcess, msStock)
			if err != nil {
				log.Fatalln("Error on payment process")
			}

			if okPaymentProcess {
				readerStock.CommitMessages(context.Background())
			}
		}()

		go func() {
			ListeningPayment(paymentChannel)
			msPayment := <-paymentChannel
			switch string(msPayment.Key) {
			case "approved":
				stock.ReleaseStockForDelivery(writerReleaseStockForDelivery, msPayment)
			case "refused":
				stock.CancelReservedStock(writerCancelReservedStock, msPayment)
			}
		}()
	}

}

func getTopics() (t common.Topics) {
	orderCreated := common.ConfigTopic("orderCreated", 1, 1)
	t.Itens = append(t.Itens, orderCreated)

	paymentProcess := common.ConfigTopic("paymentProcess", 1, 1)
	paymentApproved := common.ConfigTopic("paymentApproved", 1, 1)
	paymentRefused := common.ConfigTopic("paymentRefused", 1, 1)
	t.Itens = append(t.Itens, paymentApproved)
	t.Itens = append(t.Itens, paymentProcess)
	t.Itens = append(t.Itens, paymentRefused)

	bookStock := common.ConfigTopic("bookStock", 1, 1)
	stockReserved := common.ConfigTopic("stockReserved", 1, 1)
	cancelReservedStock := common.ConfigTopic("cancelReservedStock", 1, 1)
	releaseStockForDelivery := common.ConfigTopic("releaseStockForDelivery", 1, 1)
	t.Itens = append(t.Itens, bookStock)
	t.Itens = append(t.Itens, stockReserved)
	t.Itens = append(t.Itens, cancelReservedStock)
	t.Itens = append(t.Itens, releaseStockForDelivery)

	return
}

func createSagaOrder() {
	conn, err := kafka.Dial("tcp", kafkaURL)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}

	fmt.Printf("%s %d ", controller.Host, controller.Port)

	conncontroller, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}

	t := getTopics()
	err = conncontroller.CreateTopics(t.Itens...)
	if err != nil {
		panic(err.Error())
	}

}

func main() {
	createSagaOrder()
	startSagaOrder()
}
