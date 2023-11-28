package utils

import (
	"fmt"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func CreateQueue(channel *amqp.Channel, queueName string) {
	_, err := channel.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func CreateExchange(channel *amqp.Channel, exchange string) {
	err := channel.ExchangeDeclare(exchange, amqp.ExchangeDirect, false, false, false, false, nil)
	if err != nil {
		fmt.Printf("Error ocurred while creating exchange: %s", err.Error())
		os.Exit(1)
	}
}

func BindQueue(channel *amqp.Channel, queueName, routingKey, exchange string) {
	if err := channel.QueueBind(queueName, routingKey, exchange, false, nil); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func InspectNumberOfMessages(channel *amqp.Channel, queue string) int {
	q, err := channel.QueueDeclarePassive(queue, false, false, false, false, nil)
	if err != nil {
		fmt.Println(err.Error())
		time.Sleep(1 * time.Second)
		return 0
	}
	return q.Messages
}
