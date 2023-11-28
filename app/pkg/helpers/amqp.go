package helpers

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// WrapAmqpPublishing - returns amqp publishing with msg inside
func WrapAmqpPublishing(msg []byte) amqp.Publishing {
	return amqp.Publishing{
		Timestamp: time.Now(),
		AppId:     "party-mq",
		Body:      msg,
	}
}
