package helpers

import (
	"time"

	"github.com/streadway/amqp"
)

// WrapAmqpPublishing - returns amqp publishing with msg inside
func WrapAmqpPublishing(msg []byte) amqp.Publishing {
	return amqp.Publishing{
		Headers:         nil,
		ContentType:     "",
		ContentEncoding: "",
		DeliveryMode:    0,
		Priority:        0,
		CorrelationId:   "",
		ReplyTo:         "",
		Expiration:      "",
		MessageId:       "",
		Timestamp:       time.Now(),
		Type:            "",
		UserId:          "",
		AppId:           "party-mq",
		Body:            msg,
	}
}
