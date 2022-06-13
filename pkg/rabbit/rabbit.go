package rabbit

import (
	"fmt"
	"time"

	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type AmqpOrchestrator interface {
	CreateResources() error
	GetChannel(d Direction) (*amqp.Channel, error)
	GetMessageTemplate() amqp.Publishing
	InitPartition(clientID string) (string, error)
	InspectQueues(m map[string]string) (map[string]amqp.Queue, error)
}

type amqpCtx struct {
	connections map[Direction]*amqp.Connection
	logger      *zap.Logger
}

// Init - initializes amqp connections, handles connection close notification
func Init(url string) (AmqpOrchestrator, error) {
	notifyCloseCh := make(chan *amqp.Error)
	actx := amqpCtx{
		connections: make(map[Direction]*amqp.Connection),
		logger:      nil,
	}
	primConn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	actx.connections[DirectionPrimary] = primConn
	primConn.NotifyClose(notifyCloseCh)

	subConn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	actx.connections[DirectionSub] = subConn
	subConn.NotifyClose(notifyCloseCh)

	pubConn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	actx.connections[DirectionPub] = pubConn
	subConn.NotifyClose(notifyCloseCh)

	go actx.handleConnectionClose(notifyCloseCh)

	return &actx, nil
}

// CreateResources - creates necessary amqp resources
func (ac *amqpCtx) CreateResources() error {
	ch, err := ac.connections[DirectionPrimary].Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err = ch.ExchangeDeclare(PartyMqExchange, amqp.ExchangeDirect, false, true, false, false, nil); err != nil {
		return err
	}
	if _, err = ch.QueueDeclare(PartyMqBufferQueue, false, false, false, false, nil); err != nil {
		return err
	}

	return nil
}

// InitPartition - initializes new partition for client, returns name of created and bounded queue
func (ac *amqpCtx) InitPartition(clientID string) (string, error) {
	ch, err := ac.connections[DirectionPrimary].Channel()
	if err != nil {
		return "", err
	}
	defer ch.Close()

	queue, err := ch.QueueDeclare(fmt.Sprintf("partymq.q.partition-%s", clientID), false, false, false, false, nil)
	if err != nil {
		return "", err
	}
	err = ch.QueueBind(queue.Name, fmt.Sprintf("partymq.r.partition-%s", clientID), PartyMqExchange, false, nil)
	if err != nil {
		if _, err := ch.QueueDelete(queue.Name, false, false, false); err != nil {
			return "", err
		}
	}

	return queue.Name, nil
}

// GetChannel - based on passed direction create new amqp channel
func (ac *amqpCtx) GetChannel(d Direction) (*amqp.Channel, error) {
	ch, err := ac.connections[d].Channel()
	if err != nil {
		return nil, err
	}

	return ch, nil
}

// GetMessageTemplate - returns amqp msg boilerplate
func (ac *amqpCtx) GetMessageTemplate() amqp.Publishing {
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
		Body:            nil,
	}
}

// InspectQueues - calls channel.QueueInspect for connected clients
func (ac *amqpCtx) InspectQueues(m map[string]string) (map[string]amqp.Queue, error) {
	ch, err := ac.GetChannel(DirectionPrimary)
	if err != nil {
		return nil, err
	}
	out := make(map[string]amqp.Queue)
	for k, v := range m {
		q, err := ch.QueueInspect(v)
		if err != nil {
			ac.logger.Error(err.Error(), zap.String("clientID", k))
			out[k] = amqp.Queue{}
			continue
		}
		out[k] = q
	}
	return out, nil
}

func (ac *amqpCtx) handleConnectionClose(c <-chan *amqp.Error) {
	for {
		err := <-c
		ac.logger.Error(err.Error())
	}
}
