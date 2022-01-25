package rabbit

import (
	"fmt"
	"time"

	party "github.com/dnsx2k/party-mq/pkg/service"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

type AmqpOrchestrator interface {
	CreateResources() error
	GetChannel(d Direction) (*amqp.Channel, error)
	GetMessageTemplate() amqp.Publishing
	InitPartition(clientID string) (string, error)
	InspectQueues(m map[uuid.UUID]party.Client) (map[uuid.UUID]amqp.Queue, error)
}

type amqpCtx struct {
	primaryConnection   *amqp.Connection
	publishConnection   *amqp.Connection
	subscribeConnection *amqp.Connection
}

// TODO: Notify connection close
func Init(url string) (AmqpOrchestrator, error) {
	actx := amqpCtx{}
	primConn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	actx.primaryConnection = primConn

	subConn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	actx.subscribeConnection = subConn

	pubConn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	actx.publishConnection = pubConn

	return &actx, nil
}

func (ac *amqpCtx) CreateResources() error {
	ch, err := ac.primaryConnection.Channel()
	if err != nil {
		return err
	}

	if err = ch.ExchangeDeclare(PartyMqExchange, amqp.ExchangeDirect, false, true, false, false, nil); err != nil {
		return err
	}

	if _, err = ch.QueueDeclare(PartyMqBufferQueue, false, false, false, false, nil); err != nil {
		return err
	}

	if err = ch.Close(); err != nil {
		return err
	}

	return nil
}

func (ac *amqpCtx) InitPartition(clientID string) (string, error) {
	ch, err := ac.primaryConnection.Channel()
	if err != nil {
		return "", err
	}

	queue, err := ch.QueueDeclare(fmt.Sprintf("party-mq-partition-%s", clientID), false, false, false, false, nil)
	if err != nil{
		return "", err
	}

	err = ch.QueueBind(queue.Name, fmt.Sprintf("party-mq-partition-key-%s", clientID), PartyMqExchange, false, nil)
	if err != nil{
		// TODO: Delete declared queue "teardown"
		return "", nil
	}

	return queue.Name, nil
}

func (ac *amqpCtx) GetChannel(d Direction) (*amqp.Channel, error) {
	switch d {
	case DirectionSub:
		ch, err := ac.subscribeConnection.Channel()
		if err != nil {
			return nil, err
		}
		// TODO: Channel close notify
		return ch, nil
	case DirectionPub:
		ch, err := ac.publishConnection.Channel()
		if err != nil {
			return nil, err
		}
		// TODO: Channel close notify
		return ch, nil
	case DirectionPrimary:
		ch, err := ac.primaryConnection.Channel()
		if err != nil {
			return nil, err
		}
		// TODO: Channel close notify
		return ch, nil
	default:
		// TODO: Return some error
		return nil, nil
	}
}

func (ac *amqpCtx)GetMessageTemplate() amqp.Publishing{
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

func (ac *amqpCtx)InspectQueues(m map[uuid.UUID]party.Client) (map[uuid.UUID]amqp.Queue, error){
	ch, err := ac.GetChannel(DirectionPrimary)
	if err != nil{
		return nil, err
	}
	out := make(map[uuid.UUID]amqp.Queue)
	for k, v := range m{
		// TODO: Handle error or at least log it
		q, _ := ch.QueueInspect(v.QueueName)
		out[k] = q
	}
	return out, nil
}
