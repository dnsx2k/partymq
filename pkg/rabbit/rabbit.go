package rabbit

import (
	"fmt"

	"github.com/dnsx2k/partymq/pkg/helpers"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type AmqpOrchestrator interface {
	CreateResources(sourceExchange, sourceRouting string) error
	GetChannel(d Direction) (*amqp.Channel, error)
	InitPartition(clientID string) (Partition, error)
	InspectQueues(queues []string) (map[string]amqp.Queue, error)
	InspectQueue(queueName string) (amqp.Queue, error)
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
func (ac *amqpCtx) CreateResources(sourceExchange, sourceRouting string) error {
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
	if err = ch.QueueBind(PartyMqBufferQueue, sourceRouting, sourceExchange, true, nil); err != nil {
		return err
	}

	return nil
}

type Partition struct {
	Queue      string
	RoutingKey string
}

// InitPartition - initializes new partition for client, returns name of created and bounded queue
func (ac *amqpCtx) InitPartition(hostname string) (Partition, error) {
	ch, err := ac.connections[DirectionPrimary].Channel()
	defer ch.Close()
	if err != nil {
		return Partition{}, err
	}

	// queue naming should be configurable in order to satisfy user's standards
	queue, err := ch.QueueDeclare(helpers.QueueFromHostname(hostname), false, false, false, false, nil)
	if err != nil {
		return Partition{}, err
	}
	err = ch.QueueBind(queue.Name, fmt.Sprintf("party-mq-partition-key-%s", hostname), PartyMqExchange, false, nil)
	if err != nil {
		if _, err := ch.QueueDelete(queue.Name, false, false, false); err != nil {
			return Partition{}, err
		}
	}

	return Partition{queue.Name, fmt.Sprintf("party-mq-partition-key-%s", hostname)}, nil
}

// GetChannel - based on passed direction create new amqp channel
func (ac *amqpCtx) GetChannel(d Direction) (*amqp.Channel, error) {
	ch, err := ac.connections[d].Channel()
	if err != nil {
		return nil, err
	}

	return ch, nil
}

// InspectQueues - calls channel.QueueInspect for connected clients
func (ac *amqpCtx) InspectQueues(queues []string) (map[string]amqp.Queue, error) {
	ch, err := ac.GetChannel(DirectionPrimary)
	if err != nil {
		return nil, err
	}
	out := make(map[string]amqp.Queue)
	for i := range queues {
		q, err := ch.QueueInspect(queues[i])
		if err != nil {
			ac.logger.Error(err.Error(), zap.String("queue", queues[i]))
			out[queues[i]] = amqp.Queue{}
			continue
		}
		out[queues[i]] = q
	}
	return out, nil
}

// InspectQueue - calls channel.QueueInspect for connected clients
func (ac *amqpCtx) InspectQueue(queueName string) (amqp.Queue, error) {
	ch, err := ac.GetChannel(DirectionPrimary)
	if err != nil {
		return amqp.Queue{}, err
	}
	q, err := ch.QueueInspect(queueName)
	if err != nil {
		return amqp.Queue{}, err
	}

	return q, nil
}

func (ac *amqpCtx) handleConnectionClose(c <-chan *amqp.Error) {
	for {
		err := <-c
		ac.logger.Error(err.Error())
	}
}
