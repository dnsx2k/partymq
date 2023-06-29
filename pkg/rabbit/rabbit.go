package rabbit

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type AmqpOrchestrator interface {
	CreateResources(sourceExchange, sourceRouting string) error
	GetChannel(d Direction) (*amqp.Channel, error)
}

type amqpCtx struct {
	connections map[Direction]*amqp.Connection
	logger      *zap.Logger
}

// Init - initializes amqp connections, handles connection close notification
func Init(url string, logger *zap.Logger) (AmqpOrchestrator, error) {
	notifyCloseCh := make(chan *amqp.Error)
	actx := amqpCtx{
		connections: make(map[Direction]*amqp.Connection),
		logger:      logger,
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

// GetChannel - based on passed direction create new amqp channel
func (ac *amqpCtx) GetChannel(d Direction) (*amqp.Channel, error) {
	ch, err := ac.connections[d].Channel()
	if err != nil {
		return nil, err
	}

	return ch, nil
}

func (ac *amqpCtx) handleConnectionClose(c <-chan *amqp.Error) {
	for {
		err := <-c
		ac.logger.Error(err.Error())
	}
}
