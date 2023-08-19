package consumer

import (
	"context"
	"encoding/json"

	"github.com/dnsx2k/partymq/pkg/rabbit"
	"github.com/dnsx2k/partymq/pkg/sender"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type consumerCtx struct {
	amqpOrchestrator rabbit.AmqpOrchestrator
	sender           sender.Sender
	logger           *zap.Logger
	source           string
	key              string
}

// New - creation function
func New(amqpOrch rabbit.AmqpOrchestrator, sender sender.Sender, logger *zap.Logger, source, key string) *consumerCtx {
	return &consumerCtx{
		amqpOrchestrator: amqpOrch,
		sender:           sender,
		logger:           logger,
		source:           source,
		key:              key,
	}
}

func (cs *consumerCtx) Start(ctx context.Context) error {
	msgs, err := cs.consume(rabbit.PartyMqBufferQueue, "party-mq")
	if err != nil {
		return err
	}
	fKey := fetchKeyFn(cs.source, cs.key)

	go func() {
		for {
			select {
			case msg := <-msgs:
				if err := cs.process(msg, fKey); err != nil {
					cs.logger.Error("error occurred while processing message", zap.String("priority", "low"), zap.Error(err))
				}
			}
			// TODO: Break on graceful shutdown :)
		}
	}()

	return nil
}

func (cs *consumerCtx) consume(queue, consumer string) (<-chan amqp.Delivery, error) {
	ch, err := cs.amqpOrchestrator.GetChannel(rabbit.DirectionSub)
	if err != nil {
		return nil, err
	}

	if err = ch.Qos(10, 0, false); err != nil {
		return nil, err
	}
	msgChan, err := ch.Consume(queue, consumer, false, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	return msgChan, nil
}

func (cs *consumerCtx) process(msg amqp.Delivery, fKey func(msg *amqp.Delivery) string) error {
	key := fKey(&msg)

	err, ok := cs.sender.Send(msg.Body, key)
	if err != nil {
		_ = msg.Ack(false)
		return err
	}
	if !ok {
		return nil
	}

	if err = msg.Ack(false); err != nil {
		return err
	}

	return nil
}

func fetchKeyFn(source, key string) func(msg *amqp.Delivery) string {
	switch source {
	case "header":
		return func(msg *amqp.Delivery) string {
			h, ok := msg.Headers[key]
			if !ok {
				return ""
			}
			keyStr, ok := h.(string)
			if !ok {
			}
			return keyStr
		}
	case "body":
		return func(msg *amqp.Delivery) string {
			m := map[string]any{}
			if err := json.Unmarshal(msg.Body, &m); err != nil {
				return ""
			}
			keyStr, ok := m[key].(string)
			if !ok {
			}
			return keyStr
		}
	}

	return nil
}
