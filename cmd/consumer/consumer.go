package consumer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/dnsx2k/partymq/pkg/partition"
	"github.com/dnsx2k/partymq/pkg/rabbit"
	"github.com/dnsx2k/partymq/pkg/sender"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type consumerCtx struct {
	amqpOrchestrator rabbit.AmqpOrchestrator
	sender           sender.Sender
	logger           *zap.Logger
	queue            string
	keySource        string
	keyName          string
}

// New - creation function
func New(amqpOrch rabbit.AmqpOrchestrator, sender sender.Sender, logger *zap.Logger, queue, keySource, keyName string) *consumerCtx {
	return &consumerCtx{
		amqpOrchestrator: amqpOrch,
		sender:           sender,
		logger:           logger,
		queue:            queue,
		keySource:        keySource,
		keyName:          keyName,
	}
}

func (cs *consumerCtx) Start(ctx context.Context, doneCh chan struct{}) error {
	msgs, err := cs.consume(cs.queue, "party-mq")
	if err != nil {
		return err
	}
	fKey := fetchKeyFn(cs.keySource, cs.keyName)

	go func() {
		for {
			select {
			case msg := <-msgs:
				key := fKey(&msg)

				if err := cs.sender.Send(ctx, msg.Body, key); err != nil {
					switch err {
					case partition.ErrClientNotFound:
						// 'long-pooling' also could be done with pausing msg delivery by ch.Flow(false)
						time.Sleep(10 * time.Second)
						_ = msg.Reject(true)
						continue
					default:
						cs.logger.Error("error occurred while processing message", zap.String("priority", "low"), zap.Error(err))
						_ = msg.Nack(false, true)
						continue
					}
				}
				_ = msg.Ack(false)
			case <-doneCh:
				return
			}
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
				return ""
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
				return ""
			}
			return keyStr
		}
	}

	return nil
}
