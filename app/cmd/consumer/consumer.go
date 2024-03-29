package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/dnsx2k/partymq/app/pkg/partition"
	rabbit2 "github.com/dnsx2k/partymq/app/pkg/rabbit"
	"github.com/dnsx2k/partymq/app/pkg/sender"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

var running = false

type consumerCtx struct {
	amqpOrchestrator rabbit2.AmqpOrchestrator
	sender           sender.Sender
	logger           *zap.Logger
	queue            string
	keySource        string
	keyName          string
	start            chan struct{}
	stop             chan struct{}
}

// New - creation function
func New(amqpOrch rabbit2.AmqpOrchestrator, sender sender.Sender, logger *zap.Logger, queue, keySource, keyName string) *consumerCtx {
	cctx := &consumerCtx{
		amqpOrchestrator: amqpOrch,
		sender:           sender,
		logger:           logger,
		queue:            queue,
		keySource:        keySource,
		keyName:          keyName,
		start:            make(chan struct{}, 1),
		stop:             make(chan struct{}, 1),
	}
	return cctx
}

func (cs *consumerCtx) CheckState() {
	for {
		<-time.After(10 * time.Second)
		if cs.sender.Ready() && !running {
			cs.start <- struct{}{}
		} else if !cs.sender.Ready() && running {
			cs.stop <- struct{}{}
		}
	}
}

func (cs *consumerCtx) Consume(ctx context.Context, exit chan struct{}) error {
	fKey := fetchKeyFn(cs.keySource, cs.keyName)
	var consumerChan *amqp.Channel
	for {
		select {
		case <-cs.start:
			ch, err := cs.amqpOrchestrator.GetChannel(rabbit2.DirectionSub)
			if err != nil {
				return err
			}
			consumerChan = ch

			if err = ch.Qos(10, 0, false); err != nil {
				return err
			}
			msgs, err := ch.Consume(cs.queue, "party-mq", false, false, false, false, nil)
			if err != nil {
				return err
			}
			go func() {
				running = true
				for {
					select {
					case msg := <-msgs:
						key := fKey(&msg)
						// hack: amqp lib will spam with nil messages
						if msg.Body == nil {
							time.Sleep(1 * time.Second)
							continue
						}
						if err := cs.sender.Send(ctx, msg.Body, msg.Headers, key); err != nil {
							// consumer should disconnect while there is no client, but with 10sec gap it is still possible
							if errors.Is(err, partition.ErrClientNotFound) {
								_ = msg.Reject(true)
								continue
							} else {
								cs.logger.Error("error occurred while processing message", zap.String("priority", "low"), zap.Error(err))
								_ = msg.Nack(false, true)
								continue
							}
						}
						_ = msg.Ack(false)
					case <-exit:
						_ = consumerChan.Cancel("party-mq", true)
						return
					}
				}
			}()
		case <-cs.stop:
			_ = consumerChan.Cancel("party-mq", true)
			running = false
		}
	}
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
