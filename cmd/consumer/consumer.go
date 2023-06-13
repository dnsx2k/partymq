package consumer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dnsx2k/partymq/pkg/rabbit"
	"github.com/dnsx2k/partymq/pkg/sender"
	"github.com/dnsx2k/partymq/pkg/transfer"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type consumerCtx struct {
	amqpOrchestrator rabbit.AmqpOrchestrator
	sender           sender.Sender
	transferSrv      transfer.Transferer
	logger           *zap.Logger
	source           string
	key              string
}

// New - creation function
func New(amqpOrch rabbit.AmqpOrchestrator, sender sender.Sender, transfer transfer.Transferer, logger *zap.Logger, source, key string) *consumerCtx {
	return &consumerCtx{
		amqpOrchestrator: amqpOrch,
		sender:           sender,
		transferSrv:      transfer,
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
	transferRequested := cs.transferSrv.WaitForTransfer()

	go func() {
		for {
			var err error
			select {
			case msg := <-msgs:
				if err = cs.process(msg); err != nil {
					cs.logger.Error("error occurred while processing messages", zap.String("priority", "low"), zap.Error(err))
				}
			case queue := <-transferRequested:
				if err = cs.transfer(queue); err != nil {
					cs.logger.Error("error occurred while performing transfer", zap.String("priority", "low"), zap.Error(err))
				}
			}
			// TODO: Break on graceful shutdown :)
		}
	}()

	return nil
}

// consume
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

// Consume - consumes messages from source queue, forwards to partition
func (cs *consumerCtx) get(queue string) error {
	ch, err := cs.amqpOrchestrator.GetChannel(rabbit.DirectionSub)
	if err != nil {
		return err
	}

	if err = ch.Qos(10, 0, false); err != nil {
		return err
	}

	fetchKey := fetchKeyFn(cs.source, cs.key)
	finished := false
	for {
		fn := func() error {
			msg, ok, err := ch.Get(queue, false)
			if err != nil {
				_ = msg.Ack(false)
				return err
			}
			finished = !ok

			key, err := fetchKey(&msg)
			if err != nil {
				_ = msg.Ack(false)
				return err
			}

			err, ok = cs.sender.Send(msg.Body, key)
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
		if err := fn(); err != nil {
			cs.logger.Error("error occurred while performing transfer", zap.String("priority", "low"), zap.Error(err))
		}
		if finished {
			break
		}
	}

	return nil
}

func (cs *consumerCtx) process(msg amqp.Delivery) error {
	fetchKey := fetchKeyFn(cs.source, cs.key)
	key, err := fetchKey(&msg)
	if err != nil {
		_ = msg.Ack(false)
		return err
	}

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

func (cs *consumerCtx) transfer(queue string) error {
	cs.logger.Info("transfer requested", zap.String("queue", queue))
	csm := func() {
		if err := cs.get(queue); err != nil {
			cs.logger.Error("error occurred while performing transfer", zap.String("priority", "high"), zap.Error(err))
		}
	}
	csm()

	return nil
}

func fetchKeyFn(source, key string) func(msg *amqp.Delivery) (string, error) {
	switch source {
	case "header":
		return func(msg *amqp.Delivery) (string, error) {
			h, ok := msg.Headers[key]
			if !ok {
				return "", fmt.Errorf("headers do not contains such key:%s", key)
			}
			keyStr, ok := h.(string)
			if !ok {
				return "", fmt.Errorf("value under: %s can not be converted into string", key)
			}
			return keyStr, nil
		}
	case "body":
		return func(msg *amqp.Delivery) (string, error) {
			m := map[string]any{}
			if err := json.Unmarshal(msg.Body, &m); err != nil {
				return "", err
			}
			keyStr, ok := m[key].(string)
			if !ok {
				return "", fmt.Errorf("value under: %s can not be converted into string", key)
			}
			return keyStr, nil
		}
	}

	return nil
}
