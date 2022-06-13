package consumer

import (
	"encoding/json"

	"github.com/dnsx2k/partymq/pkg/rabbit"
	"github.com/dnsx2k/partymq/pkg/service"
)

type consumerCtx struct {
	amqpOrchestrator  rabbit.AmqpOrchestrator
	partyOrchestrator service.PartyOrchestrator
	partitionKey      string
}

func New(partitionKey string, amqpOrch rabbit.AmqpOrchestrator, partyOrch service.PartyOrchestrator) *consumerCtx {
	return &consumerCtx{
		partitionKey:      partitionKey,
		amqpOrchestrator:  amqpOrch,
		partyOrchestrator: partyOrch,
	}
}

func (cs *consumerCtx) Consume() (<-chan error, error) {
	ch, err := cs.amqpOrchestrator.GetChannel(rabbit.DirectionSub)
	if err != nil {
		return nil, err
	}

	if err = ch.Qos(10, 0, false); err != nil {
		return nil, err
	}
	msgChan, err := ch.Consume(rabbit.PartyMqBufferQueue, "partymq", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	errChan := make(chan error)
	go func() {
		m := map[string]any{}
		for msg := range msgChan {
			if err := json.Unmarshal(msg.Body, &m); err != nil {
				errChan <- err
			}
			key := m[cs.partitionKey].(string)
			if err = cs.partyOrchestrator.Send(msg.Body, key); err != nil {
				errChan <- err
			}
			if err = msg.Ack(false); err != nil {
				errChan <- err
			}
		}
	}()

	return errChan, nil
}
