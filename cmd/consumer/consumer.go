package consumer

import (
	"encoding/json"

	"github.com/dnsx2k/party-mq/pkg/rabbit"
	"github.com/dnsx2k/party-mq/pkg/service"
)

type Ctx struct {
	amqpOrchestrator rabbit.AmqpOrchestrator
	partyOrchestrator service.PartyOrchestrator
	partitionKey string
}

func New(partitionKey string, amqpOrch rabbit.AmqpOrchestrator, partyOrch service.PartyOrchestrator) *Ctx {
	return &Ctx{
		partitionKey: partitionKey,
		amqpOrchestrator: amqpOrch,
		partyOrchestrator: partyOrch,
	}
}

func (cs *Ctx) Consume() (<- chan error, error){
	subChan, err := cs.amqpOrchestrator.GetChannel(rabbit.DirectionSub)
	if err != nil{
		return nil, err
	}

	if err = subChan.Qos(10, 0, false); err != nil{
		return nil, err
	}

	msgChan, err := subChan.Consume(rabbit.PartyMqBufferQueue, "partymq", false, false, false, false, nil)
	if err != nil{
		return nil, err
	}

	errChan := make(chan error)
	go func(){
		m := map[string]interface{}{}
		for msg := range msgChan{
			if err := json.Unmarshal(msg.Body, &m); err != nil{
				errChan <- err
			}
			key := m[cs.partitionKey].(string)
			if err = cs.partyOrchestrator.Send(msg.Body, key); err != nil{
				errChan <- err
			}
			if err = msg.Ack(false); err != nil{
				errChan <- err
			}
		}
	}()

	return errChan, nil
}