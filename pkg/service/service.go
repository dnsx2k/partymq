package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/dnsx2k/partymq/pkg/rabbit"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

const MaxClients int = 50

type srvContext struct {
	logger           *zap.Logger
	amqpOrchestrator rabbit.AmqpOrchestrator
	partitionKey     string
	clients          map[string]Client
	publishChan      *amqp.Channel
}

// PartyOrchestrator interface
type PartyOrchestrator interface {
	BindClient(clientID string) (string, error)
	UnbindClient(ID string)
	Send(msg []byte, key string) error
	WatchHealth(checkInterval, noConsumerTimeout time.Duration)
}

// New - creation function for PartyOrchestrator
func New(amqp rabbit.AmqpOrchestrator, logger *zap.Logger) (PartyOrchestrator, error) {
	ch, err := amqp.GetChannel(rabbit.DirectionPub)
	if err != nil {
		return nil, err
	}
	return &srvContext{
		amqpOrchestrator: amqp,
		logger:           logger,
		publishChan:      ch,
		clients:          make(map[string]Client),
	}, nil
}

// BindClient - creates new client, with bounded partition
func (srv *srvContext) BindClient(clientID string) (string, error) {
	if len(srv.clients) >= MaxClients {
		return "", errors.New("max clients number already reached")
	}

	qName, err := srv.amqpOrchestrator.InitPartition(clientID)
	if err != nil {
		return "", err
	}
	srv.logger.Info("client bound to partition", zap.String("id", clientID), zap.String("queue_name", qName))

	routingKey := fmt.Sprintf("party-mq-partition-key-%s", clientID)
	srv.clients[clientID] = Client{
		queueName:  qName,
		routingKey: routingKey,
		keys:       map[string]struct{}{},
	}

	return qName, nil
}

// UnbindClient - unbinds client, after messages won't be proxied through that client
func (srv *srvContext) UnbindClient(ID string) {
	delete(srv.clients, ID)
	srv.logger.Info("client unbound", zap.String("id", ID))
}

// Send - sends message on partition based on passed key
func (srv *srvContext) Send(msg []byte, key string) error {
	var routingKey string
	ok, ID := srv.containsKey(key)
	if !ok {
		ID := srv.getRandomClientID()
		srv.clients[ID].keys[key] = struct{}{}
	}
	routingKey = srv.clients[ID].routingKey

	tmp := srv.amqpOrchestrator.GetMessageTemplate()
	tmp.Body = msg
	if err := srv.publishChan.Publish(rabbit.PartyMqExchange, routingKey, false, false, tmp); err != nil {
		return err
	}

	return nil
}

// WatchHealth - interval checks whether any of queue got consumer, if not messages are transferred into another active client
func (srv *srvContext) WatchHealth(checkInterval, noConsumerTimeout time.Duration) {
	ch, err := srv.amqpOrchestrator.GetChannel(rabbit.DirectionPrimary)
	if err != nil {
		srv.logger.Error(err.Error())
		return
	}

	for {
		time.Sleep(checkInterval)
		if len(srv.clients) == 0 {
			continue
		}
		queues := make(map[string]string, 0)
		for k, v := range srv.clients {
			queues[k] = v.queueName
		}
		inspected, _ := srv.amqpOrchestrator.InspectQueues(queues)
		for k, v := range inspected {
			if v.Consumers == 0 {
				go func(cID string) {
					time.Sleep(noConsumerTimeout)
					if c, ok := srv.clients[cID]; ok {
						q, _ := ch.QueueInspect(c.queueName)
						if q.Consumers == 0 {
							srv.UnbindClient(cID)
							if err = srv.transfer(c.queueName); err != nil {
								srv.logger.Error("error occurred while transferring messages", zap.Error(err))
							}
							return
						}
					}
				}(k)
			}
		}
	}
}

func (srv *srvContext) transfer(sourceQueue string) error {
	subCh, err := srv.amqpOrchestrator.GetChannel(rabbit.DirectionSub)
	if err != nil {
		return err
	}

	err = subCh.Qos(10, 0, false)
	if err != nil {
		return err
	}

	msgs, err := subCh.Consume(sourceQueue, "party-mq-transfer", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for msg := range msgs {
		m := map[string]any{}
		err := json.Unmarshal(msg.Body, &m)
		if err != nil {
			return err
		}

		key := m[srv.partitionKey].(string)
		if err = srv.Send(msg.Body, key); err != nil {
			return err
		}
		_ = msg.Ack(false)
	}

	return nil
}

func getRandomInt(min, max int) int {
	if min == 0 && max == 0 {
		return 0
	}
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max - min + 1)
}

func (srv *srvContext) containsKey(key string) (bool, string) {
	for clientID, _ := range srv.clients {
		if _, ok := srv.clients[clientID].keys[key]; ok {
			return true, clientID
		}
	}
	return false, ""
}

func (srv *srvContext) getRandomClientID() string {
	numOfClients := 0
	indexMap := map[int]string{}
	for k, _ := range srv.clients {
		numOfClients++
		indexMap[numOfClients] = k
	}
	random := getRandomInt(0, numOfClients-1)
	return indexMap[random]
}
