package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/dnsx2k/party-mq/pkg/rabbit"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

const MaxClients int = 50

type SrvContext struct {
	amqpOrchestrator rabbit.AmqpOrchestrator

	// TODO: Check if keys map is assigned to default value or nil
	// holds key as key and clientID as value
	keys map[string]uuid.UUID

	partitionKey string

	clients map[uuid.UUID]Client
	uuids   []uuid.UUID

	publishChan *amqp.Channel
}

type PartyOrchestrator interface {
	BindClient(clientID string) (string, error)
	Send(msg []byte, key string) error
	WatchHealth(checkInterval, noConsumerTimeout time.Duration)
}

type Client struct {
	QueueName string
	RoutingKey string
}

func New(amqp rabbit.AmqpOrchestrator) (PartyOrchestrator, error){
	pubChan, err := amqp.GetChannel(rabbit.DirectionPub)
	if err != nil{
		return nil, err
	}
	return &SrvContext{
		amqpOrchestrator: amqp,
		publishChan:      pubChan,
		clients: make(map[uuid.UUID]Client),
		keys: make(map[string]uuid.UUID),
		uuids: make([]uuid.UUID, 0),
	}, nil
}

func(srv *SrvContext) BindClient(clientID string) (string, error){
	if len(srv.clients) >= MaxClients{
		return "", errors.New("max clients number already reached")
	}

	qName, err := srv.amqpOrchestrator.InitPartition(clientID)
	if err != nil{
		return "", err
	}
	log.Printf("client %s binded to partition: %s", clientID, qName)

	routingKey := fmt.Sprintf("party-mq-partition-key-%s", clientID)
	ID := uuid.New()
	srv.clients[ID] = Client{
		QueueName:  qName,
		RoutingKey: routingKey,
	}
	srv.uuids = append(srv.uuids, ID)

	return qName, nil
}

func(srv *SrvContext) UnbindClient(ID uuid.UUID){
	// TODO: CHECK IF RANGE COPIES
	m := srv.keys
	for key, val := range m{
		if ID == val{
			delete(srv.keys, key)
		}
	}
	delete(srv.clients, ID)

	index := 0
	for i := range srv.uuids{
		if srv.uuids[i] == ID{
			index = i
			break
		}
	}
	srv.uuids[index] = srv.uuids[len(srv.uuids)-1]
	srv.uuids = srv.uuids[:len(srv.uuids)-1]
}

func (srv *SrvContext) Send(msg []byte, key string) error {
	var routingKey string
	ID, ok := srv.keys[key]
	if !ok{
		index := getRandomInt(0, len(srv.clients) - 1)
		ID = srv.uuids[index]
		srv.keys[key] = ID
	}
	routingKey = srv.clients[ID].RoutingKey

	tmp := srv.amqpOrchestrator.GetMessageTemplate()
	tmp.Body = msg
	err := srv.publishChan.Publish(rabbit.PartyMqExchange, routingKey, false, false, tmp)
	if err != nil{
		return err
	}
	return nil
}

// WatchHealth
func (srv *SrvContext) WatchHealth(checkInterval, noConsumerTimeout time.Duration){
	ch, err := srv.amqpOrchestrator.GetChannel(rabbit.DirectionPrimary)
	if err != nil{
		fmt.Println(err.Error())
		return
	}

	for{
		time.Sleep(checkInterval)
		// TODO: Handle error or at least log it
		if len(srv.clients) == 0{
			continue
		}
		inspected, _ := srv.amqpOrchestrator.InspectQueues(srv.clients)
		for k, v := range inspected{
			if v.Consumers == 0{
				go func(cID uuid.UUID){
					time.Sleep(noConsumerTimeout)
					if c, ok := srv.clients[cID]; ok{
						q, _ := ch.QueueInspect(c.QueueName)
						if q.Consumers == 0{
							srv.UnbindClient(cID)
							// TODO: Log error while transferring msgs
							go srv.transfer(c.QueueName)
							return
						}
					}
				}(k)
			}
		}
	}
}

func (srv *SrvContext) transfer(sourceQueue string) (error, <-chan error) {
	subCh, err := srv.amqpOrchestrator.GetChannel(rabbit.DirectionSub)
	if err != nil{
		return err, nil
	}

	// TODO: Check prefetch size
	err = subCh.Qos(10, 0, false)
	if err != nil{
		return err, nil
	}

	msgs, err := subCh.Consume(sourceQueue, "party-mq-transfer", false, false, false, false, nil)
	if err != nil{
		return err, nil
	}

	errCh := make(chan error)
	go func() {
		for msg := range msgs{
			m := map[string]interface{}{}
			err := json.Unmarshal(msg.Body, &m)
			if err != nil{
				errCh <- err
			}

			key := m[srv.partitionKey].(string)
			if err = srv.Send(msg.Body, key); err != nil{
				errCh <- err
			}
			_ = msg.Ack(false)
		}
	}()

	return nil, errCh
}

func getRandomInt(min, max int) int{
	if min == 0 && max == 0{
		return 0
	}
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max - min + 1)
}