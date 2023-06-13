package sender

import (
	"github.com/dnsx2k/partymq/pkg/helpers"
	"github.com/dnsx2k/partymq/pkg/partition"
	"github.com/dnsx2k/partymq/pkg/rabbit"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type srvContext struct {
	cache       partition.Cache
	publishChan *amqp.Channel
	logger      *zap.Logger
}

type Sender interface {
	Send(msg []byte, key string) (error, bool)
}

// New - creation function for PartyOrchestrator
func New(cache partition.Cache, pubCh *amqp.Channel, logger *zap.Logger) (Sender, error) {
	return &srvContext{
		publishChan: pubCh,
		cache:       cache,
		logger:      logger,
	}, nil
}

// Send - sends message on partition based on passed key
func (srv *srvContext) Send(msg []byte, key string) (error, bool) {
	part, ok := srv.cache.GetKey(key)
	if !ok {
		part = srv.cache.AssignToFreePartition(key)
	}

	pub := helpers.WrapAmqpPublishing(msg)
	if err := srv.publishChan.Publish(rabbit.PartyMqExchange, part.RoutingKey, false, false, pub); err != nil {
		return err, true
	}

	return nil, true
}
