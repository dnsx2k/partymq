package sender

import (
	"context"

	"github.com/dnsx2k/partymq/pkg/helpers"
	"github.com/dnsx2k/partymq/pkg/partition"
	"github.com/dnsx2k/partymq/pkg/rabbit"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type srvContext struct {
	cache       partition.Cache
	publishChan *amqp.Channel
	logger      *zap.Logger
}

type Sender interface {
	Send(ctx context.Context, msg []byte, headers amqp.Table, key string) error
	Ready() bool
}

// New - creation function for PartyOrchestrator
func New(cache partition.Cache, pubCh *amqp.Channel, logger *zap.Logger) (Sender, error) {
	return &srvContext{
		publishChan: pubCh,
		cache:       cache,
		logger:      logger,
	}, nil
}

func (srv *srvContext) Ready() bool {
	return srv.cache.AnyClients()
}

// Send - sends message on partition based on passed key
func (srv *srvContext) Send(ctx context.Context, msg []byte, headers amqp.Table, key string) error {
	routingKey, err := srv.cache.GetRoutingKey(key)
	if err != nil {
		return err
	}
	if routingKey == "" {
		routingKey = srv.cache.AssignToFreePartition(key)
	}

	pub := helpers.WrapAmqpPublishing(msg)
	pub.Headers = headers
	if err := srv.publishChan.PublishWithContext(ctx, rabbit.PartyMqExchange, routingKey, false, false, pub); err != nil {
		return err
	}

	return nil
}
