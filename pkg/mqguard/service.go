package mqguard

import (
	"time"

	"github.com/dnsx2k/partymq/pkg/partition"
	"github.com/dnsx2k/partymq/pkg/rabbit"
	"github.com/dnsx2k/partymq/pkg/transfer"
	"go.uber.org/zap"
)

type srvContext struct {
	logger           *zap.Logger
	amqpOrchestrator rabbit.AmqpOrchestrator
	transferSrv      transfer.Transferer
	cache            partition.Cache
}

type Watcher interface {
	Watch() error
}

var checkInterval time.Duration
var consumerTimeout time.Duration

func New(queueCheckInterval, noConsumerTimeout time.Duration, transfer transfer.Transferer, logger *zap.Logger, cache partition.Cache, amqpOrchestrator rabbit.AmqpOrchestrator) *srvContext {
	checkInterval = queueCheckInterval
	consumerTimeout = noConsumerTimeout

	return &srvContext{
		logger:           logger,
		transferSrv:      transfer,
		amqpOrchestrator: amqpOrchestrator,
		cache:            cache,
	}
}

// Watch - interval checks whether any of queue got consumer, if not messages are transferred into another active client
func (srv *srvContext) Watch() error {
	ch, err := srv.amqpOrchestrator.GetChannel(rabbit.DirectionPrimary)
	if err != nil {
		return err
	}

	go func() {
		for {
			// Consider checking if next watch execution is after or before, delete time Sleep, low prio for now
			time.Sleep(checkInterval)
			partitions := srv.cache.GetPartitions()
			if len(partitions) == 0 {
				continue
			}
			queues := make([]string, len(partitions))
			for i := range partitions {
				queues[i] = partitions[i].QueueName
			}
			inspected, _ := srv.amqpOrchestrator.InspectQueues(queues)
			for k, v := range inspected {
				if v.Consumers == 0 {
					// Consider to use wait group to wait for all routines that waits for consumer to appear, high prio
					go func(queue string) {
						time.Sleep(consumerTimeout)
						q, _ := ch.QueueInspect(queue)
						if q.Consumers == 0 {
							srv.transferSrv.Transfer(q.Name)
						}
					}(k)
				}
			}
		}
	}()

	return nil
}
