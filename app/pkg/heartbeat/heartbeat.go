package heartbeat

import (
	"sync"
	"time"

	"github.com/dnsx2k/partymq/app/pkg/partition"
	"go.uber.org/zap"
)

type HeartBeater interface {
	Beat(hostname string)
}

type srvContext struct {
	cache     partition.Cache
	expiry    map[string]time.Time
	mutex     sync.Mutex
	logger    *zap.Logger
	clientTTL time.Duration
}

func New(cache partition.Cache, logger *zap.Logger, clientTTL, checkInterval time.Duration) HeartBeater {
	srvCtx := srvContext{
		cache:     cache,
		expiry:    make(map[string]time.Time),
		mutex:     sync.Mutex{},
		logger:    logger,
		clientTTL: clientTTL,
	}
	go func() {
		for {
			<-time.After(checkInterval)
			srvCtx.check()
		}
	}()

	return &srvCtx
}

func (srv *srvContext) Beat(hostname string) {
	srv.mutex.Lock()
	defer srv.mutex.Unlock()

	srv.expiry[hostname] = time.Now().Add(srv.clientTTL)
}

func (srv *srvContext) check() {
	srv.mutex.Lock()
	defer srv.mutex.Unlock()

	now := time.Now()
	for hostname, expiry := range srv.expiry {
		if now.After(expiry) {
			srv.cache.Delete(hostname)
			delete(srv.expiry, hostname)
			srv.logger.Info("client expired", zap.String("hostname", hostname))
		}
	}
}
