package heartbeat

import (
	"sync"
	"time"

	"github.com/dnsx2k/partymq/pkg/partition"
	"go.uber.org/zap"
)

// TODO: make it configurable
var expireAfter = 120 * time.Second
var checkInterval = 30 * time.Second

type HeartBeater interface {
	Beat(hostname string)
}

type srvContext struct {
	cache  partition.Cache
	expiry map[string]time.Time
	mutex  sync.Mutex
	logger *zap.Logger
}

func New(cache partition.Cache, logger *zap.Logger) HeartBeater {
	srvCtx := srvContext{
		cache:  cache,
		expiry: make(map[string]time.Time),
		mutex:  sync.Mutex{},
		logger: logger,
	}
	go func() {
		for {
			time.Sleep(checkInterval)
			srvCtx.check()
		}
	}()

	return &srvCtx
}

func (srv *srvContext) Beat(hostname string) {
	srv.mutex.Lock()
	defer srv.mutex.Unlock()

	srv.expiry[hostname] = time.Now().Add(expireAfter)
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
