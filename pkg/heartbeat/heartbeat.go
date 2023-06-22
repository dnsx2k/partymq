package heartbeat

import (
	"sync"
	"time"

	"github.com/dnsx2k/partymq/pkg/partition"
)

// TODO: make it configurable
var expireAfter = 120 * time.Second
var checkInterval = 30 * time.Second

type HeartBeater interface {
	Beat(hostname string)
}

type srvContext struct {
	cache partition.Cache
	times map[string]time.Time
	mutex sync.Mutex
}

func New(cache partition.Cache) HeartBeater {
	srvCtx := srvContext{
		cache: cache,
		times: make(map[string]time.Time),
		mutex: sync.Mutex{},
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

	srv.times[hostname] = time.Now().Add(expireAfter)
}

func (srv *srvContext) check() {
	srv.mutex.Lock()
	defer srv.mutex.Unlock()

	now := time.Now()
	for hostname, expiry := range srv.times {
		if now.After(expiry) {
			srv.cache.Delete(hostname)
		}
		delete(srv.times, hostname)
	}
}
