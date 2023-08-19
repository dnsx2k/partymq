package partition

import (
	"hash/fnv"
	"math"
	"sync"
)

var anyClients = false

type Cache interface {
	GetKey(key string) (string, bool)
	AnyClients() bool
	GetPartitions() []string
	AddClient(hostname string) bool
	AddPending(hostname, routingKey string)
	AssignToFreePartition(key string) string
	Delete(hostname string)
}

type cacheCtx struct {
	keys    map[string]uint32
	counter map[uint32]int
	clients map[uint32]string
	pending map[string]string
	mutex   sync.RWMutex
}

func NewCache() Cache {
	cCtx := cacheCtx{
		keys:    make(map[string]uint32),
		counter: make(map[uint32]int),
		clients: make(map[uint32]string),
		pending: make(map[string]string),
		mutex:   sync.RWMutex{},
	}

	return &cCtx
}

func (cCtx *cacheCtx) GetKey(key string) (string, bool) {
	cCtx.mutex.RLock()
	defer cCtx.mutex.RUnlock()

	// return random partition if there's no key
	if key == "" {
		// map iteration will return different result each time, so we can consider as random partition
		for _, v := range cCtx.clients {
			return v, true
		}
	}

	h, ok := cCtx.keys[key]
	if !ok {
		return "", false
	}

	return cCtx.clients[h], true
}

func (cCtx *cacheCtx) AnyClients() bool {
	return anyClients
}

func (cCtx *cacheCtx) GetPartitions() []string {
	cCtx.mutex.RLock()
	defer cCtx.mutex.RUnlock()
	p := make([]string, 0)
	for _, v := range cCtx.clients {
		p = append(p, v)
	}
	return p
}

func (cCtx *cacheCtx) AddClient(hostname string) bool {
	cCtx.mutex.Lock()
	defer cCtx.mutex.Unlock()
	routingKey, ok := cCtx.pending[hostname]
	if !ok {
		return false
	}
	h := hash(hostname)
	cCtx.clients[h] = routingKey
	cCtx.counter[h] = 0
	delete(cCtx.pending, hostname)

	anyClients = true

	return true
}

func (cCtx *cacheCtx) AddPending(hostname, routingKey string) {
	cCtx.mutex.Lock()
	defer cCtx.mutex.Unlock()
	cCtx.pending[hostname] = routingKey

	anyClients = true
}

func (cCtx *cacheCtx) AssignToFreePartition(key string) string {
	c := math.MaxInt
	cCtx.mutex.Lock()
	defer cCtx.mutex.Unlock()
	var h uint32
	for k, v := range cCtx.counter {
		if c > v {
			c = v
			h = k
		}
	}
	cCtx.keys[key] = h
	cCtx.counter[h]++

	return cCtx.clients[h]
}

func (cCtx *cacheCtx) Delete(hostname string) {
	cCtx.delete(hostname)
}

func (cCtx *cacheCtx) delete(hostname string) {
	h := hash(hostname)
	cCtx.mutex.Lock()
	defer cCtx.mutex.Unlock()
	delete(cCtx.clients, h)
	for k, v := range cCtx.keys {
		if v == h {
			delete(cCtx.keys, k)
		}
	}
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
