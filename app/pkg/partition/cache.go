package partition

import (
	"errors"
	"hash/fnv"
	"math"
	"sync"
)

var anyClients = false

var (
	ErrClientNotFound = errors.New("client not found")
)

type Cache interface {
	GetRoutingKey(key string) (string, error)
	GetPartitions() []string

	AddPending(hostname, routingKey string) error
	AddReady(hostname string) error

	AnyClients() bool

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

func (cCtx *cacheCtx) GetRoutingKey(key string) (string, error) {
	cCtx.mutex.RLock()
	defer cCtx.mutex.RUnlock()

	if len(cCtx.clients) == 0 {
		return "", ErrClientNotFound
	}

	// return random partition if there's no key
	if key == "" {
		// map iteration will return different result each time, so we can consider as random partition
		for _, v := range cCtx.clients {
			return v, nil
		}
	}

	h, ok := cCtx.keys[key]
	if !ok {
		return "", nil
	}

	return cCtx.clients[h], nil
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

func (cCtx *cacheCtx) AddPending(hostname, routingKey string) error {
	cCtx.mutex.Lock()
	defer cCtx.mutex.Unlock()
	if _, alreadyPending := cCtx.pending[hostname]; alreadyPending {
		return errors.New("client already in pending status")
	}
	cCtx.pending[hostname] = routingKey

	return nil
}

func (cCtx *cacheCtx) AddReady(hostname string) error {
	cCtx.mutex.Lock()
	defer cCtx.mutex.Unlock()
	h := hash(hostname)
	if _, alreadyReady := cCtx.counter[h]; alreadyReady {
		return errors.New("client already in ready status")
	}

	routingKey, ok := cCtx.pending[hostname]
	if !ok {
		return errors.New("client not found in pending status")
	}

	cCtx.clients[h] = routingKey
	cCtx.counter[h] = 0
	cCtx.rebalance(h)

	delete(cCtx.pending, hostname)
	if !anyClients {
		anyClients = true
	}

	return nil
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
	if len(cCtx.clients) == 0 {
		anyClients = false
	}
}

func (cCtx *cacheCtx) rebalance(hash uint32) {
	cSum := 0
	for k := range cCtx.counter {
		cSum += cCtx.counter[k]
	}
	av := cSum / len(cCtx.clients)
	for k, id := range cCtx.keys {
		if av == 0 {
			break
		}
		cCtx.keys[k] = hash
		cCtx.counter[hash] += 1
		cCtx.counter[id] -= 1
		av--
	}
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
