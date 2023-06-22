package partition

import (
	"hash/fnv"
	"math"
	"sync"
)

var anyClients = false

type Cache interface {
	GetKey(key string) (Partition, bool)
	AnyClients() bool
	GetPartitions() []Partition
	AddClient(hostname, queueName, routingKey string)
	AssignToFreePartition(key string) Partition
	Delete(hostname string)
}

type cacheCtx struct {
	keys    map[string]uint32
	counter map[uint32]int
	clients map[uint32]Partition
	mutex   sync.RWMutex
}

func NewCache() Cache {
	cCtx := cacheCtx{
		keys:    make(map[string]uint32),
		counter: make(map[uint32]int),
		clients: make(map[uint32]Partition),
		mutex:   sync.RWMutex{},
	}

	return &cCtx
}

func (cCtx *cacheCtx) GetKey(key string) (Partition, bool) {
	cCtx.mutex.RLock()
	defer cCtx.mutex.RUnlock()
	h, ok := cCtx.keys[key]
	if !ok {
		return Partition{}, false
	}

	return cCtx.clients[h], true
}

func (cCtx *cacheCtx) AnyClients() bool {
	return anyClients
}

func (cCtx *cacheCtx) GetPartitions() []Partition {
	cCtx.mutex.RLock()
	defer cCtx.mutex.RUnlock()
	p := make([]Partition, 0)
	for _, v := range cCtx.clients {
		p = append(p, v)
	}
	return p
}

func (cCtx *cacheCtx) AddClient(hostname, queueName, routingKey string) {
	h := hash(hostname)
	cCtx.mutex.Lock()
	defer cCtx.mutex.Unlock()
	cCtx.clients[h] = Partition{
		QueueName:  queueName,
		RoutingKey: routingKey,
	}
	cCtx.counter[h] = 0

	anyClients = true
}

func (cCtx *cacheCtx) AssignToFreePartition(key string) Partition {
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
