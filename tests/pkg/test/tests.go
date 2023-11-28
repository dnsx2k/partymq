package test

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"

	amqp "github.com/rabbitmq/amqp091-go"
)

type meta struct {
	bodyHash uint64
	key      string
	num      int
}

type Result struct {
	DataComplete            bool
	DataOrdered             bool
	DataUniqe               bool
	DataProperlyPartitioned bool
}

func Execute(channel *amqp.Channel, partitions []string, messagesVolume int) Result {
	messages := make(map[string][]meta, 0)
	for i := 0; i < len(partitions); i++ {
		m := make([]meta, 0)

		if err := channel.Qos(100, 0, false); err != nil {
			fmt.Println(err)
		}
		for {
			d, ok, err := channel.Get(partitions[i], true)
			if !ok {
				break
			}
			if err != nil {
				fmt.Println(err)
			}

			msg := map[string]any{}
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				fmt.Println()
			}
			bString := string(d.Body)

			num := msg["num"].(float64)
			metaData := meta{
				bodyHash: hash(bString),
				key:      d.Headers["partitionKey"].(string),
				num:      int(num),
			}
			m = append(m, metaData)
		}
		messages[partitions[i]] = m
	}

	return Result{
		DataComplete:            dataIsComplete(messages, messagesVolume),
		DataOrdered:             dataIsOrdered(messages),
		DataUniqe:               dataIsUnique(messages),
		DataProperlyPartitioned: dataIsProperlyPartitioned(messages),
	}
}

func dataIsComplete(m map[string][]meta, numOfEvents int) bool {
	total := 0
	for _, v := range m {
		total += len(v)
	}
	fmt.Println(total)
	return total == numOfEvents
}

func dataIsProperlyPartitioned(m map[string][]meta) bool {
	// load all keys into maps for given partiton
	pKeys := make(map[string]map[string]struct{})
	for k, _ := range m {
		p := make(map[string]struct{})
		for i := range m[k] {
			p[m[k][i].key] = struct{}{}
		}
		pKeys[k] = p
	}

	for partitionName, _ := range pKeys {
		fmt.Println(partitionName)
		for partitionKey, _ := range pKeys[partitionName] {
			for partitionName2, _ := range pKeys {
				if partitionName2 == partitionName {
					continue
				}
				_, exists := pKeys[partitionName2][partitionKey]
				if exists {
					fmt.Println("Doubled")
					return false
				}
			}
		}
	}

	return true
}

func dataIsOrdered(m map[string][]meta) bool {
	for k, _ := range m {
		ordered := make([]int, len(m[k]))
		for i := range m[k] {
			ordered[i] = m[k][i].num
		}
		sort.Ints(ordered)

		for i := range m[k] {
			if m[k][i].num != ordered[i] {
				return false
			}
		}
	}

	return true
}

func dataIsUnique(m map[string][]meta) bool {
	hashes := make(map[uint64]struct{}, 0)
	for k, _ := range m {
		for i := range m[k] {
			_, exists := hashes[m[k][i].bodyHash]
			if exists {
				return false
			}
			hashes[m[k][i].bodyHash] = struct{}{}
		}
	}
	return true
}

func hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
