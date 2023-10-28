package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

type meta struct {
	bodyHash uint64
	key      string
	num      int
}

// PASSED
// (partitionCount, messageCount) (2, 500), (3, 5k), (3, 50k), (3, 100k), (3, 1m)
func main() {
	partitionStr := flag.String("partitions", "partition01;partition02;partition03", "partitions, separated by semicolon.")
	amqpCs := flag.String("amqpCs", "amqp://guest:guest@127.0.0.1:5672", "RabbitMQ connection string.")
	numOfMsg := flag.Int("numOfMsg", 1000000, "Number of messages")

	rabbitMq, err := amqp.Dial(*amqpCs)
	if err != nil {
		fmt.Println(err.Error())
	}

	ch, err := rabbitMq.Channel()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	partitions := strings.Split(*partitionStr, ";")

	messages := make(map[string][]meta, 0)
	for i := 0; i < len(partitions); i++ {
		m := make([]meta, 0)

		if err = ch.Qos(100, 0, false); err != nil {
			fmt.Println(err)
		}
		for {
			d, ok, err := ch.Get(partitions[i], true)
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

	if !dataIsOrdered(messages) {
		fmt.Println("Data is not ordered!")
	} else {
		fmt.Println("Data is ordered")
	}

	if !dataIsComplete(messages, *numOfMsg) {
		fmt.Println("Data incomplete!")
	} else {
		fmt.Println("Data is complete")
	}

	if !dataIsProperlyPartitioned(messages) {
		fmt.Println("Data is not partitioned")
	} else {
		fmt.Println("Data is partitioned")
	}

	if !dataIsUnique(messages) {
		fmt.Println("Data is not unique")
	} else {
		fmt.Println("Data is unique")
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
