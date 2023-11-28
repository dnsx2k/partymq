package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	queue    = "partymq.q.source"
	exchange = "partymq.ex.source"
)

func InitEmitterResources(channel *amqp.Channel) {
	CreateExchange(channel, exchange)
	CreateQueue(channel, queue)
	BindQueue(channel, queue, "#", exchange)
}

func Seed(ctx context.Context, channel *amqp.Channel, volume, idPoolSize int) {
	start := time.Now()
	pool := make([]string, idPoolSize)
	for i := range pool {
		pool[i] = uuid.NewString()
	}

	counter := 0
	for i := 0; i < volume; i++ {
		msg := build(pool[random(idPoolSize)], counter)
		if err := channel.PublishWithContext(ctx, exchange, "#", false, false, msg); err != nil {
			fmt.Println(err)
		}
		counter++
	}
	time.Sleep(3 * time.Second)
	for i := 0; i < 3; i++ {
		alreadyOnQueue := InspectNumberOfMessages(channel, queue)
		if alreadyOnQueue != volume {
			missing := volume - alreadyOnQueue
			for j := 0; j < missing; j++ {
				msg := build(pool[random(idPoolSize)], counter)
				if err := channel.PublishWithContext(ctx, exchange, "#", false, false, msg); err != nil {
					fmt.Println(err)
				}
				counter++
			}
		} else {
			break
		}
		time.Sleep(1 * time.Second)
	}
	elapsed := time.Since(start)
	fmt.Printf("Successfuly published: %v messages, elapsed: %s", volume, elapsed.String())
}

func build(ID string, n int) amqp.Publishing {
	msg := map[string]interface{}{
		"id":          ID,
		"num":         n,
		"name":        fmt.Sprintf("%s+random:%v", "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit"),
		"description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris vehicula interdum neque et sollicitudin. Nunc tincidunt sem ut nisi efficitur, eu ullamcorper augue ullamcorper. Proin iaculis arcu lectus, sed faucibus tortor aliquet vel. Nulla egestas purus erat, quis rhoncus lorem dictum fringilla. Maecenas quis ultricies justo, ac malesuada ligula. Nunc a ligula eget nisi elementum sodales. Curabitur eleifend sit amet ex a fringilla. Proin consectetur eu eros id mollis. Ut sed facilisis tellus, vel imperdiet odio. In pretium dolor lectus, vel iaculis tortor tincidunt sit amet. Vivamus varius nisi eu. ",
	}
	body, _ := json.Marshal(msg)

	return amqp.Publishing{
		Headers: map[string]interface{}{"partitionKey": ID},
		Body:    body,
	}
}

func random(poolVolume int) int {
	src := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(src)
	return rnd.Intn(poolVolume - 1)
}
