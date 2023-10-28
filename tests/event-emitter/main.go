package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

var idPool []string

var num = 1

func main() {
	num := flag.Int("n", 1000, "Number of messages to be emitted.")
	exchange := flag.String("exchangeName", "partymq.ex.source", "Name of the source exchange.")
	queue := flag.String("queueName", "partymq.ex.queue", "Name of the source queue.")
	amqpCs := flag.String("amqpCs", "amqp://guest:guest@127.0.0.1:5672", "RabbitMQ connection string.")
	flag.Parse()

	rabbitMq, err := amqp.Dial(*amqpCs)
	if err != nil {
		fmt.Println(err.Error())
	}

	ch, err := rabbitMq.Channel()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	err = ch.ExchangeDeclare(*exchange, amqp.ExchangeDirect, false, false, false, false, nil)
	if err != nil {
		fmt.Printf("Error ocurred while creating exchange: %s", err.Error())
		os.Exit(1)
	}
	_, err = ch.QueueDeclare(*queue, false, false, false, false, nil)
	if err != nil {
		fmt.Printf("Error ocurred while creating queue: %s", err.Error())
		os.Exit(1)
	}

	if err = ch.QueueBind(*queue, "#", *exchange, false, nil); err != nil {
		fmt.Printf("Error ocurred while binding queue: %s", err.Error())
		os.Exit(1)
	}

	idPool = make([]string, 50_000)
	for i := range idPool {
		idPool[i] = uuid.NewString()
	}

	start := time.Now()

	ctx := context.Background()
	for j := 0; j < *num; j++ {
		_ = emit(ctx, ch, *exchange)
	}

	// make sure that we sent exact amount of messages
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		q, err := ch.QueueDeclarePassive(*queue, false, false, false, false, nil)
		if err != nil {
			fmt.Printf("Error ocurred while inspecting queue: %s", err.Error())
			os.Exit(1)
		}

		if q.Messages != *num {
			missing := *num - q.Messages
			for j := 0; j < missing; j++ {
				_ = emit(ctx, ch, *exchange)
			}
		} else {
			break
		}
		time.Sleep(1 * time.Second)
	}

	elapsed := time.Since(start)
	fmt.Printf("Successfuly published: %v messages, elapsed: %s", *num, elapsed.String())
}

func emit(ctx context.Context, ch *amqp.Channel, exchange string) error {
	src := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(src)
	n := rnd.Intn(len(idPool) - 1)
	ID := idPool[n]

	msg := map[string]interface{}{
		"id":          ID,
		"num":         num,
		"name":        fmt.Sprintf("%s+random:%v", "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit"),
		"description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris vehicula interdum neque et sollicitudin. Nunc tincidunt sem ut nisi efficitur, eu ullamcorper augue ullamcorper. Proin iaculis arcu lectus, sed faucibus tortor aliquet vel. Nulla egestas purus erat, quis rhoncus lorem dictum fringilla. Maecenas quis ultricies justo, ac malesuada ligula. Nunc a ligula eget nisi elementum sodales. Curabitur eleifend sit amet ex a fringilla. Proin consectetur eu eros id mollis. Ut sed facilisis tellus, vel imperdiet odio. In pretium dolor lectus, vel iaculis tortor tincidunt sit amet. Vivamus varius nisi eu. ",
	}
	body, _ := json.Marshal(msg)
	err := ch.PublishWithContext(ctx, exchange, "#", false, false, amqp.Publishing{
		Body:    body,
		Headers: map[string]interface{}{"partitionKey": ID},
	})
	if err != nil {
		return err
	}
	num++

	return nil
}
