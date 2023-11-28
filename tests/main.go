package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dnsx2k/partymq/tests/pkg/scenarios"
	"github.com/dnsx2k/partymq/tests/pkg/test"
	"github.com/dnsx2k/partymq/tests/pkg/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

var testScenarios = map[string]utils.Scenario{
	"simple": scenarios.Simple,
}

func main() {
	// TODO: Replace with partymq healthcheck configured in docker-compose file
	fmt.Println("Sleep...")
	time.Sleep(15 * time.Second)
	ctx := context.Background()
	amqpCs := os.Getenv("RABBIT_CS")

	conn, err := amqp.Dial(amqpCs)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	utils.InitEmitterResources(ch)
	for k, v := range testScenarios {
		utils.Seed(ctx, ch, v.MessageVolume, v.IdPoolSize)

		start := time.Now()
		v.ExecuteScenarioFn(ch)
		fmt.Printf("Executed scenario: %s, with %v messages and %v of ID pool size. In %v", k, v.MessageVolume, v.IdPoolSize, time.Since(start).Seconds())

		result := test.Execute(ch, v.Partitions, v.MessageVolume)
		fmt.Printf("Complete: %v, Ordered: %v, Unique: %v, Partitioned: %v", result.DataComplete, result.DataOrdered, result.DataUniqe, result.DataProperlyPartitioned)
	}
}
