package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dnsx2k/partymq/tests/pkg/scenarios"
	"github.com/dnsx2k/partymq/tests/pkg/test"
	"github.com/dnsx2k/partymq/tests/pkg/utils"
	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
)

var testScenarios = map[string]utils.Scenario{
	"simple": scenarios.Simple,
}

func main() {
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

	router := gin.Default()
	router.Handle(http.MethodPost, "/run", func(c *gin.Context) {
		for k, v := range testScenarios {
			utils.Seed(c, ch, v.MessageVolume, v.IdPoolSize)

			start := time.Now()
			v.ExecuteScenarioFn(ch)
			fmt.Printf("Executed scenario: %s, with %v messages and %v of ID pool size. In %v", k, v.MessageVolume, v.IdPoolSize, time.Since(start).Seconds())

			result := test.Execute(ch, v.Partitions, v.MessageVolume)
			c.JSON(http.StatusOK, map[string]any{
				"complete": result.DataComplete, "ordered": result.DataOrdered, "unique": result.DataUniqe, "partitioned": result.DataProperlyPartitioned,
			})
		}
		c.Status(200)
	})
	go func() {
		if err := router.Run("0.0.0.0:8080"); err != nil {
			fmt.Println(err.Error())
		}
	}()

	interruptChan := make(chan os.Signal)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-interruptChan
}
