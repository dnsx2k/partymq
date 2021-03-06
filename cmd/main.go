package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dnsx2k/partymq/cmd/config"
	"github.com/dnsx2k/partymq/cmd/consumer"
	"github.com/dnsx2k/partymq/cmd/partitionhttphandler"
	"github.com/dnsx2k/partymq/pkg/rabbit"
	"github.com/dnsx2k/partymq/pkg/service"
	"github.com/gin-gonic/gin"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}

	var appCfg config.Config
	if err := envconfig.Process("partymq", &appCfg); err != nil {
		log.Fatal(err.Error())
	}

	amqpOrchestrator, err := rabbit.Init(appCfg.RabbitMqConnectionString)
	if err != nil {
		log.Fatal(err.Error())
	}

	if err = amqpOrchestrator.CreateResources(); err != nil {
		log.Fatal(err.Error())
	}

	partyOrchestrator, err := service.New(amqpOrchestrator, logger)
	if err != nil {
		log.Fatal(err.Error())
	}

	router := gin.Default()
	handler := partitionhttphandler.New(partyOrchestrator)
	handler.RegisterRoute(router)

	partyConsumer := consumer.New(appCfg.PartitionKey, amqpOrchestrator, partyOrchestrator)
	chErr, err := partyConsumer.Consume()
	if err != nil {
		logger.Fatal(err.Error())
	}

	go func() {
		for err = range chErr {
			logger.Error(err.Error())
		}
	}()

	hcInterval, err := time.ParseDuration(appCfg.HealthCheckInterval)
	if err != nil {
		log.Fatal(err.Error())
	}
	noConsumerTimeout, err := time.ParseDuration(appCfg.NoConsumerTimeout)
	if err != nil {
		log.Fatal(err.Error())
	}

	go partyOrchestrator.WatchHealth(hcInterval, noConsumerTimeout)

	go func() {
		if err := router.Run("127.0.0.1:8080"); err != nil {
			fmt.Println(err.Error())
		}
	}()

	//TODO: Graceful shutdown
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
