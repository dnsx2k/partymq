package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ardanlabs/conf/v3"
	"github.com/dnsx2k/partymq/cmd/config"
	"github.com/dnsx2k/partymq/cmd/consumer"
	"github.com/dnsx2k/partymq/cmd/partitionhttphandler"
	"github.com/dnsx2k/partymq/pkg/mqguard"
	"github.com/dnsx2k/partymq/pkg/partition"
	"github.com/dnsx2k/partymq/pkg/rabbit"
	"github.com/dnsx2k/partymq/pkg/sender"
	"github.com/dnsx2k/partymq/pkg/transfer"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}

	var appCfg config.Config
	help, err := conf.Parse("PARTYMQ", &appCfg)
	if err != nil {
		if errors.Is(err, conf.ErrHelpWanted) {
			fmt.Println(help)
		}
		fmt.Printf("parsing config: %w", err)
		os.Exit(1)
	}

	amqpOrchestrator, err := rabbit.Init(appCfg.RabbitConnectionString)
	if err != nil {
		log.Fatal(err.Error())
	}

	if err = amqpOrchestrator.CreateResources(appCfg.Source.Exchange, appCfg.Source.Key); err != nil {
		log.Fatal(err.Error())
	}

	transferSrv := transfer.New(logger)
	cache := partition.NewCache(transferSrv)

	hcInterval, err := time.ParseDuration(appCfg.MqGuard.HealthCheckInterval)
	if err != nil {
		log.Fatal(err.Error())
	}
	noConsumerTimeout, err := time.ParseDuration(appCfg.MqGuard.NoConsumerTimeout)
	if err != nil {
		log.Fatal(err.Error())
	}
	guardian := mqguard.New(hcInterval, noConsumerTimeout, transferSrv, logger, cache, amqpOrchestrator)
	if err := guardian.Watch(); err != nil {
		log.Fatal(err.Error())
	}

	ch, err := amqpOrchestrator.GetChannel(rabbit.DirectionPub)
	if err != nil {
		log.Fatal(err.Error())
	}
	senderSrv, err := sender.New(cache, ch, logger)

	// AMQP

	partyConsumer := consumer.New(amqpOrchestrator, senderSrv, transferSrv, logger, appCfg.KeyConfig.Source, appCfg.KeyConfig.Key)
	if err := partyConsumer.Start(context.Background()); err != nil {
		logger.Fatal(err.Error())
	}

	// HTTP

	router := gin.Default()
	handler := partitionhttphandler.New(amqpOrchestrator, cache, transferSrv, logger)
	handler.RegisterRoute(router)

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
