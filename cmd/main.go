package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ardanlabs/conf/v3"
	"github.com/dnsx2k/partymq/cmd/clientshttphandler"
	"github.com/dnsx2k/partymq/cmd/config"
	"github.com/dnsx2k/partymq/cmd/consumer"
	"github.com/dnsx2k/partymq/pkg/heartbeat"
	"github.com/dnsx2k/partymq/pkg/partition"
	"github.com/dnsx2k/partymq/pkg/rabbit"
	"github.com/dnsx2k/partymq/pkg/sender"
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

	amqpOrchestrator, err := rabbit.Init(appCfg.RabbitConnectionString, logger)
	if err != nil {
		log.Fatal(err.Error())
	}

	if err = amqpOrchestrator.CreateResources(appCfg.Source.Exchange, appCfg.Source.Key); err != nil {
		log.Fatal(err.Error())
	}

	ch, err := amqpOrchestrator.GetChannel(rabbit.DirectionPub)
	if err != nil {
		log.Fatal(err.Error())
	}

	cache := partition.NewCache()
	senderSrv, err := sender.New(cache, ch, logger)

	// AMQP

	partyConsumer := consumer.New(amqpOrchestrator, senderSrv, logger, appCfg.KeyConfig.Source, appCfg.KeyConfig.Key)
	doneCh := make(chan struct{})
	ctx := context.Background()
	if err := partyConsumer.Start(ctx, doneCh); err != nil {
		logger.Fatal(err.Error())
	}

	hc := heartbeat.New(cache, logger)

	// HTTP

	router := gin.Default()
	handler := clientshttphandler.New(cache, hc, logger)
	handler.RegisterRoute(router)

	go func() {
		if err := router.Run("127.0.0.1:8080"); err != nil {
			fmt.Println(err.Error())
		}
	}()

	shutdown(doneCh, 10*time.Second)
}

func shutdown(doneCh chan struct{}, timeout time.Duration) {
	interruptChan := make(chan os.Signal)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-interruptChan
	signal.Stop(interruptChan)

	doneCh <- struct{}{}

	// TODO: Do it with context
	<-time.After(timeout)
}
