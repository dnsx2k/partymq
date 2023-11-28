package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ardanlabs/conf/v3"
	"github.com/dnsx2k/partymq/app/cmd/config"
	"github.com/dnsx2k/partymq/app/cmd/consumer"
	"github.com/dnsx2k/partymq/app/cmd/handlers"
	"github.com/dnsx2k/partymq/app/pkg/heartbeat"
	"github.com/dnsx2k/partymq/app/pkg/partition"
	rabbit2 "github.com/dnsx2k/partymq/app/pkg/rabbit"
	"github.com/dnsx2k/partymq/app/pkg/sender"
	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
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

	amqpOrchestrator, err := rabbit2.Init(appCfg.RabbitConnectionString, logger)
	if err != nil {
		log.Fatal(err.Error())
	}

	if err = amqpOrchestrator.CreateExchange(rabbit2.PartyMqExchange, amqp.ExchangeDirect); err != nil {
		log.Fatal(err.Error())
	}

	ch, err := amqpOrchestrator.GetChannel(rabbit2.DirectionPub)
	if err != nil {
		log.Fatal(err.Error())
	}

	cache := partition.NewCache()
	senderSrv, err := sender.New(cache, ch, logger)

	// AMQP

	partyConsumer := consumer.New(amqpOrchestrator, senderSrv, logger, appCfg.SourceQueue, appCfg.KeyConfig.Source, appCfg.KeyConfig.Key)
	doneCh := make(chan struct{})
	ctx := context.Background()
	// TODO: handle error in different way
	go func() {
		if err := partyConsumer.Consume(ctx, doneCh); err != nil {
			logger.Fatal(err.Error())
		}
	}()
	go partyConsumer.CheckState()

	hc := heartbeat.New(cache, logger)

	// HTTP

	router := gin.Default()
	handler := handlers.New(cache, hc, logger)
	handler.RegisterRoute(router)

	// HC
	router.Handle(http.MethodGet, "/health", func(c *gin.Context) {
		c.Status(http.StatusOK)
		fmt.Println("Service is healthy")
		return
	})

	go func() {
		if err := router.Run("0.0.0.0:8085"); err != nil {
			fmt.Println(err.Error())
		}
	}()

	shutdown(doneCh, logger, 10*time.Second)
}

// TODO: Improve
func shutdown(doneCh chan struct{}, logger *zap.Logger, timeout time.Duration) {
	interruptChan := make(chan os.Signal)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-interruptChan
	signal.Stop(interruptChan)

	doneCh <- struct{}{}

	<-time.After(timeout)

	logger.Info("Service exiting")
}
