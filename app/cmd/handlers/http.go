package handlers

import (
	"net/http"

	"github.com/dnsx2k/partymq/app/pkg/heartbeat"
	"github.com/dnsx2k/partymq/app/pkg/helpers"
	"github.com/dnsx2k/partymq/app/pkg/partition"
	"github.com/dnsx2k/partymq/app/pkg/rabbit"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type HandlerCtx struct {
	cache     partition.Cache
	heartbeat heartbeat.HeartBeater
	logger    *zap.Logger
}

func New(cache partition.Cache, heartbeat heartbeat.HeartBeater, logger *zap.Logger) *HandlerCtx {
	return &HandlerCtx{
		cache:     cache,
		heartbeat: heartbeat,
		logger:    logger,
	}
}

func (c *HandlerCtx) RegisterRoute(router gin.IRouter) {
	router.POST("clients/:hostname/bind", c.bind)
	router.POST("clients/:hostname/ready", c.ready)
	router.POST("clients/:hostname/unbind", c.unbind)
	router.POST("clients/:hostname/heartbeat", c.beat)
}

func (c *HandlerCtx) bind(cGin *gin.Context) {
	hostname := cGin.Param("hostname")

	routingKey := helpers.BuildRoutingKey(hostname)
	if err := c.cache.AddPending(hostname, routingKey); err != nil {
		cGin.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.logger.Info("client requested a binding", zap.String("hostname", hostname), zap.String("routing_key", routingKey))

	cGin.JSON(http.StatusOK, gin.H{"routingKey": routingKey, "exchange": rabbit.PartyMqExchange})
}

func (c *HandlerCtx) ready(cGin *gin.Context) {
	hostname := cGin.Param("hostname")

	if err := c.cache.AddReady(hostname); err != nil {
		cGin.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": err.Error()})
		c.logger.Error("binding unsuccessful", zap.String("hostname", hostname))
		return
	}
	c.heartbeat.Beat(hostname)
	c.logger.Info("binding successful", zap.String("hostname", hostname))

	cGin.Status(http.StatusOK)
}

func (c *HandlerCtx) unbind(cGin *gin.Context) {
	hostname := cGin.Param("hostname")
	c.cache.Delete(hostname)

	cGin.Status(http.StatusOK)
}

func (c *HandlerCtx) beat(cGin *gin.Context) {
	hostname := cGin.Param("hostname")
	c.heartbeat.Beat(hostname)

	cGin.Status(http.StatusOK)
}
