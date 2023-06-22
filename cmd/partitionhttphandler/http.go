package partitionhttphandler

import (
	"net/http"

	"github.com/dnsx2k/partymq/pkg/heartbeat"
	"github.com/dnsx2k/partymq/pkg/helpers"
	"github.com/dnsx2k/partymq/pkg/partition"
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
		cache: cache, logger: logger,
		heartbeat: heartbeat,
	}
}

func (c *HandlerCtx) RegisterRoute(router gin.IRouter) {
	router.POST("/:hostname/bind", c.bind)
	router.POST("/:hostname/ready", c.ready)
	router.POST("/heartbeat/:hostname")
	router.POST("/unbind/:hostname", c.unbind)
}

func (c *HandlerCtx) bind(cGin *gin.Context) {
	hostname := cGin.Param("hostname")

	routingKey := helpers.BuildRoutingKey(hostname)
	c.cache.AddPending(hostname, routingKey)
	c.logger.Info("client requested a binding", zap.String("hostname", hostname), zap.String("routing_key", routingKey))

	cGin.JSON(http.StatusOK, gin.H{"routingKey": routingKey})
}

func (c *HandlerCtx) ready(cGin *gin.Context) {
	hostname := cGin.Param("hostname")

	if ok := c.cache.AddClient(hostname); !ok {
		// TODO: Precise error message
		cGin.AbortWithStatusJSON(http.StatusBadRequest, "no such a hostname among pending clients")
		return
	}
	c.heartbeat.Beat(hostname)
	c.logger.Info("client successfully bound", zap.String("hostname", hostname))

	cGin.Status(http.StatusOK)
}

func (c *HandlerCtx) beat(cGin *gin.Context) {
	hostname := cGin.Param("hostname")
	c.heartbeat.Beat(hostname)

	cGin.Status(http.StatusOK)
}

func (c *HandlerCtx) unbind(cGin *gin.Context) {
	hostname := cGin.Param("hostname")
	c.cache.Delete(hostname)

	cGin.Status(http.StatusOK)
}
