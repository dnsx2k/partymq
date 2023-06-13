package partitionhttphandler

import (
	"net/http"

	"github.com/dnsx2k/partymq/pkg/helpers"
	"github.com/dnsx2k/partymq/pkg/partition"
	"github.com/dnsx2k/partymq/pkg/rabbit"
	"github.com/dnsx2k/partymq/pkg/transfer"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type HandlerCtx struct {
	amqpOrchestrator rabbit.AmqpOrchestrator
	cache            partition.Cache
	transferService  transfer.Transferer
	logger           *zap.Logger
}

func New(amqpOrchestrator rabbit.AmqpOrchestrator, cache partition.Cache, transferSrv transfer.Transferer, logger *zap.Logger) *HandlerCtx {
	return &HandlerCtx{
		amqpOrchestrator: amqpOrchestrator, cache: cache, transferService: transferSrv, logger: logger,
	}
}

func (c *HandlerCtx) RegisterRoute(router gin.IRouter) {
	router.POST("/bind/:hostname", c.bindHandler)
	router.POST("/unbind/:hostname", c.unbindHandler)
}

func (c *HandlerCtx) bindHandler(cGin *gin.Context) {
	hostname := cGin.Param("hostname")

	p, err := c.amqpOrchestrator.InitPartition(hostname)
	if err != nil {
		cGin.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
	}
	c.cache.AddClient(hostname, p.Queue, p.RoutingKey)
	c.logger.Info("client bound to partition", zap.String("hostname", hostname), zap.String("queue_name", p.Queue))

	if err != nil {
		cGin.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	cGin.JSON(http.StatusOK, gin.H{"queue": p.Queue})
}

func (c *HandlerCtx) unbindHandler(cGin *gin.Context) {
	hostname := cGin.Param("hostname")
	c.transferService.Transfer(helpers.QueueFromHostname(hostname))

	cGin.Status(http.StatusOK)
}
