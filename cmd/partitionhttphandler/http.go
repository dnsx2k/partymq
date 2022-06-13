package partitionhttphandler

import (
	"fmt"
	"net/http"

	"github.com/dnsx2k/partymq/pkg/service"
	"github.com/gin-gonic/gin"
)

type HandlerCtx struct {
	partyOrchestrator service.PartyOrchestrator
}

func New(partySrv service.PartyOrchestrator) *HandlerCtx {
	return &HandlerCtx{
		partyOrchestrator: partySrv,
	}
}

func (c *HandlerCtx) RegisterRoute(router gin.IRouter) {
	router.POST("/bind/:id", c.bindHandler)
	router.POST("/unbind/:id", c.unbindHandler)
}

func (c *HandlerCtx) bindHandler(cGin *gin.Context) {
	ID := cGin.Param("id")

	queue, err := c.partyOrchestrator.BindClient(ID)
	if err != nil {
		cGin.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	cGin.JSON(http.StatusOK, fmt.Sprintf("{\"queue\":%s}", queue))
}

func (c *HandlerCtx) unbindHandler(cGin *gin.Context) {
	ID := cGin.Param("id")

	c.partyOrchestrator.UnbindClient(ID)
	cGin.Status(http.StatusOK)
}
