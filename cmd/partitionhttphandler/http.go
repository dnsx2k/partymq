package partitionhttphandler

import (
	"fmt"
	"net/http"

	"github.com/dnsx2k/party-mq/pkg/service"
	"github.com/gin-gonic/gin"
)

type HandlerCtx struct {
	partyOrchestrator service.PartyOrchestrator
}

func New(partySrv service.PartyOrchestrator) *HandlerCtx{
	return &HandlerCtx{
		partyOrchestrator: partySrv,
	}
}

func (c *HandlerCtx)RegisterRoute(router gin.IRouter) {
	router.POST("/bind/:id", c.bindHandler)
}

func (c *HandlerCtx) bindHandler(cGin *gin.Context){
	ID := cGin.Param("id")

	queue, err := c.partyOrchestrator.BindClient(ID)
	if err != nil{
		cGin.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	cGin.JSON(http.StatusOK, fmt.Sprintf("{\"queue\":%s}", queue))
}