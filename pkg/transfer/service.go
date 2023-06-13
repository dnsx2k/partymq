package transfer

import (
	"go.uber.org/zap"
)

type srvContext struct {
	logger    *zap.Logger
	listeners []chan string
}

type Transferer interface {
	WaitForTransfer() chan string
	Transfer(queue string)
}

func New(logger *zap.Logger) Transferer {
	return &srvContext{
		logger:    logger,
		listeners: make([]chan string, 0),
	}
}

func (srvCtx *srvContext) WaitForTransfer() chan string {
	listener := make(chan string)
	srvCtx.listeners = append(srvCtx.listeners, listener)
	return listener
}

func (srvCtx *srvContext) Transfer(queue string) {
	srvCtx.transfer(queue)
}

func (srvCtx *srvContext) transfer(queue string) {
	for i := range srvCtx.listeners {
		srvCtx.listeners[i] <- queue
	}
}
