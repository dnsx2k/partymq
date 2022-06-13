package service

type Client struct {
	queueName  string
	routingKey string
	keys       map[string]struct{}
}
