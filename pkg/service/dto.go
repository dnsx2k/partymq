package service

// Client - represents client connected to partymq service
type Client struct {
	queueName  string
	routingKey string
	keys       map[string]struct{}
}
