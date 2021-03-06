package rabbit

const (
	PartyMqBufferQueue string = "partymq.q.buffer"
	PartyMqExchange    string = "partymq.ex.write"
)

// Direction - type for amqp connection - PUB/SUB/PRIMARY
type Direction string

const (
	DirectionPrimary Direction = "PRIMARY"
	DirectionPub     Direction = "PUB"
	DirectionSub     Direction = "SUB"
)
