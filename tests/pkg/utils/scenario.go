package utils

import amqp "github.com/rabbitmq/amqp091-go"

type Scenario struct {
	MessageVolume     int
	IdPoolSize        int
	Partitions        []string
	ExecuteScenarioFn func(channel *amqp.Channel)
}
