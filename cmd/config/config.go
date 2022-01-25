package config

type Config struct {
	RabbitMqConnectionString string  `envconfig:"partymq_rabbit_cs" required:"true"`
	PartitionKey string `envconfig:"partymq_partition_key" default:"id"`
	HealthCheckInterval string
	NoConsumerTimeout string
}
