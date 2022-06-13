package config

// Config - app config
type Config struct {
	RabbitMqConnectionString string `envconfig:"partymq_rabbit_cs" required:"true"`
	PartitionKey             string `envconfig:"partymq_partition_key" default:"id"`
	HealthCheckInterval      string `envconfig:"partymq_health_check_interval" default:"30s"`
	NoConsumerTimeout        string `envconfig:"partymq_no_consumer_timeout" default:"10s"`
}
