package config

import "github.com/ardanlabs/conf/v3"

// Config - app config
type Config struct {
	conf.Version
	RabbitConnectionString string `conf:"env:rabbit_cs, default:amqp://guest:guest@localhost:5672"`
	SourceQueue            string `conf:"default:PartyMq.Q.Default,help:source queue, app will consume messages from this queue"`
	KeyConfig              struct {
		Source string `conf:"default:header, help:points to a source for fetching partition key, possible values are: header, body"`
		Key    string `conf:"default:partitionKey, help:key for partitionKey value"`
	}
}
