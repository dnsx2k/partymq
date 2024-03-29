package config

import "github.com/ardanlabs/conf/v3"

// Config - app config
type Config struct {
	conf.Version
	RabbitConnectionString string `conf:"env:RABBIT_CS,default:amqp://guest:guest@localhost:5672"`
	SourceQueue            string `conf:"env:source_queue,default:partymq.q.source,help:source queue, app will consume messages from this queue"`
	KeyConfig              struct {
		Source string `conf:"default:header,help:points to a source for fetching partition key, possible values are: header, body"`
		Key    string `conf:"default:partitionKey,help:key for partitionKey value"`
	}
	HeartBeatConfig struct {
		CheckInterval string `conf:"default:30s,help:duration, after this span background job will inspect whether clients are idle"`
		ExpiresAfter  string `conf:"default:120s,help:duration, after this span client will be deleted if no heartbeat sent"`
	}
}
