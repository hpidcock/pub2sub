package main

import (
	"time"

	"github.com/koding/multiconfig"
)

type Config struct {
	Port                   int           `default:"5005"`
	RedisAddress           string        `default:"localhost:6379"`
	EtcdAddress            string        `default:"localhost:2379"`
	MaxSubscribeDuration   time.Duration `default:"5m"`
	QueueKeepAliveDuration time.Duration `default:"5m"`
}

func NewConfig() (Config, error) {
	config := Config{}
	m := multiconfig.New()
	m.MustLoad(&config)

	return config, nil
}
