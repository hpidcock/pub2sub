package main

import (
	"github.com/koding/multiconfig"
)

type Config struct {
	JWTKey           string
	Port             int    `default:"5002"`
	RedisAddress     string `default:"localhost:6379"`
	EtcdAddress      string `default:"localhost:2379"`
	Layer            int    `default:"0"`
	TerminationLayer bool   `default:"true"`
}

func NewConfig() (Config, error) {
	config := Config{}
	m := multiconfig.New()
	m.MustLoad(&config)

	return config, nil
}
