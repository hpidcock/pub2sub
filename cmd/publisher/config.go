package main

import (
	"github.com/koding/multiconfig"
)

type Config struct {
	JWTKey       string
	Port         int    `default:"5001"`
	RedisAddress string `default:"localhost:6379"`
	EtcdAddress  string `default:"localhost:2379"`
}

func NewConfig() (Config, error) {
	config := Config{}
	m := multiconfig.New()
	m.MustLoad(&config)

	return config, nil
}
