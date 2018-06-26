package main

import (
	"github.com/koding/multiconfig"
)

type Config struct {
	Port           int    `default:"5001"`
	RedisAddress   string `default:"localhost:6379"`
	RedisCluster   bool   `default:"false"`
	EtcdAddress    string `default:"localhost:2379"`
	AnnouceAddress string `default:"localhost"`

	ClusterName string `required:"true"`
}

func NewConfig() (Config, error) {
	config := Config{}
	m := multiconfig.New()
	m.MustLoad(&config)

	return config, nil
}
