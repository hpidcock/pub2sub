package main

type Config struct {
	JWTKey       string
	Port         int
	RedisAddress string
}

func NewConfig() (Config, error) {
	// TODO: Handle loading config
	return Config{
		JWTKey:       "test",
		Port:         5001,
		RedisAddress: "localhost:6379",
	}, nil
}
