package main

type Config struct {
	JWTKey       string
	GRPCPort     int
	RedisAddress string
}

func NewConfig() (Config, error) {
	// TODO: Handle loading config
	return Config{
		JWTKey:       "test",
		GRPCPort:     52998,
		RedisAddress: "localhost:6379",
	}, nil
}
