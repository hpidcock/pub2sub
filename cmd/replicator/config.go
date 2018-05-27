package main

type Config struct {
	JWTKey           string
	Port             int
	RedisAddress     string
	Layer            int
	TerminationLayer bool
}

func NewConfig() (Config, error) {
	// TODO: Handle loading config
	return Config{
		JWTKey:           "test",
		Port:             5002,
		RedisAddress:     "localhost:6379",
		TerminationLayer: true,
	}, nil
}
