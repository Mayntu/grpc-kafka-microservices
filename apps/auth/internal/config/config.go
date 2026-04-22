package config

import (
	"log"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env         string `yaml:"env" env:"ENV" env-default:"local"`
	DatabaseURL string `yaml:"database_url" env:"DATABASE_URL" env-required:"true"`
	RPCServer   `yaml:"rpc_server"`
	HTTPServer  `yaml:"http_server"`
	JWT         `yaml:"jwt"`
	KafkaServer `yaml:"kafka_server"`
}

type HTTPServer struct {
	Address         string        `yaml:"address" env:"HTTP_ADDRESS" env-default:"localhost:8080"`
	Port            string        `yaml:"port" env:"HTTP_PORT" env-default:"8080"`
	Timeout         time.Duration `yaml:"timeout" env:"HTTP_TIMEOUT" env-default:"4s"`
	IdleTimeout     time.Duration `yaml:"idle_timeout" env:"HTTP_IDLE_TIMEOUT" env-default:"60s"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" env:"HTTP_SHUTDOWN_TIMEOUT" env-default:"60s"`
}

type RPCServer struct {
	Port string `yaml:"port" env:"RPC_SERVER_PORT" env-default:"50051"`
}

type KafkaServer struct {
	Brokers          []string `yaml:"brokers" env:"KAFKA_SERVER_BROKERS" env-separator:","`
	UserCreatedTopic string   `yaml:"user_created_topic" env:"KAFKA_SERVER_USER_CREATED_TOPIC" env-default:"user_created"`
}

type JWT struct {
	SecretKey string        `yaml:"secret_key" env:"JWT_SECRET_KEY" env-required:"true"`
	TTL       time.Duration `yaml:"ttl" env:"JWT_TTL" env-default:"10m"`
}

func MustLoad() *Config {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = ".env"
	}

	var cfg Config

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("could not read config: %s", err)
	}
	return &cfg
}
