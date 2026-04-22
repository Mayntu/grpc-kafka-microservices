package config

import (
	"log"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env         string `yaml:"env" env:"ENV" env-default:"local"`
	KafkaServer `yaml:"kafka_server"`
}

type KafkaServer struct {
	Brokers        []string      `yaml:"brokers" env:"KAFKA_SERVER_BROKERS" env-separator:","`
	GroupID        string        `yaml:"group_id" env:"KAFKA_SERVER_GROUP_ID" env-default:"notify-service"`
	CommitInterval time.Duration `yaml:"commit_interval" env:"KAFKA_SERVER_COMMIT_INTERVAL" env-default:"0"`
	DLQMaxRetries  int           `yaml:"dlq_max_retries" env:"KAFKA_SERVER_DLQ_MAX_RETRIES" env-default:"3"`
}

func MustLoad() *Config {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = ".env"
	}
	var cfg Config
	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("could not read env config: %s", err)
	}
	return &cfg
}
