package config

import (
	"fmt"

	"github.com/spf13/viper"
)

// config.go is used to load and parse environment variables (like DB passwords).
// It keeps hardcoded values out of the main codebase.

// Config holds all the configuration for our application

type Config struct {
	Server    ServerConfig
	Database  DatabaseConfig
	Kafka     KafkaConfig
	Providers ProvidersConfig
}

type ServerConfig struct {
	Port string
}

type DatabaseConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
}

type KafkaConfig struct {
	Brokers []string
	Topic   string
}

type ProvidersConfig struct {
	SendGrid SendGridConfig
	Twilio   TwilioConfig
}

type SendGridConfig struct {
	ApiKey    string `mapstructure:"api_key"`
	FromEmail string `mapstructure:"from_email"`
}

type TwilioConfig struct {
	AccountSID string `mapstructure:"account_sid"`
	AuthToken  string `mapstructure:"auth_token"`
	FromNumber string `mapstructure:"from_number"`
}

// Load the config from yaml file.

func LoadConfig() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	return &cfg, nil
}
