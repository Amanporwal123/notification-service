package repository

import (
	"fmt"

	"github.com/Amanporwal123/notification-service/internal/config"
	"github.com/Amanporwal123/notification-service/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// DB is a global variable that will hold our database connection.
var DB *gorm.DB

// ConnectToDB initializes the database connection.
func ConnectToDB(cfg config.DatabaseConfig) error {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName)
	var err error
	
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect to db: %w", err)
	}

	logger.Log.Info("Successfully connected to PostgreSQL database!",
		zap.String("host", cfg.Host),
		zap.String("port", cfg.Port),
	)

	return nil
}