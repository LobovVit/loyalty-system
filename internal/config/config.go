package config

import (
	"flag"
	"fmt"
	"log"
	"strconv"

	"github.com/caarlos0/env/v6"
)

type Config struct {
	Host         string `env:"RUN_ADDRESS"`
	LogLevel     string `env:"LOG_LEVEL"`
	DSN          string `env:"DATABASE_URI"`
	AccrualHost  string `env:"ACCRUAL_SYSTEM_ADDRESS"`
	Salt         string `env:"SALT"`
	JWTKey       string `env:"JWT_KEY"`
	JWTExp       int64  `env:"JWT_EXP"`
	BatchLimit   int    `env:"BATCH_LIMIT"`
	SendLimit    int    `env:"SEND_LIMIT"`
	PollInterval int    `env:"POOL_INTERVAL"`
}

func GetConfig() (*Config, error) {
	config := &Config{}
	err := env.Parse(config)
	if err != nil {
		return nil, fmt.Errorf("env parse: %w", err)
	}

	host := flag.String("a", "localhost:8081", "адрес и порт запуска сервиса") //localhost:8081
	logLevel := flag.String("l", "info", "log level")
	dsn := flag.String("d", "postgresql://postgres:password@10.66.66.3:5432/postgres?sslmode=disable", "строка подключения к БД") //postgresql://postgres:password@10.66.66.3:5432/postgres?sslmode=disable
	accrualHost := flag.String("r", "http://localhost:8080", "адрес системы расчёта начислений")                                  //http://localhost:8080
	salt := flag.String("s", "any-salt", "соль для хэша")
	jwtkey := flag.String("k", "very-secret-key", "ключ для JWT")
	jwtexp := flag.Int64("e", 3, "время жизни токена авторизации в часах")
	batchLimit := flag.Int("bl", 100, "количество заказов для обработки за один раз")
	sendLimit := flag.Int("sl", 30, "максимальное количество запросов к серверу")
	pollInterval := flag.Int("pi", 10, "интервалы времени между обработкой пачек заказов")
	flag.Parse()

	if config.Host == "" {
		config.Host = *host
	}
	if config.LogLevel == "" {
		config.LogLevel = *logLevel
	}
	if config.DSN == "" {
		config.DSN = *dsn
	}
	if config.DSN == "" {
		return nil, fmt.Errorf("строка подключения к БД - не определена")
	}
	if config.AccrualHost == "" {
		config.AccrualHost = *accrualHost
	}
	if config.Salt == "" {
		config.Salt = *salt
	}
	if config.JWTKey == "" {
		config.JWTKey = *jwtkey
	}
	if config.JWTExp == 0 {
		config.JWTExp = *jwtexp
	}
	if config.BatchLimit == 0 {
		config.BatchLimit = *batchLimit
	}
	if config.SendLimit == 0 {
		config.SendLimit = *sendLimit
	}
	if config.PollInterval == 0 {
		config.PollInterval = *pollInterval
	}
	log.Println("---config---")
	log.Println("config.Host=" + config.Host)
	log.Println("config.LogLevel=" + config.LogLevel)
	log.Println("config.DSN=" + config.DSN)
	log.Println("config.AccrualHost=" + config.AccrualHost)
	log.Println("config.Salt=" + config.Salt)
	log.Println("config.JWTKey=" + config.JWTKey)
	log.Println("config.JWTExp=" + strconv.FormatInt(config.JWTExp, 10))
	log.Println("config.BatchLimit=" + strconv.Itoa(config.BatchLimit))
	log.Println("config.SendLimit=" + strconv.Itoa(config.SendLimit))
	log.Println("config.PollInterval=" + strconv.Itoa(config.PollInterval))
	log.Println("---config---")
	return config, nil
}
