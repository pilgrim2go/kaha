package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"
)

type Config struct {
	Kafka         KafkaConfig      `toml:"Kafka"`
	Clickhouse    ClickhouseConfig `toml:"Clickhouse"`
	ProcessConfig `toml:"Process"`
}

type ProcessConfig struct {
	RenameFields   map[string]string `toml:"RenameFields`
	SubMatchValues map[string]string `toml:"SubMatchValues`
	RemoveFields   []string          `toml:"remove_fields"`
}

type KafkaConfig struct {
	KafkaProcessConfig
	KafkaConsumerConfig
}

type KafkaProcessConfig struct {
	Topics    []string `toml:"topics"`
	Consumers int      `toml:"consumers"`
	Batch     int      `toml:"batch_size"`
}

type KafkaConsumerConfig struct {
	Brokers            []string `toml:"brokers"`
	Group              string   `toml:"group"`
	AutoCommit         bool     `toml:"auto_commit"`
	AutoCommitInterval int      `toml:"auto_commit_interval_ms"`
	AutoOffsetReset    string   `toml:"auto_offset_reset"`
	SessionTimeout     int      `toml:"session_timeout_ms"`
}

type ClickhouseConfig struct {
	Node          string `toml:"node"`
	DbTable       string `toml:"db_table"`
	TimeOut       int    `toml:"timeout_seconds"`
	RetryAttempts int    `toml:"retry_attempts"`
	BackoffTime   int    `toml:"backoff_time_seconds"`
}

func main() {
	cliConfig := flag.String("config", "kaha.toml", "Kaha feeder config file path")
	cliDebug := flag.Bool("debug", false, "Debug mode")
	flag.Parse()

	var logger = NewLog("kaha", 0)

	cfg, err := loadConfig(*cliConfig)
	if err != nil {
		logger.Fatal(err)
	}

	var clickLogger *log.Logger

	if *cliDebug {
		clickLogger = NewLog("kaha clickhouse", 0)
	}

	clickh := NewClickhouse(&http.Client{
		Timeout: time.Second * time.Duration(cfg.Clickhouse.TimeOut),
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Kafka.Consumers,
			MaxIdleConnsPerHost: cfg.Kafka.Consumers,
		},
	},
		cfg.Clickhouse.Node,
		cfg.Clickhouse.RetryAttempts,
		time.Second*time.Duration(cfg.Clickhouse.BackoffTime),
		clickLogger,
	)

	consumers := make([]*Consumer, cfg.Kafka.Consumers)

	for i := 0; i < cfg.Kafka.Consumers; i++ {
		if err != nil {
			logger.Fatal(err)
		}
		consumeLog := NewLog(fmt.Sprintf("kaha consumer-%v", i+1), 0)

		process := ProcessBatch(clickh,
			&cfg.ProcessConfig,
			cfg.Clickhouse.DbTable,
			NewLogReducedFields(logger))

		if *cliDebug {
			process = LogProcessBatch(consumeLog, process)
		}

		consumer, err := NewConsumer(
			NewConsumerConfig(&cfg.Kafka.KafkaConsumerConfig),
			cfg.Kafka.Topics,
			cfg.Kafka.AutoCommit,
			cfg.Kafka.Batch,
			process,
			consumeLog,
		)
		if err != nil {
			logger.Fatal(err)
		}
		consumers[i] = consumer
	}

	RunConsumer(logger, *cliDebug, consumers...)
}
