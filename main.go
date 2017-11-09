package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	Kafka         KafkaConfig      `toml:"Kafka"`
	Clickhouse    ClickhouseConfig `toml:"Clickhouse"`
	ProcessConfig `toml:"Process"`
}

type ProcessConfig struct {
	FlatFields   map[string]string `toml:"FlatFields`
	RemoveFields []string          `toml:"remove_fields"`
}

type KafkaConfig struct {
	Broker             string   `toml:"broker"`
	Group              string   `toml:"group"`
	Topics             []string `toml:"topics"`
	Consumers          int      `toml:"consumers"`
	AutoCommit         bool     `toml:"auto_commit"`
	AutoCommitInterval int      `toml:"auto_commit_interval_ms"`
	AutoOffsetReset    string   `toml:"auto_offset_reset"`
	SessionTimeout     int      `toml:"session_timeout_ms"`
	Batch              int      `toml:"batch_size"`
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
	flag.Parse()

	var logger = NewLog("kaha", 0)

	cfg, err := loadConfig(*cliConfig)
	if err != nil {
		logger.Fatal(err)
	}

	clickh := NewClickhouse(&http.Client{Timeout: time.Second * time.Duration(cfg.Clickhouse.TimeOut)},
		cfg.Clickhouse.Node,
		cfg.Clickhouse.RetryAttempts,
		time.Second*time.Duration(cfg.Clickhouse.BackoffTime),
		NewLog("kaha clickhouse", 0))

	tableColumns, err := clickh.GetColumns(cfg.Clickhouse.DbTable)

	if err != nil {
		logger.Fatal(err)
	}

	quitPool := make(chan chan bool, cfg.Kafka.Consumers)
	var wg sync.WaitGroup

	for i := 0; i < cfg.Kafka.Consumers; i++ {
		q := make(chan bool)
		quitPool <- q
		consumer, err := NewConsumer(
			&cfg.Kafka,
			processBatch(clickh,
				&cfg.ProcessConfig,
				cfg.Clickhouse.DbTable,
				tableColumns,
				newLogReducedFields(logger)),
			&wg,
			q,
			NewLog(fmt.Sprintf("kaha consumer-%v", i+1), 0),
		)
		if err != nil {
			logger.Fatal(err)
		}
		wg.Add(1)
		go consumer.Feed()
		logger.Printf("created consumer: %v\n", consumer.Consumer)
	}

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			select {
			case sig := <-osSignals:
				logger.Printf("Caught signal %v: terminating\n", sig)
				for len(quitPool) != 0 {
					q := <-quitPool
					q <- true
				}
				return
			}
		}
	}()

	wg.Wait()
	logger.Println("all consumers closed")
}
