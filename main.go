package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/BurntSushi/toml"
)

// Config all
type Config struct {
	Kafka         KafkaConfig      `toml:"Kafka"`
	Clickhouse    ClickhouseConfig `toml:"Clickhouse"`
	ProcessConfig `toml:"Process"`
}

// ProcessConfig message mutate options
type ProcessConfig struct {
	RenameFields   map[string]string `toml:"RenameFields"`
	SubMatchValues map[string]string `toml:"SubMatchValues"`
	RemoveFields   []string          `toml:"remove_fields"`
}

// KafkaConfig process and consumer
type KafkaConfig struct {
	KafkaConsumerConfig
	KafkaConsumerConnectConfig
}

// KafkaConsumerConfig process
type KafkaConsumerConfig struct {
	Topics    []string `toml:"topics"`
	Consumers int      `toml:"consumers"`
	Batch     int      `toml:"batch_size"`
}

// KafkaConsumerConnectConfig consumer
type KafkaConsumerConnectConfig struct {
	Brokers            []string `toml:"brokers"`
	Group              string   `toml:"group"`
	AutoCommit         bool     `toml:"auto_commit"`
	AutoCommitInterval int      `toml:"auto_commit_interval_ms"`
	AutoOffsetReset    string   `toml:"auto_offset_reset"`
	SessionTimeout     int      `toml:"session_timeout_ms"`
}

// ClickhouseConfig client
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

	var logger = newLog("kaha", 0)

	var cfg Config

	if err := loadConfig(*cliConfig, &cfg); err != nil {
		logger.Fatal(err)
	}

	var clickhLog *log.Logger

	if *cliDebug {
		clickhLog = newLog("kaha clickhouse", 0)
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
		clickhLog,
	)

	consumers := make([]*Consumer, cfg.Kafka.Consumers)

	for i := 0; i < cfg.Kafka.Consumers; i++ {
		consumerLog := newLog(fmt.Sprintf("kaha consumer-%v", i+1), 0)

		process := ProcessBatch(clickh,
			&cfg.ProcessConfig,
			cfg.Clickhouse.DbTable,
			NewLogReducedFields(consumerLog))

		if *cliDebug {
			process = LogProcessBatch(consumerLog, process)
		}

		consumer, err := NewConsumer(
			NewConsumerConfig(&cfg.Kafka.KafkaConsumerConnectConfig),
			cfg.Kafka.Topics,
			cfg.Kafka.AutoCommit,
			cfg.Kafka.Batch,
			process,
			consumerLog,
		)
		if err != nil {
			logger.Fatal(err)
		}
		consumers[i] = consumer
	}

	RunConsumer(logger, *cliDebug, consumers...)
}

type logWriter struct {
}

func (writer logWriter) Write(bytes []byte) (int, error) {
	return fmt.Print(time.Now().UTC().Format("Jan 02 15:04:05") + " " + string(bytes))
}

func newLog(name string, flag int) *log.Logger {
	return log.New(new(logWriter), name+" ", flag)
}

func loadConfig(f string, cfg *Config) (err error) {
	b, err := ioutil.ReadFile(f)
	if err != nil {
		return fmt.Errorf("could not read file %s: %v", f, err)
	}

	if _, err = toml.Decode(string(b), cfg); err != nil {
		return fmt.Errorf("could not parse file %s: %v", f, err)
	}

	return nil
}
