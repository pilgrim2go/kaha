package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/mikechris/kaha/clickhouse"
	"github.com/mikechris/kaha/consumers"
	"github.com/mikechris/kaha/models"
	"github.com/mikechris/kaha/producers"
)

func main() {
	cliConfig := flag.String("config", "kaha.toml", "Kaha feeder config file path")
	cliDebug := flag.Bool("debug", false, "Debug mode")

	flag.Parse()

	var logger = models.NewLog(os.Stderr, "", 0)
	var cfg models.Config

	if err := loadConfig(*cliConfig, &cfg); err != nil {
		logger.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-osSignals
		logger.Printf("terminating after received signal: %v\n", sig)
		cancel()
	}()

	var wg sync.WaitGroup

loop:
	for _, cfg := range cfg.Consumers {
		consumers := make([]consumers.Consumer, cfg.Consumers)
		for i := 0; i < cfg.Consumers; i++ {
			consumer, err := createConsumer(cfg, *cliDebug, models.NewLog(os.Stderr, cfg.Name, 0))

			if err != nil {
				logger.Println(err)
				cancel()
				break loop
			}
			consumers[i] = consumer
		}

		p, err := producers.CreateProducer(cfg.ProducerConfig.Name,
			cfg.ProducerConfig.Config,
			*cliDebug,
			models.NewLog(os.Stderr, cfg.ProducerConfig.Name, 0))
		if err != nil {
			logger.Println(err)
			cancel()
			break loop

		}

		for _, c := range consumers {
			wg.Add(1)
			go c.Consume(ctx, p, &wg)
		}
		logger.Printf("started consumers %v for producer %v\n", consumers, p)
	}

	wg.Wait()
	logger.Println("all consumers closed")
}

func loadConfig(f string, cfg *models.Config) (err error) {
	b, err := ioutil.ReadFile(f)
	if err != nil {
		return fmt.Errorf("could not read file %s: %v", f, err)
	}

	if _, err = toml.Decode(string(b), cfg); err != nil {
		return fmt.Errorf("could not parse file %s: %v", f, err)
	}

	return nil
}

func createConsumer(cfg *models.ConsumerConfig, debug bool, logger *log.Logger) (consumers.Consumer, error) {
	if cfg.ProducerConfig.Name == "clickhouse" {
		onlyFields, err := getOnlyFieldsFromClickhouse(cfg.ProducerConfig.Config, debug)
		if err != nil {
			return nil, err
		}
		cfg.ProcessConfig.OnlyFields = onlyFields
	}

	return consumers.CreateConsumer(cfg.Name, cfg.Config, cfg.ProcessConfig, debug, logger)
}

func getOnlyFieldsFromClickhouse(config map[string]interface{}, debug bool) ([]string, error) {
	var clickhLog *log.Logger

	if debug {
		clickhLog = models.NewLog(os.Stderr, "clickhouse", 0)
	}

	var clickhConfig models.ClickhouseConfig

	buf := &bytes.Buffer{}
	if err := toml.NewEncoder(buf).Encode(config); err != nil {
		return nil, err
	}

	if err := toml.Unmarshal(buf.Bytes(), &clickhConfig); err != nil {
		return nil, err
	}

	clickh := clickhouse.NewClient(&http.Client{
		Timeout: time.Second * time.Duration(clickhConfig.TimeOut),
	},
		clickhConfig.Node,
		clickhConfig.RetryAttempts,
		time.Second*time.Duration(clickhConfig.BackoffTime),
		clickhLog,
	)

	return clickh.GetColumns(clickhConfig.DbTable)
}
