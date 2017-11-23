package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/mikechris/kaha/clickhouse"
	"github.com/mikechris/kaha/consumer"
	"github.com/mikechris/kaha/models"
	"github.com/mikechris/kaha/producer"
)

func main() {
	cliConfig := flag.String("config", "kaha.toml", "Kaha feeder config file path")
	cliDebug := flag.Bool("debug", false, "Debug mode")
	flag.Parse()

	var logger = models.NewLog("", 0)

	var cfg models.Config

	if err := loadConfig(*cliConfig, &cfg); err != nil {
		logger.Fatal(err)
	}

	var clickhLog *log.Logger

	if *cliDebug {
		clickhLog = models.NewLog("clickhouse", 0)
	}

	for _, cfg := range cfg.Consumer {
		var clickhConfig models.ClickhouseConfig

		buf := &bytes.Buffer{}
		if err := toml.NewEncoder(buf).Encode(cfg.ProducerConfig.Config); err != nil {
			logger.Fatal(err)
		}

		if err := toml.Unmarshal(buf.Bytes(), &clickhConfig); err != nil {
			logger.Fatal(err)
		}

		clickh := clickhouse.NewClient(&http.Client{
			Timeout: time.Second * time.Duration(clickhConfig.TimeOut),
		},
			clickhConfig.Node,
			clickhConfig.RetryAttempts,
			time.Second*time.Duration(clickhConfig.BackoffTime),
			clickhLog,
		)

		// get clickhouse table structure
		columns, err := clickh.GetColumns(clickhConfig.DbTable)
		if err != nil {
			logger.Fatal(err)
		}

		cfg.ProcessConfig.OnlyFields = columns

		consumers, err := consumer.CreateConsumer(cfg.Name, cfg.Consumers, cfg.Config, cfg.ProcessConfig)
		if err != nil {
			logger.Fatal(err)
		}

		p, err := producer.CreateProducer(cfg.ProducerConfig.Name, cfg.ProducerConfig.Config)
		if err != nil {
			log.Fatal(err)
		}
		consumer.RunConsumer(logger, p, consumers...)
	}

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

// LogProcessBatch log time spend on messages processing
func LogProcessBatch(l *log.Logger, process consumer.ProcessMessagesFunc) consumer.ProcessMessagesFunc {
	return func(producer io.Writer, messages []*kafka.Message) error {
		start := time.Now()
		l.Printf("start processing of %d messages", len(messages))
		err := process(producer, messages)
		if err != nil {
			return err
		}
		l.Printf("finished processing of %d messages time: %v", len(messages), time.Since(start))
		return nil
	}
}
