package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/mikechris/kaha/clickhouse"
	"github.com/mikechris/kaha/config"
	"github.com/mikechris/kaha/consumer"
	"github.com/mikechris/kaha/producer"
)

var version string

func main() {
	cliConfig := flag.String("config", "kaha.toml", "Kaha feeder config file path")
	cliDebug := flag.Bool("debug", false, "Debug mode")
	cliVersion := flag.Bool("version", false, "Print program version")

	flag.Parse()

	if *cliVersion == true {
		fmt.Printf("Compiled: %s\n", version)
		os.Exit(0)
	}

	var logger = newLog(os.Stderr, "", 0)

	cfg, err := config.FromFile(*cliConfig)
	if err != nil {
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
		consumers := make([]consumer.Consumer, cfg.Consumers)
		for i := 0; i < cfg.Consumers; i++ {
			if cfg.Producer.Name == "clickhouse" {
				onlyFields, err := getOnlyFieldsFromClickhouse(cfg.Producer.Custom, *cliDebug)
				if err != nil {
					cancel()
					break loop
				}
				cfg.Process.OnlyFields = onlyFields
			}

			consumer, err := consumer.CreateConsumer(cfg.Name, cfg.Custom, cfg.Process, *cliDebug, newLog(os.Stderr, cfg.Name, 0))

			if err != nil {
				logger.Println(err)
				cancel()
				break loop
			}
			consumers[i] = consumer
		}

		p, err := producer.CreateProducer(cfg.Producer.Name,
			cfg.Producer.Custom,
			*cliDebug,
			newLog(os.Stderr, cfg.Producer.Name, 0))
		if err != nil {
			logger.Println(err)
			cancel()
			break loop

		}

		for _, c := range consumers {
			wg.Add(1)
			go func(c consumer.Consumer) {
				if err := c.Consume(ctx, p); err != nil {
					logger.Print(err)
				}
				wg.Done()
			}(c)
		}
		logger.Printf("started consumers %v for producer %v\n", consumers, p)
	}
	wg.Wait()
	logger.Println("all consumers closed")
}

// getOnlyFieldsFromClickhouse get only fields for process based on clickhouse table columns
func getOnlyFieldsFromClickhouse(cfg map[string]interface{}, debug bool) ([]string, error) {
	var clickhLog *log.Logger
	if debug {
		clickhLog = newLog(os.Stderr, "clickhouse", 0)
	}

	var clickhConfig producer.ClickhouseClientConfig

	buf := &bytes.Buffer{}
	if err := toml.NewEncoder(buf).Encode(cfg); err != nil {
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

type logWriter struct {
	out io.Writer
}

func (w logWriter) Write(bytes []byte) (int, error) {
	return fmt.Fprint(w.out, time.Now().UTC().Format("Jan 02 15:04:05")+" "+string(bytes))
}

func newLog(out io.Writer, name string, flag int) *log.Logger {
	if name != "" {
		name = " " + name
	}
	prefix := strings.Split(os.Args[0], "/")
	name = prefix[len(prefix)-1] + name
	return log.New(&logWriter{out}, name+" ", flag)
}
