package producer

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/mikechris/kaha/clickhouse"
)

const clickhName = "clickhouse"

type clickhouseProducer struct {
	*clickhouse.Client
	DbTable string
}

// ClickhouseClientConfig contains configuration for clickhouse http client.
type ClickhouseClientConfig struct {
	Node          string `toml:"node"`
	DbTable       string `toml:"db_table"`
	TimeOut       int    `toml:"timeout_seconds"`
	RetryAttempts int    `toml:"retry_attempts"`
	BackoffTime   int    `toml:"backoff_time_seconds"`
	ConnLimit     int    `toml:"conn_limit"`
}

func (c clickhouseProducer) String() string {
	return fmt.Sprintf("%s table %s", clickhName, c.DbTable)
}

func init() {
	registerProducer(clickhName, newClickhouseProducer)
}

func newClickhouseProducer(cfg map[string]interface{}, debug bool, logger *log.Logger) (producer io.Writer, err error) {
	var cfgClickh ClickhouseClientConfig

	buf := &bytes.Buffer{}

	if err := toml.NewEncoder(buf).Encode(cfg); err != nil {
		return nil, err
	}

	if err := toml.Unmarshal(buf.Bytes(), &cfgClickh); err != nil {
		return nil, err
	}

	var log *log.Logger

	if debug {
		log = logger
	}

	clickh := clickhouse.NewClient(&http.Client{
		Timeout: time.Second * time.Duration(cfgClickh.TimeOut),
		Transport: &http.Transport{
			MaxIdleConns:        cfgClickh.ConnLimit,
			MaxIdleConnsPerHost: cfgClickh.ConnLimit,
		},
	},
		cfgClickh.Node,
		cfgClickh.RetryAttempts,
		time.Second*time.Duration(cfgClickh.BackoffTime),
		log,
	)
	return &clickhouseProducer{
		Client:  clickh,
		DbTable: cfgClickh.DbTable,
	}, err
}

func (c *clickhouseProducer) Write(p []byte) (int, error) {
	return len(p), c.InsertIntoJSONEachRow(c.DbTable, p)
}
