package producers

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/mikechris/kaha/clickhouse"
	"github.com/mikechris/kaha/models"
)

const clickhName = "clickhouse"

type clickhouseProducer struct {
	*clickhouse.Client
	DbTable string
}

func (c clickhouseProducer) String() string {
	return fmt.Sprintf("%s table %s", clickhName, c.DbTable)
}

func init() {
	registerProducer(clickhName, newClickhouseProducer)
}

func newClickhouseProducer(config map[string]interface{}, debug bool, logger *log.Logger) (producer io.Writer, err error) {
	var cfgClickh models.ClickhouseConfig

	buf := &bytes.Buffer{}

	if err := toml.NewEncoder(buf).Encode(config); err != nil {
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
