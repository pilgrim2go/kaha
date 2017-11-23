package producer

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mikechris/kaha/clickhouse"
	"github.com/mikechris/kaha/models"
)

type Clickhouse struct {
	*clickhouse.Client
	DbTable string
}

func init() {
	registerProducer("clickhouse", newClickhouse)
}

func newClickhouse(config map[string]interface{}) (producer io.Writer, err error) {
	var node, dbTable string
	var connLimit, timeout, retry, backoff int

	for k, v := range config {
		switch k {
		case "node":
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s value %v is not string", k, v)
			}
			node = s
		case "db_table":
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s value %v is not string", k, v)
			}
			dbTable = s
		case "conn_limit":
			i, ok := v.(int64)
			if !ok {
				return nil, fmt.Errorf("%s value %v is not int", k, v)
			}
			connLimit = int(i)
		case "timeout_seconds":
			i, ok := v.(int64)
			if !ok {
				return nil, fmt.Errorf("%s value %v is not int", k, v)
			}
			timeout = int(i)
		case "retry_attempts":
			i, ok := v.(int64)
			if !ok {
				return nil, fmt.Errorf("%s value %v is not int", k, v)
			}
			retry = int(i)
		case "backoff_time_seconds":
			i, ok := v.(int64)
			if !ok {
				return nil, fmt.Errorf("%s value %v is not int", k, v)
			}
			backoff = int(i)
		}
	}
	clickh := clickhouse.NewClient(&http.Client{
		Timeout: time.Second * time.Duration(timeout),
		Transport: &http.Transport{
			MaxIdleConns:        connLimit,
			MaxIdleConnsPerHost: connLimit,
		},
	},
		node,
		retry,
		time.Second*time.Duration(backoff),
		models.NewLog("clickhouse", 0),
	)
	return &Clickhouse{
		Client:  clickh,
		DbTable: dbTable,
	}, err
}

func (c *Clickhouse) Write(p []byte) (int, error) {
	return len(p), c.InsertIntoJSONEachRow(c.DbTable, p)
}
