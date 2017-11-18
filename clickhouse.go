package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Clickhouse struct {
	Client Client
}

func NewClickhouse(client *http.Client, host string, attempts int, backoff time.Duration, log *log.Logger) *Clickhouse {
	c := Decorate(client,
		Logging(log),
		ApiAddr(host),
		FaultTolerance(attempts, backoff))
	return &Clickhouse{
		Client: c,
	}
}

func (c *Clickhouse) GetColumns(dbTableName string) (columns []string, err error) {
	type describeTable struct {
		Data []map[string]string `json:"data"`
	}

	var desc describeTable

	err = c.Do(http.MethodGet, fmt.Sprintf("/?query=DESCRIBE%%20%s%%20FORMAT%%20JSON", dbTableName), nil, "applicaiton/json", &desc)
	if err != nil {
		return nil, fmt.Errorf("could not get describe of %s: %v", dbTableName, err)
	}

	for _, column := range desc.Data {
		for key, value := range column {
			if key == "name" {
				columns = append(columns, value)
			}
		}
	}
	return columns, nil
}

func (c *Clickhouse) InsertIntoJSONEachRow(dbTableName string, rows [][]byte) error {
	b := make([]byte, 0)

	for _, row := range rows {
		b = append(b, row...)
		b = append(b, []byte("\n")...)
	}

	if err := c.Do(http.MethodPost,
		fmt.Sprintf("/?query=INSERT%%20INTO%%20%s%%20FORMAT%%20JSONEachRow", dbTableName),
		bytes.NewBuffer(b),
		"text/plain",
		nil); err != nil {
		return fmt.Errorf("could not post rows to %s: %v", dbTableName, err)
	}
	return nil
}

// Do Make request and if resp contains data unmarshall it to v
func (c *Clickhouse) Do(httpMethod string, uri string, payload io.Reader, contentType string, v interface{}) (err error) {
	req, err := http.NewRequest(httpMethod, uri, payload)
	req.Header.Set("Content-Type", contentType)

	if err != nil {
		return err
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad response from server: %s message: %s", resp.Status, body)
	}

	if v == nil {
		return nil
	}

	return json.Unmarshal(body, v)
}

// A Client sends http.Requests and returns http.Responses or errors in
// case of failure.
type Client interface {
	Do(*http.Request) (*http.Response, error)
}

// ClientFunc is a function type that implements the Client interface.
type ClientFunc func(*http.Request) (*http.Response, error)

// Do implements Client interface
func (f ClientFunc) Do(r *http.Request) (*http.Response, error) {
	return f(r)
}

// A Decorator wraps a Client with extra behaviour.
type Decorator func(Client) Client

// Logging returns a Decorator that logs a Client's requests.
func Logging(l *log.Logger) Decorator {
	return func(c Client) Client {
		return ClientFunc(func(r *http.Request) (resp *http.Response, err error) {
			id := rand.Int63()
			l.Printf("[%d] %s %s %d", id, r.Method, r.URL, r.ContentLength)
			resp, err = c.Do(r)
			if err != nil {
				l.Printf("[%d] %v", id, err)
				return resp, err
			}
			l.Printf("[%d] %s %s %s", id, r.Method, r.URL, resp.Status)
			return resp, err
		})
	}
}

// FaultTolerance returns a Decorator that extends a Client with fault tolerance
// configured with the given attempts and backoff duration.
func FaultTolerance(attempts int, backoff time.Duration) Decorator {
	return func(c Client) Client {
		return ClientFunc(func(r *http.Request) (res *http.Response, err error) {
			for i := 0; i <= attempts; i++ {
				if res, err = c.Do(r); err == nil {
					break
				}
				slow := 1
				// Code: 252 DB::Exception: Too many parts (300). Merges are processing significantly slower than inserts
				// This error needs longer backoff time
				if strings.HasPrefix(err.Error(), "Code: 252") {
					slow = 10
				}
				time.Sleep(backoff * time.Duration(i) * time.Duration(slow))
			}
			return res, err
		})
	}
}

// ApiAddr Add API address to request
func ApiAddr(apiAddr string) Decorator {
	return func(c Client) Client {
		return ClientFunc(func(r *http.Request) (*http.Response, error) {
			if strings.HasPrefix(r.URL.String(), apiAddr) {
				return c.Do(r)
			}
			url, err := url.Parse(apiAddr + r.URL.String())
			if err != nil {
				return nil, err
			}
			r.URL = url
			return c.Do(r)
		})
	}
}

// Decorate decorates a Client c with all the given Decorators, in order.
func Decorate(c Client, ds ...Decorator) Client {
	decorated := c
	for _, decorate := range ds {
		decorated = decorate(decorated)
	}
	return decorated
}
