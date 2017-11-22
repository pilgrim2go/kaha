package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// ProcessMessagesFunc type used for message processing
type ProcessMessagesFunc func([]*kafka.Message) error

// Consumer kafka client
type Consumer struct {
	*kafka.Consumer
	process ProcessMessagesFunc

	autoCommit bool
	batchSize  int

	logger *log.Logger
}

// NewConsumer create new kafka client
func NewConsumer(cfg *kafka.ConfigMap, topics []string, autoCommit bool, batchSize int, process ProcessMessagesFunc, logger *log.Logger) (*Consumer, error) {
	c, err := kafka.NewConsumer(cfg)

	if err != nil {
		return nil, fmt.Errorf("could not create consumer: %s", err)
	}

	err = c.SubscribeTopics(topics, nil)

	if err != nil {
		return nil, fmt.Errorf("could not to subscribe to topics: %s %s", topics, err)
	}

	return &Consumer{
		Consumer:   c,
		process:    process,
		autoCommit: autoCommit,
		batchSize:  batchSize,
		logger:     logger,
	}, nil
}

// NewConsumerConfig create new consumer config
func NewConsumerConfig(cfg *KafkaConsumerConnectConfig) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers":               strings.Join(cfg.Brokers, ","),
		"group.id":                        cfg.Group,
		"session.timeout.ms":              cfg.SessionTimeout,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"enable.auto.commit":              cfg.AutoCommit,
		"auto.commit.interval.ms":         cfg.AutoCommitInterval,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": cfg.AutoOffsetReset}}
}

// Consume read and process messages from kafka
func (c *Consumer) Consume(wg *sync.WaitGroup, quit chan bool, logEOF bool) {
	shutDown := func(err error) {
		if err != nil {
			c.logger.Println(err)
		}

		c.logger.Printf("closing consumer: %v\n", c.Consumer)

		errs := make(chan error)
		go func() {
			errs <- c.Close()
		}()

		select {
		case err := <-errs:
			if err != nil {
				c.logger.Printf("could not close consumer: %v", err)
			}
			c.logger.Printf("closed consumer: %v\n", c.Consumer)
		case <-time.After(time.Second * 5):
			c.logger.Printf("timeout while closing consumer: %v\n", c.Consumer)
		}
		wg.Done()
	}

	messages := make([]*kafka.Message, 0, c.batchSize)

	for {
		select {
		case <-quit:
			c.logger.Printf("quit signal received for %v", c.Consumer)
			shutDown(nil)
			return
		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				c.logger.Printf("%v\n", e)
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				c.logger.Printf("%v\n", e)
				c.Unassign()
			case *kafka.Message:
				messages = append(messages, e)
				if len(messages) != c.batchSize {
					continue
				}

				if err := c.process(messages); err != nil {
					shutDown(fmt.Errorf("could not process batch: %v", err))
					return
				}

				if !c.autoCommit {
					if _, err := c.CommitOffsets(getOffsets(messages)); err != nil {
						shutDown(fmt.Errorf("could not commit offsets: %v", err))
						return
					}
				}

				messages = messages[:0] // trim back to zero size
			case kafka.PartitionEOF:
				if logEOF {
					c.logger.Printf("reached %v\n", e)
				}
			case kafka.Error:
				shutDown(e)
				return
			}
		}
	}
}

// RunConsumer start and manage one or more consumers
func RunConsumer(logger *log.Logger, logEOF bool, consumer ...*Consumer) {
	quitPool := make(chan chan bool, len(consumer))
	var wg sync.WaitGroup

	for _, c := range consumer {
		q := make(chan bool)
		quitPool <- q

		wg.Add(1)
		go c.Consume(&wg, q, logEOF)
		logger.Printf("started consumer: %v\n", c.Consumer)
	}

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case sig := <-osSignals:
			logger.Printf("caught signal %v: terminating\n", sig)
			for q := range quitPool {
				q <- true
			}
		}
	}()

	wg.Wait()
	logger.Println("all consumers closed")
}

// ProcessBatch default process function that sends bulk of messages from kafka to clichouse database table
func ProcessBatch(clickh *Clickhouse, pc *ProcessConfig, dbTableName string, lrf *LogReducedFields) ProcessMessagesFunc {
	// get clickhouse table structure
	tableColumns, err := clickh.GetColumns(dbTableName)
	if err != nil {
		panic(err)
	}
	// compile regexps for submatch mutator
	rgxps := make(map[string]*regexp.Regexp)
	if len(pc.SubMatchValues) > 0 {
		for field, rgxpStr := range pc.SubMatchValues {
			rgxps[field] = regexp.MustCompile(rgxpStr)
		}
	}
	return func(batch []*kafka.Message) error {
		var rows [][]byte

		for _, m := range batch {
			var msg Message
			if err := json.Unmarshal(m.Value, &msg); err != nil {
				return fmt.Errorf("could not parse message: %v", err)
			}

			msg.RenameFields(pc.RenameFields)
			msg.RemoveFields(pc.RemoveFields)
			if err := msg.SubMatchValues(rgxps); err != nil {
				return fmt.Errorf("could not submatch values: %v", err)
			}
			reducedFields := msg.ReduceToFields(tableColumns)
			lrf.LogReduced(reducedFields)
			msg.RemoveEmptyFields()

			b, err := json.Marshal(msg)
			if err != nil {
				return fmt.Errorf("could not serialize message to json: %v", err)
			}

			rows = append(rows, b)
		}
		return clickh.InsertIntoJSONEachRow(dbTableName, rows)
	}
}

func getOffsets(messages []*kafka.Message) []kafka.TopicPartition {
	offsets := make([]kafka.TopicPartition, len(messages))
	for i, m := range messages {
		offsets[i] = m.TopicPartition
		offsets[i].Offset++
	}
	return offsets
}

// LogProcessBatch log time spend on messages processing
func LogProcessBatch(l *log.Logger, process ProcessMessagesFunc) ProcessMessagesFunc {
	return func(messages []*kafka.Message) error {
		start := time.Now()
		l.Printf("start processing of %d messages", len(messages))
		err := process(messages)
		if err != nil {
			return err
		}
		l.Printf("finished processing of %d messages time: %v", len(messages), time.Since(start))
		return nil
	}
}

// LogReducedFields Custom logger for message processing
type LogReducedFields struct {
	logged map[string]bool
	*log.Logger
}

// LogReduced log field only once
func (l *LogReducedFields) LogReduced(fields map[string]interface{}) {
	for key, value := range fields {
		if _, ok := l.logged[key]; ok {
			continue
		}
		l.Printf("reduced field %s: %v", key, value)
		l.logged[key] = true
	}
}

// NewLogReducedFields creator
func NewLogReducedFields(l *log.Logger) *LogReducedFields {
	return &LogReducedFields{
		logged: make(map[string]bool),
		Logger: l,
	}
}
