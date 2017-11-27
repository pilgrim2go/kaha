package consumers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/mikechris/kaha/models"
)

// kafkaConfig process and consumer
type kafkaConfig struct {
	kafkaConsumerConfig
	kafkaConsumerConnectConfig
}

// kafkaConsumerConfig process
type kafkaConsumerConfig struct {
	Topics    []string `toml:"topics"`
	Consumers int      `toml:"consumers"`
	Batch     int      `toml:"batch_size"`
	MaxWait   int      `toml:"max_wait_seconds"`
}

// kafkaConsumerConnectConfig consumer
type kafkaConsumerConnectConfig struct {
	Brokers            []string `toml:"brokers"`
	Group              string   `toml:"group"`
	AutoCommit         bool     `toml:"auto_commit"`
	AutoCommitInterval int      `toml:"auto_commit_interval_ms"`
	AutoOffsetReset    string   `toml:"auto_offset_reset"`
	SessionTimeout     int      `toml:"session_timeout_ms"`
}

// kafkaConsumer kafkaConsumer client
type kafkaConsumer struct {
	*kafka.Consumer
	process processMessagesFunc

	autoCommit bool
	batchSize  int
	maxWait    time.Duration
	logEOF     bool

	logger *log.Logger
}

func init() {
	registerConsumer("kafka", newKafkaConsumer)
}

func newKafkaConsumer(config map[string]interface{}, processCfg models.ProcessConfig, debug bool, logger *log.Logger) (Consumer, error) {
	var cfgKafka kafkaConfig

	buf := &bytes.Buffer{}
	if err := toml.NewEncoder(buf).Encode(config); err != nil {
		return nil, err
	}

	if err := toml.Unmarshal(buf.Bytes(), &cfgKafka); err != nil {
		return nil, err
	}

	consumerCfg := newConsumerConfig(&cfgKafka.kafkaConsumerConnectConfig)

	c, err := kafka.NewConsumer(consumerCfg)

	if err != nil {
		return nil, fmt.Errorf("could not create consumer: %s", err)
	}

	err = c.SubscribeTopics(cfgKafka.Topics, nil)

	if err != nil {
		return nil, fmt.Errorf("could not to subscribe to topics: %s %s", cfgKafka.Topics, err)
	}

	logger.SetPrefix(logger.Prefix() + c.String() + " ")

	process := processBatch(processCfg, models.NewLogReducedFields(logger))

	if debug {
		process = logProcessBatch(logger, process)
	}

	return &kafkaConsumer{
		Consumer:   c,
		process:    process,
		autoCommit: cfgKafka.AutoCommit,
		batchSize:  cfgKafka.Batch,
		maxWait:    time.Duration(cfgKafka.MaxWait) * time.Second,
		logger:     logger,
		logEOF:     debug,
	}, nil
}

// newConsumerConfig create new consumer config
func newConsumerConfig(cfg *kafkaConsumerConnectConfig) *kafka.ConfigMap {
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
func (c *kafkaConsumer) Consume(ctx context.Context, producer io.Writer, wg *sync.WaitGroup) {
	shutDown := func(err error) {
		if err != nil {
			c.logger.Println(err)
		}

		c.logger.Println("trying to close")

		errs := make(chan error)
		go func() {
			errs <- c.Close()
		}()

		select {
		case err := <-errs:
			if err != nil {
				c.logger.Printf("could not close: %v", err)
			}
			c.logger.Println("closed")
		case <-time.After(time.Second * 5):
			c.logger.Printf("timeout while closing")
		}
		wg.Done()
	}

	wait := time.Now()
	queue := make([]*kafka.Message, 0, c.batchSize)

loop:
	for {
		select {
		case <-ctx.Done():
			c.logger.Println(ctx.Err())
			shutDown(nil)
			break loop
		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				c.logger.Println(e)
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				c.logger.Println(e)
				c.Unassign()
			case *kafka.Message:
				queue = append(queue, e)
				if len(queue) < c.batchSize && time.Since(wait) < c.maxWait {
					continue
				}

				messages := make([]*models.Message, len(queue))

				for i := 0; i < len(queue); i++ {
					var msg models.Message
					if err := json.Unmarshal(queue[i].Value, &msg); err != nil {
						shutDown(fmt.Errorf("could not parse message: %v", err))
						break loop
					}
					messages[i] = &msg
				}

				if err := c.process(producer, messages); err != nil {
					shutDown(fmt.Errorf("could not process batch: %v", err))
					break loop
				}

				if !c.autoCommit {
					if _, err := c.CommitOffsets(getOffsets(queue)); err != nil {
						shutDown(fmt.Errorf("could not commit offsets: %v", err))
						break loop
					}
				}

				queue = queue[:0] // trim back to zero size
				wait = time.Now()
			case kafka.PartitionEOF:
				if c.logEOF {
					c.logger.Printf("reached %v\n", e)
				}
			case kafka.Error:
				shutDown(e)
				break loop
			}
		}
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
