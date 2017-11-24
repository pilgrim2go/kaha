package consumers

import (
	"bytes"
	"context"
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

// ProcessMessagesFunc type used for message processing
type ProcessMessagesFunc func(io.Writer, []*kafka.Message) error

// Kafka kafka client
type Kafka struct {
	*kafka.Consumer
	process ProcessMessagesFunc

	autoCommit bool
	batchSize  int
	logEOF     bool

	logger *log.Logger
}

func init() {
	registerConsumer("kafka", newKafka)
}

func newKafka(config map[string]interface{}, processCfg models.ProcessConfig, debug bool) (Consumer, error) {
	var cfgKafka KafkaConfig

	buf := &bytes.Buffer{}
	if err := toml.NewEncoder(buf).Encode(config); err != nil {
		return nil, err
	}

	if err := toml.Unmarshal(buf.Bytes(), &cfgKafka); err != nil {
		return nil, err
	}

	consumerCfg := NewConsumerConfig(&cfgKafka.KafkaConsumerConnectConfig)

	c, err := kafka.NewConsumer(consumerCfg)

	if err != nil {
		return nil, fmt.Errorf("could not create consumer: %s", err)
	}

	err = c.SubscribeTopics(cfgKafka.Topics, nil)

	if err != nil {
		return nil, fmt.Errorf("could not to subscribe to topics: %s %s", cfgKafka.Topics, err)
	}

	logger := models.NewLog(c.String(), 0)
	process := ProcessBatch(processCfg, models.NewLogReducedFields(logger))

	if debug {
		process = LogProcessBatch(logger, process)
	}

	return &Kafka{
		Consumer:   c,
		process:    process,
		autoCommit: cfgKafka.AutoCommit,
		batchSize:  cfgKafka.Batch,
		logger:     logger,
		logEOF:     debug,
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
func (c *Kafka) Consume(producer io.Writer, ctx context.Context, wg *sync.WaitGroup) {
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
				c.logger.Printf(" not close: %v", err)
			}
			c.logger.Println("closed")
		case <-time.After(time.Second * 5):
			c.logger.Printf("timeout while closing")
		}
		wg.Done()
	}

	messages := make([]*kafka.Message, 0, c.batchSize)

	for {
		select {
		case <-ctx.Done():
			c.logger.Println(ctx.Err())
			shutDown(nil)
			return
		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				c.logger.Println(e)
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				c.logger.Println(e)
				c.Unassign()
			case *kafka.Message:
				messages = append(messages, e)
				if len(messages) != c.batchSize {
					continue
				}

				if err := c.process(producer, messages); err != nil {
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
				if c.logEOF {
					c.logger.Printf("reached %v\n", e)
				}
			case kafka.Error:
				shutDown(e)
				return
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
