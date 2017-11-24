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
	logEOF     bool

	logger *log.Logger
}

func init() {
	registerConsumer("kafka", newKafkaConsumer)
}

func newKafkaConsumer(config map[string]interface{}, processCfg models.ProcessConfig, debug bool) (Consumer, error) {
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

	logger := models.NewLog(c.String(), 0)
	process := processBatch(processCfg, models.NewLogReducedFields(logger))

	if debug {
		process = logProcessBatch(logger, process)
	}

	return &kafkaConsumer{
		Consumer:   c,
		process:    process,
		autoCommit: cfgKafka.AutoCommit,
		batchSize:  cfgKafka.Batch,
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

				msgs := make([]*models.Message, len(messages))

				for i := 0; i < len(messages); i++ {
					var msg models.Message
					if err := json.Unmarshal(messages[i].Value, &msg); err != nil {
						shutDown(fmt.Errorf("could not parse message: %v", err))
						return
					}
					msgs[i] = &msg
				}

				if err := c.process(producer, msgs); err != nil {
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
