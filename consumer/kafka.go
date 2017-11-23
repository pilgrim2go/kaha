package consumer

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/mikechris/kaha/models"
)

func init() {
	registerConsumer("kafka", newKafka)
}

func newKafka(config map[string]interface{}, processCfg models.ProcessConfig) (Consumer, error) {
	var brokers, topics []string
	var group, autoOffsetReset string
	var autoCommit bool
	var autoCommitInterval, sessionTimeout, batchSize int

	for k, v := range config {
		switch k {
		case "brokers":
			a, ok := v.([]interface{})
			if !ok {
				return nil, fmt.Errorf("k %s value %v is not an array", k, v)
			}
			var as []string
			for _, s := range a {
				s, ok := s.(string)
				if !ok {
					return nil, fmt.Errorf("%s value %v is not string", k, s)
				}
				as = append(as, s)
			}
			brokers = as
		case "group":
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s value %v is not string", k, v)
			}
			group = s
		case "topics":
			fmt.Println(reflect.TypeOf(v))
			a, ok := v.([]interface{})
			if !ok {
				return nil, fmt.Errorf("%s value %v is not an array", k, v)
			}
			var as []string
			for _, s := range a {
				s, ok := s.(string)
				if !ok {
					return nil, fmt.Errorf("%s value %v is not string", k, s)
				}
				as = append(as, s)
			}
			topics = as
		case "auto_commit":
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("%s value %v is not bool", k, v)
			}
			autoCommit = b
		case "auto_commit_interval_ms":
			i, ok := v.(int64)
			if !ok {
				return nil, fmt.Errorf("%s value %v is not int", k, v)
			}
			autoCommitInterval = int(i)
		case "auto_offset_reset":
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s value %v is not string", k, v)
			}
			autoOffsetReset = s
		case "session_timeout_ms":
			i, ok := v.(int64)
			if !ok {
				return nil, fmt.Errorf("%s value %v is not int", k, v)
			}
			sessionTimeout = int(i)
		case "batch_size":
			i, ok := v.(int64)
			if !ok {
				return nil, fmt.Errorf("%s value %v is not int", k, v)
			}
			batchSize = int(i)
		}
	}
	consumerConfig := NewConsumerConfig(&KafkaConsumerConnectConfig{
		Brokers:            brokers,
		Group:              group,
		AutoCommit:         autoCommit,
		AutoCommitInterval: autoCommitInterval,
		AutoOffsetReset:    autoOffsetReset,
		SessionTimeout:     sessionTimeout,
	})

	c, err := kafka.NewConsumer(consumerConfig)

	if err != nil {
		return nil, fmt.Errorf("could not create consumer: %s", err)
	}

	err = c.SubscribeTopics(topics, nil)

	if err != nil {
		return nil, fmt.Errorf("could not to subscribe to topics: %s %s", topics, err)
	}
	logger := models.NewLog(c.String(), 0)
	return &Kafka{
		Consumer:   c,
		process:    ProcessBatch(processCfg, models.NewLogReducedFields(logger)),
		autoCommit: autoCommit,
		batchSize:  batchSize,
		logger:     logger,
	}, nil
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

// Consume read and process messages from kafka
func (c *Kafka) Consume(producer io.Writer, wg *sync.WaitGroup, quit chan bool) {
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

// ProcessBatch default process function that sends bulk of messages from kafka to clichouse database table
func ProcessBatch(pc models.ProcessConfig, lrf *models.LogReducedFields) ProcessMessagesFunc {
	// compile regexps for submatch mutator
	rgxps := make(map[string]*regexp.Regexp)
	if len(pc.SubMatchValues) > 0 {
		for field, rgxpStr := range pc.SubMatchValues {
			rgxps[field] = regexp.MustCompile(rgxpStr)
		}
	}
	return func(producer io.Writer, batch []*kafka.Message) error {
		var rows []byte

		for _, m := range batch {
			var msg models.Message
			if err := json.Unmarshal(m.Value, &msg); err != nil {
				return fmt.Errorf("could not parse message: %v", err)
			}

			msg.RenameFields(pc.RenameFields)
			msg.RemoveFields(pc.RemoveFields)
			if err := msg.SubMatchValues(rgxps); err != nil {
				return fmt.Errorf("could not submatch values: %v", err)
			}
			reducedFields := msg.ReduceToFields(pc.OnlyFields)
			lrf.LogReduced(reducedFields)
			msg.RemoveEmptyFields()

			b, err := json.Marshal(msg)
			if err != nil {
				return fmt.Errorf("could not serialize message to json: %v", err)
			}

			rows = append(rows, b...)
			rows = append(rows, []byte("\n")...)
		}
		_, err := producer.Write(rows)
		return err
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

// KafkaConsumerConnectConfig consumer
type KafkaConsumerConnectConfig struct {
	Brokers            []string
	Group              string
	AutoCommit         bool
	AutoCommitInterval int
	AutoOffsetReset    string
	SessionTimeout     int
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
