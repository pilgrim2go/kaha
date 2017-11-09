package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ProcessMessagesFunc func([]*kafka.Message) error

type Consumer struct {
	autocommit bool
	batch      int
	process    ProcessMessagesFunc
	*kafka.Consumer

	wg     *sync.WaitGroup
	quit   chan bool
	logger *log.Logger
}

func NewConsumer(cfg *KafkaConfig, processBatch ProcessMessagesFunc, wg *sync.WaitGroup, quit chan bool, logger *log.Logger) (*Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               cfg.Broker,
		"group.id":                        cfg.Group,
		"session.timeout.ms":              cfg.SessionTimeout,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"enable.auto.commit":              cfg.AutoCommit,
		"auto.commit.interval.ms":         cfg.AutoCommitInterval,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": cfg.AutoOffsetReset}})

	if err != nil {
		return nil, fmt.Errorf("could not create consumer: %s", err)
	}

	err = c.SubscribeTopics(cfg.Topics, nil)

	if err != nil {
		return nil, fmt.Errorf("could not to subscribe to topics: %s %s", cfg.Topics, err)
	}
	return &Consumer{
		autocommit: cfg.AutoCommit,
		batch:      cfg.Batch,
		process:    processBatch,
		Consumer:   c,
		wg:         wg,
		quit:       quit,
		logger:     logger,
	}, nil
}

func (kc *Consumer) Feed() {
	batch := make([]*kafka.Message, 0, kc.batch)
	run := true

	errHandle := func(err error) {
		kc.logger.Println(err)
		run = false
	}

	for run == true {
		select {
		case <-kc.quit:
			run = false
		case ev := <-kc.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				kc.logger.Printf("%v\n", e)
				kc.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				kc.logger.Printf("%v\n", e)
				kc.Unassign()
			case *kafka.Message:
				batch = append(batch, e)
				if len(batch) != kc.batch {
					continue
				}

				start := time.Now()
				kc.logger.Printf("processing batch size: %v ", kc.batch)

				if err := kc.process(batch); err != nil {
					errHandle(fmt.Errorf("could not process batch: %v", err))
					continue
				}

				if kc.autocommit {
					if _, err := kc.CommitOffsets(getOffsets(batch)); err != nil {
						errHandle(fmt.Errorf("could not commit offsets: %v", err))
						continue
					}
				}

				batch = batch[:0] // trim back to zero size
				kc.logger.Printf("batch processed time: %v\n", time.Since(start))
			case kafka.PartitionEOF:
				kc.logger.Printf("reached %v\n", e)
			case kafka.Error:
				errHandle(e)
			}
		}
	}

	kc.logger.Printf("closing consumer: %v\n", kc.Consumer)

	errs := make(chan error)
	go func() {
		errs <- kc.Close()
	}()

	select {
	case err := <-errs:
		if err != nil {
			kc.logger.Printf("could not close consumer: %v", err)
		}
		kc.logger.Printf("closed consumer: %v\n", kc.Consumer)
	case <-time.After(time.Second * 5):
		kc.logger.Printf("timeout while closing consumer: %v\n", kc.Consumer)
	}
	kc.wg.Done()
}

func processBatch(clickh *Clickhouse, kc *ProcessConfig, dbTableName string, tableColumns []string, l *logReducedFields) ProcessMessagesFunc {
	return func(batch []*kafka.Message) error {
		var rows [][]byte

		for _, m := range batch {
			var msg Message
			if err := json.Unmarshal(m.Value, &msg); err != nil {
				return fmt.Errorf("could not parse message: %v", err)
			}

			msg.FlatFields(kc.FlatFields)
			msg.RemoveFields(kc.RemoveFields)
			reducedFields := msg.ReduceFields(tableColumns)
			l.logReduced(reducedFields)
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
