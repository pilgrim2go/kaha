package consumers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"regexp"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/mikechris/kaha/models"
)

type Consumer interface {
	Consume(producer io.Writer, ctx context.Context, wg *sync.WaitGroup)
}

type consumerInit func(map[string]interface{}, models.ProcessConfig, bool) (Consumer, error)

var regConsumers = map[string]consumerInit{}

var logger *log.Logger

func init() {
	logger = models.NewLog("consumer", 0)
}

// RegisterReadProvider add uninitialized read provider
func registerConsumer(name string, init consumerInit) {
	if _, ok := regConsumers[name]; ok {
		logger.Fatalf("consumer: %s already registered", name)
	}
	regConsumers[name] = init
}

func CreateConsumer(name string, number int, consumerCfg map[string]interface{}, processCfg models.ProcessConfig, debug bool) (consumers []Consumer, err error) {
	init, ok := regConsumers[name]
	if !ok {
		return nil, fmt.Errorf("consumer: %s not registered", name)
	}
	for i := 0; i < number; i++ {
		consumer, err := init(consumerCfg, processCfg, debug)
		if err != nil {
			return nil, fmt.Errorf("could not initilize consumer %s: %v", name, err)
		}
		consumers = append(consumers, consumer)
	}
	return consumers, nil
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

// LogProcessBatch log time spend on messages processing
func LogProcessBatch(l *log.Logger, process ProcessMessagesFunc) ProcessMessagesFunc {
	return func(producer io.Writer, messages []*kafka.Message) error {
		start := time.Now()
		l.Printf("start processing of %d messages", len(messages))
		err := process(producer, messages)
		if err != nil {
			return err
		}
		l.Printf("finished processing of %d messages time: %v", len(messages), time.Since(start))
		return nil
	}
}
