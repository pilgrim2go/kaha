package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"regexp"
	"sync"
	"time"

	"github.com/mikechris/kaha/config"
	"github.com/mikechris/kaha/message"
)

// Consumer should read, process and  send data to io.Writer (producer).
// Consume should stop on event ctx.Done() and signalize end with wg.Done().
type Consumer interface {
	Consume(ctx context.Context, producer io.Writer, wg *sync.WaitGroup)
}

type consumerInit func(map[string]interface{}, config.Process, bool, *log.Logger) (Consumer, error)

var regConsumers = map[string]consumerInit{}

func registerConsumer(name string, init consumerInit) {
	if _, ok := regConsumers[name]; ok {
		panic(fmt.Sprintf("consumer %s already registered", name))
	}
	regConsumers[name] = init
}

// CreateConsumer initilizes registered consumer.
func CreateConsumer(name string, consumerCfg map[string]interface{}, processCfg config.Process, debug bool, logger *log.Logger) (consumers Consumer, err error) {
	init, ok := regConsumers[name]
	if !ok {
		return nil, fmt.Errorf("%s not registered", name)
	}
	consumer, err := init(consumerCfg, processCfg, debug, logger)
	if err != nil {
		return nil, fmt.Errorf("could not initilize %s: %v", name, err)
	}
	return consumer, nil
}

// processMessagesFunc type used for message processing.
type processMessagesFunc func(io.Writer, []*message.Message) error

// processBatch processes and sends bulk of messages to given producer.
func processBatch(pc config.Process, lrf *logReducedFields) processMessagesFunc {
	// compile regexps for submatch mutator
	rgxps := make(map[string]*regexp.Regexp)
	if len(pc.SubMatchValues) > 0 {
		for field, rgxpStr := range pc.SubMatchValues {
			rgxps[field] = regexp.MustCompile(rgxpStr)
		}
	}
	return func(producer io.Writer, batch []*message.Message) error {
		var rows []byte

		for _, msg := range batch {
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
				return fmt.Errorf("could not serialize message: %v", err)
			}

			rows = append(rows, b...)
			rows = append(rows, []byte("\n")...)
		}
		_, err := producer.Write(rows)
		return err
	}
}

// logProcessBatch log time spend by process function.
func logProcessBatch(l *log.Logger, process processMessagesFunc) processMessagesFunc {
	return func(producer io.Writer, messages []*message.Message) error {
		start := time.Now()
		l.Printf("start processing of %d messages\n", len(messages))
		err := process(producer, messages)
		if err != nil {
			return err
		}
		l.Printf("finished processing of %d messages time: %v\n", len(messages), time.Since(start))
		return nil
	}
}

// logReducedFields contains custom logger for message processing.
type logReducedFields struct {
	logged map[string]bool
	*log.Logger
}

// LogReduced logs field only once.
func (l *logReducedFields) LogReduced(fields map[string]interface{}) {
	for key, value := range fields {
		if _, ok := l.logged[key]; ok {
			continue
		}
		l.Printf("reduced field %s: %v\n", key, value)
		l.logged[key] = true
	}
}

// newLogReducedFields creates new custom logger.
func newLogReducedFields(l *log.Logger) *logReducedFields {
	return &logReducedFields{
		logged: make(map[string]bool),
		Logger: l,
	}
}
