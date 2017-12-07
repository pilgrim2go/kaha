package consumer

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/mikechris/kaha/config"
	"github.com/mikechris/kaha/message"
)

type stdinConsumer struct {
	batchSize int
	process   processMessagesFunc
	logger    *log.Logger
}

const consumerName = "stdin"

func (s stdinConsumer) String() string {
	return consumerName
}

func init() {
	registerConsumer(consumerName, newStdinConsumer)
}

func newStdinConsumer(config map[string]interface{}, processCfg config.Process, debug bool, logger *log.Logger) (Consumer, error) {
	var batchSize int

	for k, v := range config {
		switch k {
		case "batch_size":
			i, ok := v.(int64)
			if !ok {
				return nil, fmt.Errorf("%s value: %v is not int", k, v)
			}
			batchSize = int(i)
		}
	}
	process := processBatch(processCfg, newLogReducedFields(logger))

	if debug {
		process = logProcessBatch(logger, process)
	}
	return &stdinConsumer{
		batchSize: batchSize,
		process:   process,
		logger:    logger,
	}, nil

}

func (s *stdinConsumer) Consume(ctx context.Context, producer io.Writer) error {
	messages := make([]*message.Message, 0, s.batchSize)
	stdin := read(os.Stdin, s.logger) // reading from Stdin

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("close: %v", ctx.Err())
		case b, ok := <-stdin:
			if ok {
				var msg message.Message
				if err := json.Unmarshal(b, &msg); err != nil {
					return fmt.Errorf("could not parse message: %v", err)
				}

				messages = append(messages, &msg)

				if len(messages) < s.batchSize {
					continue
				}
			}

			if len(messages) > 0 {
				if err := s.process(producer, messages); err != nil {
					return fmt.Errorf("could not process messages: %v", err)
				}
				messages = messages[:0]
			}

			if !ok {
				// end of stdin stream
				return errors.New("EOF input closed")
			}
		}
	}
}

func read(r io.Reader, logger *log.Logger) <-chan []byte {
	bs := make(chan []byte)
	go func() {
		defer close(bs)
		scan := bufio.NewScanner(r)
		for scan.Scan() {
			b := scan.Bytes()
			bs <- b
		}
		if err := scan.Err(); err != nil {
			logger.Printf("failed to scan input: %v\n", err)
		}
	}()
	return bs
}
