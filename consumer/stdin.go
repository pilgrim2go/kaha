package consumer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/mikechris/kaha/model"
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

func newStdinConsumer(config map[string]interface{}, processCfg model.ProcessConfig, debug bool, logger *log.Logger) (Consumer, error) {
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
	process := processBatch(processCfg, model.NewLogReducedFields(logger))

	if debug {
		process = logProcessBatch(logger, process)
	}
	return &stdinConsumer{
		batchSize: batchSize,
		process:   process,
		logger:    logger,
	}, nil

}

func (s *stdinConsumer) Consume(ctx context.Context, producer io.Writer, wg *sync.WaitGroup) {
	messages := make([]*model.Message, 0, s.batchSize)
	stdin := read(os.Stdin, s.logger) // reading from Stdin

loop:
	for {
		select {
		case <-ctx.Done():
			wg.Done()
			return
		case b, ok := <-stdin:
			if ok {
				var msg model.Message
				if err := json.Unmarshal(b, &msg); err != nil {
					s.logger.Printf("could not parse message: %v\n", err)
					wg.Done()
					break loop
				}

				messages = append(messages, &msg)

				if len(messages) < s.batchSize {
					continue
				}
			}

			if len(messages) > 0 {
				if err := s.process(producer, messages); err != nil {
					s.logger.Println(err)
					wg.Done()
					break loop
				}
				messages = messages[:0]
			}

			if !ok {
				// end of stdin stream
				s.logger.Println("EOF input closed\n")
				wg.Done()
				break loop
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
