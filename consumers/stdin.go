package consumers

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/mikechris/kaha/models"
)

type stdinConsumer struct {
	batchSize int
	loggger   *log.Logger
	process   processMessagesFunc
}

const consumerName = "stdin"

func (s stdinConsumer) String() string {
	return consumerName
}

func init() {
	registerConsumer(consumerName, newStdinConsumer)
}

func newStdinConsumer(config map[string]interface{}, processCfg models.ProcessConfig, debug bool) (Consumer, error) {
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
	process := processBatch(processCfg, models.NewLogReducedFields(logger))

	if debug {
		process = logProcessBatch(logger, process)
	}
	return &stdinConsumer{batchSize, models.NewLog(consumerName, 0), process}, nil

}

func (s *stdinConsumer) Consume(ctx context.Context, producer io.Writer, wg *sync.WaitGroup) {
	messages := make([]*models.Message, 0, s.batchSize)
	mes := read(os.Stdin) // reading from Stdin

	for {
		select {
		case <-ctx.Done():
			wg.Done()
			return
		case b, ok := <-mes:
			if ok {
				var msg models.Message
				if err := json.Unmarshal(b, &msg); err != nil {
					s.loggger.Println(err)
					wg.Done()
					return
				}

				messages = append(messages, &msg)

				if len(messages) != s.batchSize {
					continue
				}
			}

			if len(messages) > 0 {
				if err := s.process(producer, messages); err != nil {
					s.loggger.Println(err)
					wg.Done()
					return
				}
				messages = messages[:0]
			}

			if !ok {
				// end of stdin stream
				wg.Done()
				return
			}
		}
	}
}

func read(r io.Reader) <-chan []byte {
	bs := make(chan []byte)
	go func() {
		defer close(bs)
		scan := bufio.NewScanner(r)
		for scan.Scan() {
			b := scan.Bytes()
			bs <- b
		}
	}()
	return bs
}
