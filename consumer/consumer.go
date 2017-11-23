package consumer

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/mikechris/kaha/models"
)

type Consumer interface {
	Consume(producer io.Writer, wg *sync.WaitGroup, quit chan bool)
}

type consumerInit func(map[string]interface{}, models.ProcessConfig) (Consumer, error)

var regConsumers = map[string]consumerInit{}

var logger *log.Logger

func init() {
	logger = models.NewLog("consumer", 0)
}

// RegisterReadProvider add uninitialized read provider
func registerConsumer(name string, init consumerInit) {
	if _, ok := regConsumers[name]; ok {
		logger.Fatalf("consumer %s already registered", name)
	}
	regConsumers[name] = init
}

func CreateConsumer(name string, number int, consumerCfg map[string]interface{}, processCfg models.ProcessConfig) (consumers []Consumer, err error) {
	init, ok := regConsumers[name]
	if !ok {
		return nil, fmt.Errorf("consumer %s not registered", name)
	}
	for i := 0; i < number; i++ {
		consumer, err := init(consumerCfg, processCfg)
		if err != nil {
			return nil, fmt.Errorf("could not initilize %s consumer %v", name, err)
		}
		consumers = append(consumers, consumer)
	}
	return consumers, nil
}

// RunConsumer start and manage one or more consumers
func RunConsumer(logger *log.Logger, producer io.Writer, consumer ...Consumer) {
	quitPool := make(chan chan bool, len(consumer))
	var wg sync.WaitGroup

	for _, c := range consumer {
		q := make(chan bool)
		quitPool <- q

		wg.Add(1)
		go c.Consume(producer, &wg, q)
		logger.Printf("started consumer %v\n", c)
	}

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case sig := <-osSignals:
			logger.Printf("caught signal %v terminating\n", sig)
			for q := range quitPool {
				q <- true
			}
		}
	}()

	wg.Wait()
	logger.Println("all consumers closed")
}
