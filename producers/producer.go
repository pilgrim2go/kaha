package producers

import (
	"fmt"
	"io"
	"log"

	"github.com/mikechris/kaha/models"
)

type producerInit func(map[string]interface{}, bool) (io.Writer, error)

var regConsumers = map[string]producerInit{}

var logger *log.Logger

func init() {
	logger = models.NewLog("producer", 0)
}

// RegisterReadProvider add uninitialized read provider
func registerProducer(name string, init producerInit) {
	if _, ok := regConsumers[name]; ok {
		logger.Fatalf("producer: %s already registered", name)
	}
	regConsumers[name] = init
}

func CreateProducer(name string, config map[string]interface{}, debug bool) (producer io.Writer, err error) {
	init, ok := regConsumers[name]
	if !ok {
		return nil, fmt.Errorf("producer: %s not registered", name)
	}
	producer, err = init(config, debug)
	if err != nil {
		return nil, fmt.Errorf("could not initilize producer %s: %v", name, err)
	}

	return producer, nil
}
