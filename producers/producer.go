package producers

import (
	"fmt"
	"io"
	"log"
)

type producerInit func(map[string]interface{}, bool, *log.Logger) (io.Writer, error)

var regConsumers = map[string]producerInit{}

// registerProducer add uninitialized producer
func registerProducer(name string, init producerInit) {
	if _, ok := regConsumers[name]; ok {
		panic(fmt.Sprintf("producer: %s already registered", name))
	}
	regConsumers[name] = init
}

func CreateProducer(name string, config map[string]interface{}, debug bool, logger *log.Logger) (producer io.Writer, err error) {
	init, ok := regConsumers[name]
	if !ok {
		return nil, fmt.Errorf("producer: %s not registered", name)
	}
	producer, err = init(config, debug, logger)
	if err != nil {
		return nil, fmt.Errorf("could not initilize producer %s: %v", name, err)
	}

	return producer, nil
}
