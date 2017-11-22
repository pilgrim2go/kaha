package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/BurntSushi/toml"
)

type logWriter struct {
}

func (writer logWriter) Write(bytes []byte) (int, error) {
	return fmt.Print(time.Now().UTC().Format("Jan 02 15:04:05") + " " + string(bytes))
}

// NewLog crate new log in syslog format
func NewLog(name string, flag int) *log.Logger {
	return log.New(new(logWriter), name+" ", flag)
}

func loadConfig(f string) (cfg Config, err error) {
	b, err := ioutil.ReadFile(f)
	if err != nil {
		return cfg, fmt.Errorf("could not read file %s: %v", f, err)
	}

	if _, err = toml.Decode(string(b), &cfg); err != nil {
		return cfg, fmt.Errorf("could not parse toml %s: %v", f, err)
	}

	return cfg, nil
}

type LogReducedFields struct {
	logged map[string]bool
	*log.Logger
}

func (l *LogReducedFields) LogReduced(fields map[string]interface{}) {
	for key, value := range fields {
		if _, ok := l.logged[key]; ok {
			continue
		}
		l.Printf("reduced field %s: %v", key, value)
		l.logged[key] = true
	}
}

func NewLogReducedFields(l *log.Logger) *LogReducedFields {
	return &LogReducedFields{
		logged: make(map[string]bool),
		Logger: l,
	}
}
