package models

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

type logWriter struct {
	out io.Writer
}

func (w logWriter) Write(bytes []byte) (int, error) {
	return fmt.Fprint(w.out, time.Now().UTC().Format("Jan 02 15:04:05")+" "+string(bytes))
}

func NewLog(out io.Writer, name string, flag int) *log.Logger {
	if name != "" {
		name = " " + name
	}
	prefix := strings.Split(os.Args[0], "/")
	name = prefix[len(prefix)-1] + name
	return log.New(&logWriter{out}, name+" ", flag)
}

// LogReducedFields Custom logger for message processing
type LogReducedFields struct {
	logged map[string]bool
	*log.Logger
}

// LogReduced log field only once
func (l *LogReducedFields) LogReduced(fields map[string]interface{}) {
	for key, value := range fields {
		if _, ok := l.logged[key]; ok {
			continue
		}
		l.Printf("reduced field %s: %v\n", key, value)
		l.logged[key] = true
	}
}

// NewLogReducedFields creator
func NewLogReducedFields(l *log.Logger) *LogReducedFields {
	return &LogReducedFields{
		logged: make(map[string]bool),
		Logger: l,
	}
}
