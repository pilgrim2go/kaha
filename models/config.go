package models

// Config all
type Config struct {
	Consumers []ConsumerConfig `toml:"consumer"`
}

type ConsumerConfig struct {
	Name           string                 `toml:"name"`
	Consumers      int                    `toml:"consumers"`
	Config         map[string]interface{} `toml:"config"`
	ProcessConfig  ProcessConfig          `toml:"process"`
	ProducerConfig ProducerConfig         `toml:"producer"`
}

type ProducerConfig struct {
	Name   string                 `toml:"name"`
	Config map[string]interface{} `toml:"config"`
}

// ProcessConfig message mutate options
type ProcessConfig struct {
	RenameFields   map[string]string `toml:"rename_fields"`
	SubMatchValues map[string]string `toml:"submatch_balues"`
	RemoveFields   []string          `toml:"remove_fields"`
	OnlyFields     []string          `toml:"only_fields"`
}
