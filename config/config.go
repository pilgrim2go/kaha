package config

import (
	"fmt"
	"io/ioutil"

	"github.com/BurntSushi/toml"
)

// Config contains configuration of kaha.
type Config struct {
	Consumers []*struct {
		Name      string                 `toml:"name"`
		Consumers int                    `toml:"consumers"`
		Custom    map[string]interface{} `toml:"config"`
		Process   Process                `toml:"process"`
		Producer  struct {
			Name   string                 `toml:"name"`
			Custom map[string]interface{} `toml:"config"`
		} `toml:"producer"`
	} `toml:"consumer"`
}

// Process contains configuration for message mutators.
type Process struct {
	RenameFields   map[string]string `toml:"rename_fields"`
	SubMatchValues map[string]string `toml:"submatch_values"`
	RemoveFields   []string          `toml:"remove_fields"`
	OnlyFields     []string          `toml:"only_fields"`
}

// FromFile loads config from given path.
func FromFile(path string) (*Config, error) {
	var cfg Config

	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not read file %s: %v", path, err)
	}

	if _, err = toml.Decode(string(b), &cfg); err != nil {
		return nil, fmt.Errorf("could not parse file %s: %v", path, err)
	}
	return &cfg, nil
}
