package config

import (
	"encoding/json"
	"fmt"
)

// Value for redisbed config
type Value struct {
	DataDir  string
	Host     string
	LogLevel string
	HTTPAddr string
	Prod     bool
}

// Load config
func Load() *Value {
	return &config
}

func init() {

	bytes, _ := json.Marshal(config)
	fmt.Println("redisbed conf", string(bytes))

}
