package io

// NewRedisInput for input
type NewRedisInput struct {
	Port uint16
}

// NewRedisOutput for output
type NewRedisOutput struct {
	BaseResp
	Port uint16 `json:"port"`
}
