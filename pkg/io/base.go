package io

// BaseResp for resp std
type BaseResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}
