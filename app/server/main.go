package main

import (
	"github.com/zhiqiangxu/redisbed/pkg/rest"
)

func main() {

	server := rest.NewServer()
	server.Start()

}
