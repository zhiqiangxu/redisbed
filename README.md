# redisbed

generate redis service on demand

## feature list

- [x] `get`
- [x] `set`
- [x] `del`
- [ ] `lpush`
- [ ] `lpop`
- [ ] `hset`
- [ ] `hget`
- [ ] `hdel`

## run

```go run -mod=vendor app/server/main.go```


## generate redis service

```curl -XPOST localhost:8888/redis```

## list all redis service

```curl localhost:8888/redis```