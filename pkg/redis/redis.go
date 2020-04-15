package redis

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/redcon"
	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/kv"
	"github.com/zhiqiangxu/redisbed/pkg/logger"
	"github.com/zhiqiangxu/redisbed/pkg/store"
	"go.uber.org/zap"
)

// StartByPort used when bootstrap
func StartByPort(port uint16) (err error) {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return
	}

	go Start(l)
	return
}

// Start a redis server on port
func Start(l net.Listener) (serr error) {

	port := uint16(l.Addr().(*net.TCPAddr).Port)
	logger.Instance().Info("New Redis", zap.Uint16("port", port))

	kvdb := store.Instance().GetRedisDB(port)

	serr = redcon.Serve(l,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "set":
				if len(cmd.Args) < 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				var meta *mondis.VMetaReq
				txn := kvdb.NewTransaction(true)
				defer txn.Discard()

				if len(cmd.Args) > 3 {
					i := 3
					for {
						switch strings.ToLower(string(cmd.Args[i])) {
						case "ex":
							d, err := strconv.Atoi(string(cmd.Args[i]))
							if err != nil {
								writeError(conn, err)
								return
							}
							duration := time.Second * time.Duration(d)
							meta = &mondis.VMetaReq{TTL: duration}
							i += 2
						case "px":
							d, err := strconv.Atoi(string(cmd.Args[i]))
							if err != nil {
								writeError(conn, err)
								return
							}
							duration := time.Millisecond * time.Duration(d)
							meta = &mondis.VMetaReq{TTL: duration}
							i += 2
						case "nx":
							_, _, err := txn.Get(cmd.Args[1])
							if err == kv.ErrKeyNotFound {
								err = nil
							} else if err == nil {
								conn.WriteNull()
								return
							}
							if err != nil {
								writeError(conn, err)
								return
							}
							i++
						case "xx":
							_, _, err := txn.Get(cmd.Args[1])
							if err == kv.ErrKeyNotFound {
								conn.WriteNull()
								return
							}
							if err != nil {
								writeError(conn, err)
								return
							}
							i++
						}
						if i >= len(cmd.Args) {
							break
						}
					}
				}
				err := txn.Set(cmd.Args[1], cmd.Args[2], meta)
				if err != nil {
					writeError(conn, err)
					return
				}
				err = txn.Commit()
				if err == nil {
					conn.WriteString("OK")
				} else {
					writeError(conn, err)
				}

			case "get":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}

				val, _, err := kvdb.Get(cmd.Args[1])
				if err == kv.ErrKeyNotFound {
					conn.WriteNull()
				} else if err != nil {
					writeError(conn, err)
				} else {
					conn.WriteBulk(val)
				}
			case "del":
				if len(cmd.Args) < 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}

				txn := kvdb.NewTransaction(true)
				defer txn.Discard()

				var n int
				for i := 1; i < len(cmd.Args); i++ {
					exists, err := txn.Exists(cmd.Args[1])
					if err != nil {
						writeError(conn, err)
						return
					}
					if exists {
						n++
					} else {
						continue
					}
					err = txn.Delete(cmd.Args[1])
					if err != nil {
						writeError(conn, err)
						return
					}
				}

				err := txn.Commit()
				if err != nil {
					writeError(conn, err)
					return
				}

				conn.WriteInt(n)
			}
		},
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)

	return
}

func writeError(conn redcon.Conn, err error) {
	conn.WriteError(fmt.Sprintf("Fail: %v", err))
}
