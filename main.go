package main

import (
	"flag"
	"github.com/Sirupsen/logrus"
	"github.com/brettlangdon/golab/server"
	"github.com/garyburd/redigo/redis"
	"net"
	"strings"
	"time"
)

func newPool(host string, idle int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     idle,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			server.Log.Debugln("Worker Connecting to Redis Host", host)
			c, err := redis.Dial("tcp", host)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			server.Log.Debugln("Worker PINGing Redis Host", host)
			_, err := c.Do("PING")
			return err
		},
	}
}

func main() {
	var bind = flag.String("bind", ":11222", "Which [host]:port to bind to [default: ':11222']")
	var level = flag.String("log", "INFO", "Set the log level [default: 'INFO']")
	var rhost = flag.String("redis", ":6379", "Which redis [host]:port to connect to [default: ':6379']")
	var rpool = flag.Int("pool", 3, "How many redis connections to have in the pool [default: 3]")
	flag.Parse()

	switch strings.ToLower(*level) {
	case "debug":
		server.Log.Level = logrus.DebugLevel
	case "info":
		server.Log.Level = logrus.InfoLevel
	case "warn":
		server.Log.Level = logrus.WarnLevel
	case "error":
		server.Log.Level = logrus.ErrorLevel
	}

	server.Log.Infoln("Starting Golab Server")
	ln, err := net.Listen("tcp", *bind)
	if err != nil {
		return
	}

	server.Log.Debugln("Connecting to Redis Pool", *rhost, "with", *rpool, "Connections")
	var pool *redis.Pool = newPool(*rhost, *rpool)

	server.Log.Infoln("Accepting Connections on", *bind)
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go server.HandleConnection(conn, pool)
	}
}
