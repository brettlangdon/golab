package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"math/rand"
	"net"
	"strconv"
)

type ClientConnection struct {
	conn      net.Conn
	reader    *bufio.Reader
	pool      *redis.Pool
	redisConn redis.Conn
}

func (client *ClientConnection) ObtainRedisConnection() {
	client.redisConn = client.pool.Get()
}

func (client *ClientConnection) ReleaseRedisConnection() {
	client.redisConn.Close()
	client.redisConn = nil
}

func (client *ClientConnection) ReadLine() (line []byte, err error) {
	line, err = client.reader.ReadBytes('\n')
	line = bytes.Trim(line, "\r\n ")
	return line, err
}

func (client *ClientConnection) Error() (num int, err error) {
	return client.conn.Write([]byte("ERROR\r\n"))
}

func (client *ClientConnection) ClientError(msg []byte) (num int, err error) {
	response := append([]byte("CLIENT_ERROR "), msg...)
	response = append(response, '\r', '\n')
	return client.conn.Write(response)
}

func (client *ClientConnection) ServerError(msg []byte) (num int, err error) {
	response := append([]byte("Server_ERROR "), msg...)
	response = append(response, '\r', '\n')
	return client.conn.Write(response)
}

func (client *ClientConnection) SendData(data []byte) (num int, err error) {
	data = append(data, '\r', '\n')
	return client.conn.Write(data)
}

func (client *ClientConnection) SendValue(key []byte, value []byte) (num int, err error) {
	response := append([]byte("VALUE "), key...)
	size := strconv.Itoa(len(value))

	response = append(response, []byte(" 0 ")...)
	response = append(response, []byte(size)...)
	response = append(response, '\r', '\n')
	response = append(response, value...)
	response = append(response, '\r', '\n')
	return client.conn.Write(response)
}

func (client *ClientConnection) SendStat(key []byte, value []byte) (num int, err error) {
	response := append([]byte("STAT "), key...)
	response = append(response, ' ')
	response = bytes.Replace(response, []byte(":"), []byte("."), -1)
	response = append(response, value...)
	response = append(response, '\r', '\n')
	return client.conn.Write(response)
}

func (client *ClientConnection) SendEnd() (num int, err error) {
	return client.conn.Write([]byte("END\r\n"))
}

func (client *ClientConnection) SendStored() (num int, err error) {
	return client.conn.Write([]byte("STORED\r\n"))
}

func (client *ClientConnection) SendNotStored() (num int, err error) {
	return client.conn.Write([]byte("NOT_STORED\r\n"))
}

func (client *ClientConnection) SendDeleted() (num int, err error) {
	return client.conn.Write([]byte("DELETED\r\n"))
}

func (client *ClientConnection) SendTouched() (num int, err error) {
	return client.conn.Write([]byte("TOUCHED\r\n"))
}

func (client *ClientConnection) SendNotFound() (num int, err error) {
	return client.conn.Write([]byte("NOT_FOUND\r\n"))
}

func (client *ClientConnection) GetExperiment(id uint64, active bool) (data Experiment, err error) {
	dummy := Experiment{Id: id}
	raw, err := client.redisConn.Do("GET", dummy.Key())
	if err != nil {
		fmt.Printf("%+v\r\n", err)
	} else if raw != nil {
		err = json.Unmarshal(raw.([]byte), &data)
		if err == nil && data.Id == 0 {
			data.Id = id
		}

		if err == nil && active {
			raw, err := client.redisConn.Do("SISMEMBER", "active_experiments", id)
			if err != nil || int(raw.(int64)) == 0 {
				return data, errors.New("Experiment is not active")
			}
		}

	} else {
		return data, errors.New("No Experiment Found")
	}
	return data, err
}

func (client *ClientConnection) GetUserBucket(experiment Experiment, userId []byte, assign bool) (data Variant, err error) {
	userKey := append([]byte("user:"), userId...)
	raw, err := client.redisConn.Do("HGET", userKey, experiment.Key())
	if err != nil {
		fmt.Printf("%+v\r\n", err)
	} else if raw != nil {
		variantId, err := strconv.ParseUint(string(raw.([]byte)), 10, 64)
		if err == nil {
			for _, variant := range experiment.Variants {
				if variant.Id == variantId {
					data = variant
				}
			}
		}
	}

	if assign && data.Id == 0 {
		idx := rand.Intn(len(experiment.Variants))
		data = experiment.Variants[idx]
		_, err = client.redisConn.Do("HSET", userKey, experiment.Key(), data.Id)
		_, err = client.redisConn.Do("SADD", experiment.BucketUniqueKey(data.Id), userId)
	}

	_, err = client.redisConn.Do("INCR", experiment.BucketImpressionsKey(data.Id))

	return data, err
}

func (client *ClientConnection) ConvertUser(experimentId uint64, userId []byte) (err error) {
	experiment, err := client.GetExperiment(experimentId, true)
	if err != nil {
		return err
	}

	bucket, err := client.GetUserBucket(experiment, userId, false)

	if bucket.Id != 0 {
		_, err = client.redisConn.Do("SADD", experiment.ConvertUniqueKey(bucket.Id), userId)
		_, err = client.redisConn.Do("INCR", experiment.ConvertImpressionsKey(bucket.Id))
	}

	return err
}

func (client *ClientConnection) GetActiveExperimentIds() (data [][]byte, err error) {
	raw, err := client.redisConn.Do("SMEMBERS", "active_experiments")

	for _, id := range raw.([]interface{}) {
		data = append(data, id.([]byte))
	}

	return data, err
}

func (client *ClientConnection) GetAllExperimentIds() (data [][]byte, err error) {
	raw, err := client.redisConn.Do("KEYS", "experiment:*")

	for _, key := range raw.([]interface{}) {
		parts := bytes.Split(key.([]byte), []byte(":"))
		if len(parts) != 2 {
			continue
		}
		data = append(data, parts[1])
	}

	return data, err
}

func (client *ClientConnection) GetExperimentStats(experimentId uint64) (err error) {
	experiment, err := client.GetExperiment(experimentId, false)

	for _, variant := range experiment.Variants {
		raw, _ := client.redisConn.Do("SCARD", experiment.BucketUniqueKey(variant.Id))
		if raw == nil {
			raw = int64(0)
		}
		client.SendStat(experiment.BucketUniqueKey(variant.Id), []byte(strconv.Itoa(int(raw.(int64)))))

		raw, _ = client.redisConn.Do("GET", experiment.BucketImpressionsKey(variant.Id))
		if raw == nil {
			raw = []uint8{0}
		}
		client.SendStat(experiment.BucketImpressionsKey(variant.Id), raw.([]uint8))

		raw, _ = client.redisConn.Do("SCARD", experiment.ConvertUniqueKey(variant.Id))
		if raw == nil {
			raw = int64(0)
		}
		client.SendStat(experiment.ConvertUniqueKey(variant.Id), []byte(strconv.Itoa(int(raw.(int64)))))

		raw, _ = client.redisConn.Do("GET", experiment.ConvertImpressionsKey(variant.Id))
		if raw == nil {
			raw = []uint8{0}
		}
		client.SendStat(experiment.ConvertImpressionsKey(variant.Id), raw.([]uint8))
	}

	return err
}

func (client *ClientConnection) AddNewExperiment(data []byte) (err error) {
	err = json.Unmarshal(data, new(interface{}))

	if err != nil {
		return err
	}

	raw, err := client.redisConn.Do("INCR", "experiments")
	nextId := uint64(raw.(int64))

	dummy := Experiment{Id: nextId}
	_, err = client.redisConn.Do("SET", dummy.Key(), data)
	return err
}

func (client *ClientConnection) UpdateExperiment(experimentId uint64, data []byte) (err error) {
	err = json.Unmarshal(data, new(interface{}))

	if err != nil {
		return err
	}

	experiment, err := client.GetExperiment(experimentId, false)

	if err != nil {
		return err
	}

	_, err = client.redisConn.Do("SET", experiment.Key(), data)
	return err
}

func (client *ClientConnection) DeactivateExperiment(experimentId uint64) (err error) {
	_, err = client.redisConn.Do("SREM", "active_experiments", experimentId)
	return err
}

func (client *ClientConnection) ActivateExperiment(experimentId uint64) (err error) {
	_, err = client.redisConn.Do("SADD", "active_experiments", experimentId)
	return err
}
