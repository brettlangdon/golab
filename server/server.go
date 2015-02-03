package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"github.com/garyburd/redigo/redis"
	"net"
	"strconv"
)

func HandleConnection(conn net.Conn, pool *redis.Pool) {
	defer conn.Close()

	client := ClientConnection{
		conn:   conn,
		reader: bufio.NewReader(conn),
		pool:   pool,
	}
	Log.Debugln("New Client Connection =>", conn.RemoteAddr())

	for {
		line, err := client.ReadLine()
		if err != nil {
			break
		}

		Log.Debugln("New Message =>", string(line))
		err = HandleLine(line, client)
		if err != nil {
			break
		}
	}
}

func HandleLine(line []byte, client ClientConnection) (err error) {
	parts := bytes.Split(line, []byte(" "))

	if len(parts[0]) > 0 {
		command := bytes.ToLower(parts[0])
		args := parts[1:]
		switch string(command) {
		case "get", "gets":
			err = HandleGet(args, client)
		case "set", "replace":
			err = HandleSet(args, client)
		case "add":
			err = HandleAdd(args, client)
		case "delete":
			err = HandleDelete(args, client)
		case "incr":
			err = HandleIncr(args, client)
		case "touch":
			err = HandleTouch(args, client)
		case "stats":
			err = HandleStats(args, client)
		case "quit":
			err = errors.New("Quitting")
		case "decr", "flush", "append", "prepend", "cas":
			Log.Debugln("Command", string(command), "Not Implemented")
			_, err = client.ClientError([]byte("Command Not Implemented"))
		default:
			Log.Debugln("Command", string(command), "Unknown")
			_, err = client.ClientError([]byte("Unknown Command"))
		}
	}

	return err
}

func HandleGet(args [][]byte, client ClientConnection) (err error) {
	client.ObtainRedisConnection()
	defer client.ReleaseRedisConnection()

	if len(args) == 0 {
		_, err = client.ClientError([]byte("GET Command needs at least 1 '<experiment>:<user_id>' pair"))
	} else {
		for _, key := range args {
			parts := bytes.Split(key, []byte(":"))
			if len(parts) < 2 {
				_, err = client.ClientError([]byte("GET argument must be in the format '<experiment>:<user_id>'"))
			} else {
				if bytes.Equal(bytes.ToLower(parts[0]), []byte("experiment")) {
					ids := [][]byte{parts[1]}
					multi := false
					if bytes.Equal(parts[1], []byte("*")) {
						multi = true
						ids, err = client.GetAllExperimentIds()
						if err != nil {
							continue
						}
					} else if bytes.Equal(parts[1], []byte("active")) {
						multi = true
						ids, err = client.GetActiveExperimentIds()
						if err != nil {
							continue
						}
					}

					experiments := make([]Experiment, 0)
					for _, id := range ids {
						experimentId, err := strconv.ParseUint(string(id), 10, 64)
						if err != nil {
							continue
						}

						experiment, err := client.GetExperiment(experimentId, false)
						if err != nil {
							continue
						}
						experiments = append(experiments, experiment)
					}

					if len(experiments) == 0 {
						continue
					}

					var data []byte
					if len(experiments) == 1 && multi == false {
						data, err = json.Marshal(experiments[0])
					} else {
						data, err = json.Marshal(experiments)
					}

					if err != nil {
						continue
					}

					_, err = client.SendValue(key, data)
					if err != nil {
						continue
					}

				} else {
					experimentId, err := strconv.ParseUint(string(parts[0]), 10, 64)
					if err != nil {
						continue
					}
					experiment, err := client.GetExperiment(experimentId, true)
					if err != nil {
						continue
					}

					bucket, err := client.GetUserBucket(experiment, parts[1], true)
					_, err = client.SendValue(key, []byte(bucket.Value))
					if err != nil {
						break
					}
				}
			}
		}
		_, err = client.SendEnd()
	}

	return err
}

func HandleIncr(args [][]byte, client ClientConnection) (err error) {
	client.ObtainRedisConnection()
	defer client.ReleaseRedisConnection()

	if len(args) < 1 {
		_, err = client.ClientError([]byte("INCR command needs at least 1 argument <key> [delta]"))
	} else {
		parts := bytes.Split(args[0], []byte(":"))
		stored := false
		if len(parts) < 2 {
			_, err = client.ClientError([]byte("INCR key must be in the format '<experiment>:<user_id>'"))
		} else {
			experimentId, err := strconv.ParseUint(string(parts[0]), 10, 64)
			if err == nil {
				err = client.ConvertUser(experimentId, parts[1])
				stored = err == nil
			}

		}
		if stored {
			_, err = client.SendData([]byte("1"))
		} else if stored == false {
			_, err = client.SendData([]byte("0"))
		}
	}

	return err

}

func HandleAdd(args [][]byte, client ClientConnection) (err error) {
	client.ObtainRedisConnection()
	defer client.ReleaseRedisConnection()

	if len(args) < 4 {
		_, err = client.ClientError([]byte("ADD Command needs at least 4 arguments, <key> <flags> <exptime> <bytes> [noreply]"))
	} else {
		key, rawBytes := args[0], args[3]
		noreply := (len(args) >= 5 && bytes.Equal(bytes.ToLower(args[4]), []byte("noreply")))

		numBytes, err := strconv.ParseUint(string(rawBytes), 10, 64)
		stored := false
		if err == nil {
			data, err := client.ReadLine()
			if err != nil || uint64(len(data)) != numBytes {
				_, err = client.ClientError([]byte("Value length does not match number of bytes send"))
			} else {
				experimentId, err := strconv.ParseUint(string(key), 10, 64)
				if err != nil || experimentId <= 0 {
					_, err = client.ClientError([]byte("ADD key must be a positive integer"))
				}

				err = client.AddNewExperiment(data)
				stored = err == nil
			}
		}

		if stored && noreply == false {
			_, err = client.SendStored()
		} else if stored == false && noreply == false {
			_, err = client.SendNotStored()
		}

	}

	return err
}

func HandleSet(args [][]byte, client ClientConnection) (err error) {
	client.ObtainRedisConnection()
	defer client.ReleaseRedisConnection()

	if len(args) < 4 {
		_, err = client.ClientError([]byte("SET Command needs at least 4 arguments, <key> <flags> <exptime> <bytes> [noreply]"))
	} else {
		key, rawBytes := args[0], args[3]
		noreply := (len(args) >= 5 && bytes.Equal(bytes.ToLower(args[4]), []byte("noreply")))

		numBytes, err := strconv.ParseUint(string(rawBytes), 10, 64)
		stored := false
		if err == nil {
			data, err := client.ReadLine()
			if err != nil || uint64(len(data)) != numBytes {
				_, err = client.ClientError([]byte("Value length does not match number of bytes send"))
			} else {
				experimentId, err := strconv.ParseUint(string(key), 10, 64)
				if err != nil || experimentId <= 0 {
					_, err = client.ClientError([]byte("SET key must be a positive integer"))
				}

				err = client.UpdateExperiment(experimentId, data)
				stored = err == nil
			}
		}

		if stored && noreply == false {
			_, err = client.SendStored()
		} else if stored == false && noreply == false {
			_, err = client.SendNotStored()
		}

	}

	return err
}

func HandleStats(args [][]byte, client ClientConnection) (err error) {
	client.ObtainRedisConnection()
	defer client.ReleaseRedisConnection()

	if len(args) == 0 {
		args, err = client.GetActiveExperimentIds()
	}

	for _, rawId := range args {
		id, err := strconv.ParseUint(string(rawId), 10, 64)
		if err != nil {
			continue
		}
		err = client.GetExperimentStats(id)
		if err != nil {
			break
		}
	}
	_, err = client.SendEnd()

	return err
}

func HandleDelete(args [][]byte, client ClientConnection) (err error) {
	client.ObtainRedisConnection()
	defer client.ReleaseRedisConnection()

	if len(args) < 1 {
		_, err = client.ClientError([]byte("DELETE command takes 1 argument <key>"))
	} else {

		id, err := strconv.ParseUint(string(args[0]), 10, 64)
		if err == nil {
			err = client.DeactivateExperiment(id)
			if err == nil {
				_, err = client.SendDeleted()
			} else {
				_, err = client.SendNotFound()
			}
		} else {
			_, err = client.SendNotFound()
		}
	}

	return err
}

func HandleTouch(args [][]byte, client ClientConnection) (err error) {
	client.ObtainRedisConnection()
	defer client.ReleaseRedisConnection()

	if len(args) < 2 {
		_, err = client.ClientError([]byte("TOUCH command takes at least 2 arguments <key> <exptime> [noreply]"))
	} else {
		id, err := strconv.ParseUint(string(args[0]), 10, 64)
		noreply := (len(args) >= 3 && bytes.Equal(bytes.ToLower(args[2]), []byte("noreply")))

		if err == nil {
			err = client.ActivateExperiment(id)
			if err == nil && noreply == false {
				_, err = client.SendTouched()
			} else if noreply == false {
				_, err = client.SendNotFound()
			}
		} else if noreply == false {
			_, err = client.SendNotFound()
		}
	}

	return err
}
