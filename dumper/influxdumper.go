package dumper

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"

	"crypto/tls"

	"io/ioutil"

	"github.com/dgrijalva/jwt-go"
	"github.com/garyburd/redigo/redis"
	influx "github.com/influxdata/influxdb/client/v2"
)

const (
	batch      = 1000
	queue_min  = "statistics:300"
	queue_hour = "statistics:3600"
	policy     = "dumper"
	timeout    = 30
	key        = `\
-----BEGIN PUBLIC KEY-----
MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAES5X8XrfKdx9gYayFITc89wad4usrk0n2
7MjiGYvqalizeSWTHEpnd7oea9IQ8T5oJjMVH5cc0H5tFSKilFFeh//wngxIyny6
6+Vq5t5B0V0Ehy01+2ceEon2Y0XDkIKv
-----END PUBLIC KEY-----`
	refresh_url = "https://itsyou.online/v1/oauth/jwt/refresh?validity=3600"
)

type statistics struct {
	Avg   float64
	Total float64
	Max   float64
	Count int64
	Start int64
	Key   string
	Tags  map[string]string
}

type influxDumper struct {
	Client    influx.Client
	Node      string
	JWT       string
	IP        string
	Port      int64
	Database  string
	Retention string
}

func (in *influxDumper) Start() error {
	influxClient, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr: fmt.Sprintf("http://%v:%v", in.IP, in.Port),
	})
	if err != nil {
		log.Errorln("Error initializing influxdb client: ", err)
		return err
	}
	in.Client = influxClient
	defer in.Client.Close()

	q := influx.NewQuery(fmt.Sprintf("CREATE DATABASE %v", in.Database), "", "")
	_, err = in.handleRequest(q)
	if err != nil {
		log.Errorln("Error creating influxdb database: ", err)
		return err
	}

	q = influx.NewQuery(
		fmt.Sprintf("CREATE RETENTION POLICY \"%v\" ON %v DURATION %v REPLICATION 1 DEFAULT",
			policy, in.Database, in.Retention), "", "")
	_, err = in.handleRequest(q)
	if err != nil {
		log.Errorln("Error creating retention policy: ", err)
		return err
	}

	return in.dump()
}

func (in *influxDumper) dump() error {
	cl := newPool(in.Node, in.JWT)
	con := cl.Get()
	defer con.Close()

	for {
		batchPoints, err := in.getBatchPoints(con)
		if err != nil {
			return err
		}

		if len(batchPoints.Points()) == 0 {
			continue
		}

		if err := in.Client.Write(batchPoints); err != nil {
			log.Errorln("Error writing points to influx db: ", err)
		}
	}

	return nil
}

func (in *influxDumper) getBatchPoints(con redis.Conn) (influx.BatchPoints, error) {
	batchPoints, err := influx.NewBatchPoints(influx.BatchPointsConfig{
		Database:  in.Database,
		Precision: "s",
	})
	if err != nil {
		log.Errorln("Error creating batchpoints: ", err)
		return nil, err
	}

	for len(batchPoints.Points()) < batch {
		reply, err := redis.ByteSlices(con.Do("BLPOP", queue_min, queue_hour, timeout))
		if err != nil {
			if err == redis.ErrNil {
				return batchPoints, nil
			}
			log.Errorln("Error reading from redis: ", err)
			return nil, err
		}

		queue := string(reply[0])
		var stats statistics
		if err := json.Unmarshal(reply[1], &stats); err != nil {
			log.Errorln("Error unmarshaling redis reply: ", err)
			continue
		}

		hostname, err := os.Hostname()
		if err != nil {
			log.Errorln("Error getting hostname: ", err)
			hostname = "N/A"
		}
		stats.Tags["node"] = hostname

		if queue == queue_min {
			if mPoint, err := newPoint(
				fmt.Sprintf("%v|m", stats.Key), stats.Start, stats.Avg, stats.Max, stats.Tags); err == nil {
				batchPoints.AddPoint(mPoint)
			}

			if tPoint, err := newPoint(
				fmt.Sprintf("%v|t", stats.Key), stats.Start, stats.Total, stats.Max, stats.Tags); err == nil {
				batchPoints.AddPoint(tPoint)
			}
		} else {
			if hPoint, err := newPoint(
				fmt.Sprintf("%v|h", stats.Key), stats.Start, stats.Avg, stats.Max, stats.Tags); err == nil {
				batchPoints.AddPoint(hPoint)
			}
		}

	}
	return batchPoints, nil
}

func (in *influxDumper) handleRequest(q influx.Query) (*influx.Response, error) {
	response, err := in.Client.Query(q)

	if err != nil {
		return nil, err
	}
	if response.Error() != nil {
		return nil, response.Error()
	}

	return response, nil
}

func newPoint(key string, timestamp int64, value float64, max float64, tags map[string]string) (*influx.Point, error) {
	fields := map[string]interface{}{
		"value": value,
		"max":   max,
	}

	point, err := influx.NewPoint(key, tags, fields, time.Unix(timestamp, 0))
	if err != nil {
		log.Errorln("Error creating new point: ", err)
		return nil, err
	}
	return point, nil
}

func parseToken(password string) (*jwt.Token, error) {
	pub, err := jwt.ParseECPublicKeyFromPEM([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("Failed to parse jwt public key: %v", err)
	}

	token, err := jwt.Parse(password, func(t *jwt.Token) (interface{}, error) {
		m, ok := t.Method.(*jwt.SigningMethodECDSA)
		if !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", t.Header["alg"])
		}
		if t.Header["alg"] != m.Alg() {
			return nil, fmt.Errorf("Unexpected signing algorithm: %v", t.Header["alg"])
		}
		return pub, nil
	})

	return token, err
}

func refreshToken(password string) (string, error) {
	client := &http.Client{}
	req, err := http.NewRequest("POST", refresh_url, nil)
	req.Header.Add("Authorization", fmt.Sprintf("bearer %s", password))
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("Error refreshing jwt token: %v", err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("Error reading response body of refresh jwt token: %v", err)
	}
	return string(body), nil
}

func getToken(password string) (string, error) {
	token, err := parseToken(password)
	if token == nil && err != nil {
		return "", fmt.Errorf("Failed to parse jwt token: %v", err)
	}

	if !token.Valid {
		if error_type, ok := err.(*jwt.ValidationError); ok {
			if error_type.Errors&(jwt.ValidationErrorExpired) != 0 {
				password, err = refreshToken(password)
				if err != nil {
					return "", err
				}
			}
		} else {
			return "", fmt.Errorf("Invalid jwt token: %v", err)
		}
	}
	return password, nil
}

func newPool(address, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     5,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			// the redis protocol should probably be made sett-able
			c, err := redis.Dial("tcp", address, redis.DialNetDial(func(network, address string) (net.Conn, error) {
				return tls.Dial(network, address, &tls.Config{
					InsecureSkipVerify: true,
				})
			}))

			if err != nil {
				return nil, err
			}

			if len(password) > 0 {
				password, err := getToken(password)
				if err != nil {
					return nil, err
				}
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			} else {
				// check with PING
				if _, err := c.Do("PING"); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		// custom connection test method
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if _, err := c.Do("PING"); err != nil {
				return err
			}
			return nil
		},
	}
}
