package influxdumper

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/garyburd/redigo/redis"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/zero-os/0-core/client/go-client"
)

type Stats struct {
	Avg   float64
	Total float64
	Max   float64
	Count int64
	Start int64
	Key   string
	Tags  string
}

type InfluxDumper struct {
	Client    influx.Client
	Node      string
	JWT       string
	IP        string
	Port      int64
	Database  string
	Retention string
}

const (
	batch      = 1000
	queue_min  = "statistics:300"
	queue_hour = "statistics:3600"
	policy     = "dumper"
)

func (in InfluxDumper) Start() error {
	q := influx.NewQuery("SHOW DATABASES", "", "")
	response, err := in.handleRequest(q)
	if err != nil {
		log.Errorln("Err listing databases")
		return err
	}

	exists := false
	for _, db := range response.Results[0].Series[0].Values {
		if db[0] == in.Database {
			exists = true
			break
		}
	}

	if !exists {
		q := influx.NewQuery(fmt.Sprintf("CREATE DATABASE %v", in.Database), "", "")
		_, err := in.handleRequest(q)
		if err != nil {
			log.Errorln("Error creating database")
			return err
		}
	}

	q = influx.NewQuery(fmt.Sprintf("CREATE RETENTION POLICY \"%v\" ON %v DURATION %v REPLICATION 1", policy, in.Database, in.Retention), "", "")
	response, err = in.handleRequest(q)
	if err != nil {
		log.Errorln("Err listing databases")
		return err
	}

	return in.Dump()
}

func (in InfluxDumper) Dump() error {
	cl := client.NewPool(in.Node, in.JWT)
	con := cl.Get()
	defer con.Close()

	for {
		batchPoints, err := influx.NewBatchPoints(influx.BatchPointsConfig{Database: in.Database, Precision: "s"})
		if err != nil {
			log.Errorln("Error creating batchpoints")
			return err
		}
		batchPoints.SetRetentionPolicy(policy)

		for len(batchPoints.Points()) < batch {
			reply, err := redis.ByteSlices(con.Do("BLPOP", queue_min, queue_hour, 0))
			if err != nil {
				log.Errorln("Error reading from redis")
				return err
			}

			queue := string(reply[0])
			var stats Stats
			if err := json.Unmarshal(reply[1], &stats); err != nil {
				log.Errorln("Error unmarshaling redis reply")
				return err
			}

			tags := []string{}
			if stats.Tags != "" {
				tags = strings.Split(stats.Tags, " ")
			}

			tagsMap := make(map[string]string)
			for _, tag := range tags {
				result := strings.Split(tag, "=")
				tagsMap[result[0]] = result[1]
			}

			hostname, err := os.Hostname()
			if err != nil {
				log.Errorln("Error getting hostname")
				return err
			}
			tagsMap["node"] = hostname

			keys := strings.Split(stats.Key, "@")
			stats.Key = keys[0]
			fmt.Println(stats)

			if queue == queue_min {
				if point, err := NewPoint(
					fmt.Sprintf("%v|m", stats.Key), stats.Start, stats.Avg, stats.Max, tagsMap); err != nil {
					log.Errorln(err)
				} else {
					batchPoints.AddPoint(point)
				}

				if point, err := NewPoint(
					fmt.Sprintf("%v|t", stats.Key), stats.Start, stats.Total, stats.Max, tagsMap); err != nil {
					log.Errorln(err)
				} else {
					batchPoints.AddPoint(point)
				}
			} else {
				if point, err := NewPoint(
					fmt.Sprintf("%v|h", stats.Key), stats.Start, stats.Avg, stats.Max, tagsMap); err != nil {
					log.Errorln(err)
				} else {
					batchPoints.AddPoint(point)
				}
			}
		}


		if err := in.Client.Write(batchPoints); err != nil {
			log.Errorln("Error writing points to influx db")
		}
	}

	return nil
}
func (in InfluxDumper) handleRequest(q influx.Query) (*influx.Response, error) {
	response, err := in.Client.Query(q)

	if err != nil {
		return nil, err
	}
	if response.Error() != nil {
		return nil, response.Error()
	}

	return response, nil
}


func NewPoint(key string, timestamp int64, value float64, max float64, tags map[string]string) (*influx.Point, error) {
	fields := map[string]interface{}{
		"value": value,
		"max":   max,
	}

	point, err := influx.NewPoint(key, tags, fields, time.Unix(timestamp, 0))
	if err != nil {
		log.Errorln("Error creating new point")
		return nil, err
	}
	return point, nil
}
