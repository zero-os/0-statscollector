package dumper

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"os"
	"time"

	"github.com/garyburd/redigo/redis"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/zero-os/0-core/client/go-client"
)

const (
	batch      = 1000
	queue_min  = "statistics:300"
	queue_hour = "statistics:3600"
	policy     = "dumper"
	timeout    = 60
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
	cl := client.NewPool(in.Node, in.JWT)
	con := cl.Get()
	defer con.Close()

	for {
		batchPoints, err := in.getBatchPoints(con)
		if err != nil {
			return err
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
