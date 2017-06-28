package main

import (
	"os"

	"fmt"
	influx "github.com/influxdata/influxdb/client/v2"
	log "github.com/Sirupsen/logrus"

	"github.com/urfave/cli"
	"github.com/zero-os/dumper/influxdumper"
)

func main() {
	var (
		node string
		jwt string
		ip string
		port int64
		database string
		retention string
	)
	app := cli.NewApp()
	app.Version = "0.1.0"
	app.Name = "Statistics Dumper"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "node",
			Value: "192.168.122.89:6379",
			Destination: &node,
			Usage: "0core node to read statistics from",
		},
		cli.StringFlag{
			Name:  "jwt",
			Usage: "jwt token used to connect to 0core nodes",
			Destination: &jwt,
		},
		cli.StringFlag{
			Name:  "ip",
			Value: "127.0.0.1",
			Usage: "IP of the influx database",
			Destination: &ip,
		},
		cli.Int64Flag{
			Name:  "port",
			Value: 8086,
			Usage: "Port of the influx db",
			Destination: &port,
		},
		cli.StringFlag{
			Name:  "db",
			Value: "statistics",
			Usage: "Name of the influx db to be used",
			Destination: &database,
		},
		cli.StringFlag{
			Name:  "retention",
			Value: "5d",
			Usage: "Renetention duration",
			Destination: &retention,
		},
	}

	app.Action = func(c *cli.Context) {
		influxClient, err := influx.NewHTTPClient(influx.HTTPConfig{
			Addr: fmt.Sprintf("http://%v:%v", ip, port),
		})
		if err != nil {
			log.Errorln(err)
			return
		}

		ifl := influxdumper.InfluxDumper{
			Node: node,
			JWT: jwt,
			Client: influxClient,
			Database: database,
			Retention: retention,
		}

		log.Println("Starting Influx Dumper")
		if err := ifl.Start(); err != nil {
			log.Errorln(err)
		}
	}

	app.Run(os.Args)
}
