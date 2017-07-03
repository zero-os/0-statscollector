package main

import (
	log "github.com/Sirupsen/logrus"
	"os"

	"github.com/urfave/cli"
	"github.com/zero-os/0-statscollector/dumper"
)

func main() {
	app := cli.NewApp()
	app.Version = "0.1.0"
	app.Name = "Statistics Dumper"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "node",
			Value: "127.0.0.1:6379",
			Usage: "0core node to read statistics from",
		},
		cli.StringFlag{
			Name:  "jwt",
			Usage: "jwt token used to connect to 0core nodes",
		},
		cli.StringFlag{
			Name:  "ip",
			Value: "127.0.0.1",
			Usage: "IP of the influx database",
		},
		cli.Int64Flag{
			Name:  "port",
			Value: 8086,
			Usage: "Port of the influx db",
		},
		cli.StringFlag{
			Name:  "db",
			Value: "statistics",
			Usage: "Name of the influx db to be used",
		},
		cli.StringFlag{
			Name:  "retention",
			Value: "5d",
			Usage: "Renetention duration",
		},
	}

	app.Action = func(c *cli.Context) {
		influxDumper := dumper.NewInfluxDumper(
			c.String("node"), c.String("jwt"),
			c.String("ip"), c.Int64("port"),
			c.String("db"), c.String("retention"),
		)

		log.Println("Starting Influx Dumper")
		if err := influxDumper.Start(); err != nil {
			log.Errorln(err)
			os.Exit(1)
		}
	}

	app.Run(os.Args)
}
