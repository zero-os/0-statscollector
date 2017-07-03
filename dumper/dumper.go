package dumper

import (

)

type Dumper interface {
	Start() error
}

func NewInfluxDumper(node string, jwt string, ip string, port int64, db string, retention string) Dumper {
	return &influxDumper{
		Node:      node,
		JWT:       jwt,
		IP:        ip,
		Port:      port,
		Database:  db,
		Retention: retention,
	}
}
