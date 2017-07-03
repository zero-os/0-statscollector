# Zero-OS Stats Collector

The 0-statscollector collects statistics from a core0 node and dumps it into influxdb.

```bash
./0-statscollector --ip 127.0.0.1 --port 8086 --db statistics --node 127.0.0.1:6379 --retention 5d --jwt eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9
```

* `node`: the ip:port of the core0 node. Defaults to 127.0.0.1:6379.
* `ip`: The ip where the influxdb server is running. Defaults to 127.0.0.1.
* `port`: The port where the influxdb server is running. Defaults to 8086.
* `retention`: The retention policy of influxdb. Default to 5d.
* `db`: Name of the database where the stats should be dumped. Default to statistics.
* `jwt`: JWT token.

