# nmqtt_broker - WIP
MQTT broker based upon the Nim library [nmqtt](https://github.com/zevv/nmqtt).

Not used code from [nmqtt](https://github.com/zevv/nmqtt) is just commented out.
What to do with it is unclear.


# Run

Currently it spins the broker at `127.0.0.1:1883`.

```nim
nim c -d:release --gc:arc -r src/nmqtt_broker

# Use `-d:verbose` for connection info
# Use `-d:dev` for packet info
```

# TODO

## table changed while iterating

Thousands of parallel connections - subscribe and publish - causes a problem....

`Error: unhandled exception: tables.nim(667, 13) len(t) == L the length of the table changed while iterating over it [AssertionError]`


## MQTT specifications

There's a lot of things, but these are first priority.

* Disconnect client after 1,5 time the `keepAlive` time
* Retain
* Wildcards in topics: `+`
* Clean Session


## Configuration file/CLI-options

A configuration file and CLI-options.


## Password check

An implementation for a hash and salt passwords like Mosquitto.


## msgId in PingResp

`PingResp` is added to the `workQueue`, which requires the packet to be assigned
with a `msgId`. We are using a _fake_ `msgId`...
Currently using the next `msgId + 1000`. This works, but if the same client
sends 1000's packets we could hit a break. To prevent this we are using
`hasKey()`, which needs to be removed. Another solution would be to assign
`0`, but what if we encounter 1000's of `Publish` and `PingReq` at the same
time?

```nim
while ctx.workQueue.hasKey(msgId):
  msgId += 1000
```


## await or asyncCheck

```nim
await ctx.handle(pkt)
#asyncCheck ctx.handle(pkt)
```


# Performance

These benchmarks are made on a machine, which was running multiple other programs
and using the internet during the test. They are not valid results, and therefor
just used in my development process.

The benchmark was done with [mqtt-bench](https://github.com/takanorig/mqtt-bench).
It should be noted that `nmqtt_broker` is faster than  `mosquitto` to receive
the `Publish`, but **not** at handling them.


| broker       | packetsize | clients | totalcount | duration | throughput       |
|--------------|------------|---------|------------|----------|------------------|
| nmqtt_broker | 4096       | 25      | 25000      | 1145 ms  | 21834.06 msg/sec |
| mosquitto    | 4096       | 25      | 25000      | 1341 ms  | 18642.80 msg/sec |
| nmqtt_broker | 1024       | 25      | 25000      | 590 ms   | 42372.88 msg/sec |
| mosquitto    | 1024       | 25      | 25000      | 763 ms   | 32765.40 msg/sec |


## Using `-size 4096`
### nmqtt_broker -d:release --gc:arc
```shell
$ ./mqtt-bench -action=p -broker="tcp://127.0.0.1:1883" -count 1000 -clients 25 -size 4096
2020-03-31 15:00:13.006323864 +0200 CEST Start benchmark
2020-03-31 15:00:14.152084235 +0200 CEST End benchmark

Result : broker=tcp://127.0.0.1:1883, clients=25, totalCount=25000, duration=1145ms, throughput=21834.06messages/sec
```

### mosquitto
```shell
$ ./mqtt-bench -action=p -broker="tcp://127.0.0.1:1883" -count 1000 -clients 25 -size 4096
2020-03-31 14:56:37.266341509 +0200 CEST Start benchmark
2020-03-31 14:56:38.608299394 +0200 CEST End benchmark

Result : broker=tcp://127.0.0.1:1883, clients=25, totalCount=25000, duration=1341ms, throughput=18642.80messages/sec
```

## Using `-size 1024`

### nmqtt_broker -d:release --gc:arc

```shell
$ ./mqtt-bench -action=p -broker="tcp://127.0.0.1:1883" -count 1000 -clients 25
2020-03-31 15:12:06.787402556 +0200 CEST Start benchmark
2020-03-31 15:12:07.378664141 +0200 CEST End benchmark

Result : broker=tcp://127.0.0.1:1883, clients=25, totalCount=25000, duration=590ms, throughput=42372.88messages/sec
```

### mosquitto
```shell
$ ./mqtt-bench -action=p -broker="tcp://127.0.0.1:1883" -count 1000 -clients 25
2020-03-31 15:12:24.954185289 +0200 CEST Start benchmark
2020-03-31 15:12:25.717899335 +0200 CEST End benchmark

Result : broker=tcp://127.0.0.1:1883, clients=25, totalCount=25000, duration=763ms, throughput=32765.40messages/sec
```