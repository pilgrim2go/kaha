# Build

## Dependencies
* librdkafa v0.11.1 (https://github.com/edenhill/librdkafka/tree/v0.11.1)
* golang 1.9 (tested on 1.9.2)

## Using Docker - binary build for Linux x64 (tested on Debian Jessie)
Run in repo main directory
```
docker build -t kaha .
docker run -v $PWD:/gowork/src/kaha --rm -it kaha go build
```
Binary file: "kaha" will be created in current directory

## Manual
* install librdkafka library
* run ```go build```

# Configuration

## Rename or copy example.kaha.toml to kaha.toml
```
[Kafka]
broker = "kafka:9092" # Broker address format: "host:port"
group = "test_group"
topics = ["example.topic_name", "example.topic_other"]
consumers = 2 # How many consumers to start (best performence = number of partitions)
auto_commit = true # true/false If true the consumer's offset will be periodically committed in the background
auto_commit_interval_ms = 5000 # How often offset will be commited If auto_commit is enabled 
auto_offset_reset = "earliest" #  Initial offset: latest, earliest, none 
session_timeout_ms = 6000 #  Timeout used to detect consumer failures 
batch_size = 10000 # Number of messages for bulk processing 

[Clickhouse]
node = "http://clickhouse:8123" # Node http adress format: http(s)://host:port
db_table = "db_name.table_name" 
timeout_seconds = 15 # Http response timeout
retry_attempts = 3 # How many times to try if operation fails
backoff_time_seconds = 1 # How many seconds to wait until next retry

[Process] # Simple Kafka json messages processing
remove_fields = ["field1","field2"] # Name of keys that should be removed e.g. ["field1","field2"] means -> {"field1": "value", "field2: "value", "field3": "value"} will be transformed to {"field3": value"}
[Process.FlatFields]
"parent.child" = "parent_child" # Name of keys that should be flatten/renamed e.g. "parent.child" means -> {"parent": {"child": "value"}} will be transformed to {"paren_child": "value}
```

# Run
## Default (reads "kaha.toml" file from current directory)
```
kaha
```
## Custom config file
```
kaha -config configfile.toml
```