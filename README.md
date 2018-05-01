# Build

## Dependencies
* librdkafa v0.11.1 (https://github.com/edenhill/librdkafka/tree/v0.11.1)
* golang 1.10

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
[[consumer]]
    name = "kafka"
    consumers = 2 How many consumers to start (best performence = number of partitions)
    [consumer.config]
    brokers = ["kafka1", "kafka2"]  # Brokers address format: "host:port"
    group = "group"
    topics = ["topic1", "topic2"]
    auto_commit = false true/false If true the consumer's offset will be periodically committed in the background by kafka, if false by kaha
    auto_commit_interval_ms = 5000 # How often offset will be commited by kafka If auto_commit is enabled
    auto_offset_reset = "earliest" # Initial offset: latest, earliest, none
    session_timeout_ms = 6000  # Timeout used to detect consumer failures
    batch_size = 10000 # Number of messages for bulk processing
    max_wait_seconds = 10 # How many seconds wait for batch
    [consumer.process]
    remove_fields = ["field1", "field2"] # Name of keys that should be removed e.g. ["field1","field2"] means -> {"field1": "value", "field2: "value", "field3": "value"} will be transformed to {"field3": value"}
    [consumer.process.rename_fields] #  Name of keys that should be flatten/renamed e.g. "parent.child" = "parent_child"  means -> {"parent": {"child": "value"}} will be transformed to {"paren_child": "value}
    "parent1.child2" = "parent1_child1"
    "parent2.child2" = "parent2_child2"
    [consumer.process.submatch_values] # Name of keys which values should be submatched e.g. "type" = ".{2}" means -> {"type": "12345"} will be transformed to {"type": "12"}
    "key" = "regexp"
    [consumer.producer]
    name = "clickhouse"
    [consumer.producer.config]
    node = "http://localhost:8123" # Node http adress format: http(s)://host:port
    db_table = "db.table"
    timeout_seconds = 15 # Http response timeout
    retry_attempts = 3 # How many times to try if operation fails
    backoff_time_seconds = 1 # How many seconds to wait until next retry
    conn_limit = 2 # How many open connection can be established
[[consumer]]
    name = "stdin"
    consumers = 1
    [consumer.config]
    batch_size = 10000
    [consumer.process]
    remove_fields = ["field1", "field2"]
    [consumer.process.rename_fields]
    "parent1.child2" = "parent1_child1"
    "parent2.child2" = "parent2_child2"
    [consumer.process.submatch_values]
    "key" = "regexp"
    [consumer.producer]
    name = "clickhouse"
    [consumer.producer.config]
    node = "http://localhost:8123"
    db_table = "db.table"
    timeout_seconds = 15
    retry_attempts = 3
    backoff_time_seconds = 1
    conn_limit = 2
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
## Debug - more verbose logging
```
kaha -debug
```
