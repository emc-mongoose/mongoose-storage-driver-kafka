# Introduction

Kafka storage driver for Mongoose

# Features
+ Item types:
  * `data item` - Record (ProducerRecord, ConsumerRecord)
  * `path` - Topic
+ Data item operation types:
  * `create`
  * `read`
+ Path item operation types:
    * `create`
    * `read`
    * `delete`
    * `list`
`
# Design

| Kafka | Mongoose |
|---------|----------|
| Record | *Data Item* |
| Topic | *Path Item* |
| Partition | N/A |
## Record Operations

Mongoose should perform the load operations on the *records* when the configuration option `item-type` is set to `data`.

### Create
`ProducerApi` has a `KafkaProducer` class with function  [send()](http://kafka.apache.org/21/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#send-org.apache.kafka.clients.producer.ProducerRecord-org.apache.kafka.clients.producer.Callback-), which can send a record to topic.
* Steps:

### Read
`ConsumerApi` has a `KafkaConsumer` class, provided with function [poll()](https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/consumer/Consumer.html#poll-long-). According to Kafka documentation, on each poll Consumer  begins to consume records from last offset.
* Steps:

### Update
Not supported. 

### Delete
[deleteRecords()](http://kafka.apache.org/21/javadoc/org/apache/kafka/clients/admin/AdminClient.html#deleteRecords-java.util.Map-org.apache.kafka.clients.admin.DeleteRecordsOptions-) function from AdminClient(AdminClient API) class, deletes all records before the one with giving offset.

### List
Not supported.

## Topic Operations

Mongoose should perform the load operations on the *topic* when the configuration option `item-type` is set to `path`.
Apache Kafka has `AdminClient Api`, which provides function for managing and inspecting topics. 
### Create
[createTopics()](http://kafka.apache.org/21/javadoc/org/apache/kafka/clients/admin/AdminClient.html#createTopics-java.util.Collection-) creates a batch of new topics.
* Steps:

### Read
Read all records at once.
### Update
Not supported

### Delete
[deleteTopics()](http://kafka.apache.org/21/javadoc/org/apache/kafka/clients/admin/AdminClient.html#deleteTopics-java.util.Collection-) deletes a batch of topics.
* Steps:

### List
[listTopics()](http://kafka.apache.org/21/javadoc/org/apache/kafka/clients/admin/AdminClient.html#listTopics--) returns list of topics
* Steps:

## Specific Configuration Options

| Name | Type | Default Value | Description |
|---------|----------|----------|----------|
| load-op-timeoutMillis | integer | 300000 | The event read and create timeout in milliseconds |
| storage-driver-create-key-enabled | boolean | false | Creates a record with or without a key |
| storage-net-sndBuf | integer | 131072 | The size of the TCP send buffer to use when sending data. If the value is -1, the OS default will be used. |
| storage-net-rcvBuf | integer | 32768 | The size of the TCP receive buffer to use when reading data. If the value is -1, the OS default will be used. |
| storage-driver-request-size | integer | 1048576 | The maximum size of a request in bytes. This setting will limit the number of record batches the producer will send in a single request to avoid sending huge requests. |
| storage-driver-batch-size | integer | 16384 | Allows you to change the batch size |
| storage-net-linger | integer | 0 | The delay before sending the records. This setting gives the upper bound on the delay for batching: once we get *batch.size* worth of records for a partition it will be sent immediately regardless of this setting, however if we have fewer than this many bytes accumulated for this partition we will 'linger' for the specified time waiting for more records to show up. |
| storage-driver-buffer-memory | long | 33554432 | The total bytes of memory the producer can use to buffer records waiting to be sent to the server. |
| storage-driver-compression-type | string | none | The compression type for all data generated by the producer. The default is none (i.e. no compression). Valid values are none, gzip, snappy, lz4, or zstd.  |
| storage-net-node-addrs | list | "" | A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.  This list should be in the form *host1:port1*,*host2:port2* |
| storage-net-node-port | integer | 9092 | The common port number to access the storage nodes, may be overriden adding the port number to the storage-driver-addrs, for example: "127.0.0.1:9020,127.0.0.1:9022,..." |

## Custom Kafka Headers

Scenario example:

```javascript
var customKafkaHeadersConfig = {
    "storage" : {
        "driver" : {
            "create" : {
                "headers" : {
                    "header-name-0" : "header_value_0",
                    "header-name-1" : "header_value_1",
                    // ...
                    "header-name-N" : "header_value_N"
                }
            }
        }
    }
};
Load
    .config(customKafkaHeadersConfig)
    .run();
```

**Note**:
> Don't use the command line arguments for the custom Kafka headers setting.
### Expressions

Scenario example, note both the parameterized header name and value:
```javascript
var varKafkaHeadersConfig = {
    "storage" : {
        "driver" : {
            "create" : {
                "headers" : {
                    "x-amz-meta-${math:random(30) + 1}" : "${date:format("yyyy-MM-dd'T'HH:mm:ssZ").format(date:from(rnd.nextLong(time:millisSinceEpoch())))}"
                }
            }
        }
    }
};
Load
    .config(varKafkaHeadersConfig)
    .run();
```
**Notes:**
> For reading: num.consumer.fetchers	- the number fetcher threads used to fetch data, default value = 1.

> For recording: KafkaProducer contains thread, the number of threads is equal to the number of producers.

**Note about KAFKA benchmark**
Command line example:
```
./bin/kafka-run-class.sh \
org.apache.kafka.tools.ProducerPerformance --throughput=-1 \
--topic=test-one \
--num-records=50000000 \
--record-size=100 \
--producer-props bootstrap.servers=localhost:9092 \
buffer.memory=67108864 \
batch.size=8196
```
Result:
```
50000000 records sent, 68460.889663 records/sec (6.53 MB/sec), 8772.19 ms avg latency, 29552.00 ms max latency, 8072 ms 50th, 16228 ms 95th, 26685 ms 99th, 28510 ms 99.9th.
```
Computer configuration:
+ OS - Ubuntu 18.04.2 LTS
+ Memory - 3.8 GiB
+ Processor - Intel® Core™ i5-6200U CPU @ 2.30GHz × 4 
+ OS type - 64-bit


