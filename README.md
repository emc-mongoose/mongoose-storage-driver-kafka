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
[createTopics()](http://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/admin/AdminClient.html) creates a batch of new topics.
* Steps:

### Read
Read all records at once.
### Update
Not supported

### Delete
[deleteTopics()](http://kafka.apache.org/21/javadoc/org/apache/kafka/clients/admin/AdminClient.html#deleteRecords-java.util.Map-) deletes a batch of topics.
* Steps:

### List
[listTopics()](http://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/admin/AdminClient.html) returns list of topics
* Steps:

