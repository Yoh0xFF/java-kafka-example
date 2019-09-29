# Java Kafka Example
Sample project demonstrating Kafka usage with Java

- **kafka-basics** - demonstrates basic producer and consumer usage
- **kafka-producer-twitter** - demonstrates the producer which reads and produces twitter tweets
- **kafka-consumer-elasticsearch** - demonstrates the consumer which reads and stores twitters tweets in elastic search
- **kafka-streams-filter-tweets** - demonstrates kafka streams which filter twitter tweets

## CLI Commands
This section describes frequently used Kafka CLI commands.

### Start Kafka
```shell script
# Start ZooKeeper server
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka server, note that ZooKeeper server must be running
bin/kafka-server-start.sh config/server.properties
```

### Kafka Topics CLI
```shell script
# Create a Kafka topic
bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic demo-topic --create --partitions 3 --replication-factor 1

# List all Kafka topics
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Show the description of the Kafka topic
bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic demo-topic --describe

# Delete the Kafka topic
bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic demo-topic --delete
```

### Kafka Console Producer CLI
```shell script
# Start console producer
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic demo-topic

# Start console producer with additional properties
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic demo-topic --producer-property acks=all

# Start console producer with keys
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic demo-topic --property parse.key=true --property key.separator=,
```

### Kafka Console Consumer CLI
```shell script
# Start console consumer
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo-topic

# Start console consumer and read from the beginning
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo-topic --from-beginning

# Start console consumer with keys
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo-topic --from-beginning --property print.key=true --property key.separator=,

# Start console consumer as a part of the group
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo-topic --group demo-consumer-group
```

### Kafka Consumer Groups CLI
```shell script
# List all consumer groups
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Show the description of the Kafka consumer group
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group demo-consumer-group

# Reset offsets of the consumer group
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group demo-consumer-group --reset-offsets --to-earliest --execute --topic demo-topic

# Shift offsets of the consumer group
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group demo-consumer-group --reset-offsets --shift-by -2 --execute --topic demo-topic
```
