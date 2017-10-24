#!/bin/bash

### SETUP

# Download Confluent Platform 3.3.0 https://www.confluent.io/download/
# Unzip and add confluent-3.3.0/bin to your PATH

# Download and install Docker for Mac / Windows / Linux and do
docker-compose up -d
# Alternatively start postgres manually on your laptop at port 5432 and username/password = postgres/postgres

# Start the Confluent platform using the Confluent CLI
confluent start

# Create all the topics we're going to use for this demo
kafka-topics --create --topic udemy-reviews --partitions 3 --replication-factor 1 --zookeeper localhost:2181
kafka-topics --create --topic udemy-reviews-valid --partitions 3 --replication-factor 1 --zookeeper localhost:2181
kafka-topics --create --topic udemy-reviews-fraud --partitions 3 --replication-factor 1 --zookeeper localhost:2181
kafka-topics --create --topic long-term-stats --partitions 3 --replication-factor 1 --zookeeper localhost:2181
kafka-topics --create --topic recent-stats --partitions 3 --replication-factor 1 --zookeeper localhost:2181

# Build and package the different project components (make sure you have maven installed)
mvn clean package

### PLAYING

## Step 1: Review Producer

# Start an avro consumer on our reviews topic
kafka-avro-console-consumer --topic udemy-reviews --bootstrap-server localhost:9092

# And launch our first producer in another terminal !
export COURSE_ID=1075642  # Kafka for Beginners Course
java -jar udemy-reviews-producer/target/uber-udemy-reviews-producer-1.0-SNAPSHOT.jar
# This pulls overs 1000 reviews with some intentional delay of 50 ms between each send so you can see it stream in your consumer


## Step 2: Kafka Streams - Fraud Detector

# New terminal: Start an avro consumer on our valid reviews topic
kafka-avro-console-consumer --topic udemy-reviews-valid --bootstrap-server localhost:9092
# New terminal: Start an avro consumer on our fraud reviews topic
kafka-avro-console-consumer --topic udemy-reviews-fraud --bootstrap-server localhost:9092

# And launch our fraud Kafka Streams application in another terminal !
java -jar udemy-reviews-fraud/target/uber-udemy-reviews-fraud-1.0-SNAPSHOT.jar
# Keep it running.
# The execution is very quick!

# You'll see that ~50 messages (5%) were conducted to the fraud topic, the rest to the valid topic


## Step 3: Kafka Streams - Reviews Aggregator

# New terminal: Start an avro consumer on our recent stats topic
kafka-avro-console-consumer --topic recent-stats --bootstrap-server localhost:9092

# New terminal: Start an avro consumer on our long term stats topic
kafka-avro-console-consumer --topic long-term-stats --bootstrap-server localhost:9092

# Launch our review aggregator app
java -jar udemy-reviews-aggregator/target/uber-udemy-reviews-aggregator-1.0-SNAPSHOT.jar

# as we can see the recent topic only has the reviews for the past 90 days, while the long term has them all


## Step 4: Kafka Connect

# Load the recent and long term stats into Postgres using Kafka Connect Sink!
confluent load SinkTopics -d kafka-connectors/SinkTopicsInPostgres.properties

## Step 5: Play some more

# Make sure the four components are running (you can shut down the consumers)
# and fire off more producers
export COURSE_ID=1141696  # Kafka Connect Course
java -jar udemy-reviews-producer/target/uber-udemy-reviews-producer-1.0-SNAPSHOT.jar

export COURSE_ID=1141702  # Kafka Setup and Administration Course
java -jar udemy-reviews-producer/target/uber-udemy-reviews-producer-1.0-SNAPSHOT.jar

export COURSE_ID=1294188  # Kafka Streams Course
java -jar udemy-reviews-producer/target/uber-udemy-reviews-producer-1.0-SNAPSHOT.jar


## Step 6: Clean up
docker-compose down
confluent destroy