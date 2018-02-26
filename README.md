# Medium Blog Kafka Udemy
This is the source code supporting the blog post:

<https://medium.com/@stephane.maarek/how-to-use-apache-kafka-to-transform-a-batch-pipeline-into-a-real-time-one-831b48a6ad85>

# Building 

All the instructions are in [run.sh](run.sh)
```bash
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
```

# Running
All the instructions are in [run.sh](run.sh)
Sample instructions:
```bash
export COURSE_ID=1075642  # Kafka for Beginners Course
java -jar udemy-reviews-producer/target/uber-udemy-reviews-producer-1.0-SNAPSHOT.jar
```

# Video Tutorial:
[![Udemy Kafka End To End Video (medium blog)](https://img.youtube.com/vi/h5i94umfzMM/0.jpg)](https://www.youtube.com/watch?v=h5i94umfzMM)

# Learning Kafka

If you want to explore all that Kafka has to offer, you can learn Kafka with my Udemy courses:
- [Kafka for Beginners](https://goo.gl/Ri5t7D)
- [Kafka Connect](https://goo.gl/YXLpoU)
- [Kafka Streams](https://goo.gl/5jgwxQ)
- [Kafka Setup and Administration](https://goo.gl/nkS6yE)
- [Confluent Schema Registry & REST Proxy](https://goo.gl/52Dv7d)
- [Kafka Security](https://goo.gl/VF6QWT)
