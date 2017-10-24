package com.github.simplesteph.kafka.producer.udemy.runnable;

import com.github.simplesteph.avro.udemy.Review;
import com.github.simplesteph.kafka.producer.udemy.AppConfig;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

public class ReviewsAvroProducerThread implements Runnable {

    private Logger log = LoggerFactory.getLogger(ReviewsAvroProducerThread.class.getSimpleName());

    private final AppConfig appConfig;
    private final ArrayBlockingQueue<Review> reviewsQueue;
    private final CountDownLatch latch;
    private final KafkaProducer<Long, Review> kafkaProducer;
    private final String targetTopic;

    public ReviewsAvroProducerThread(AppConfig appConfig, ArrayBlockingQueue<Review> reviewsQueue, CountDownLatch latch) {
        this.appConfig = appConfig;
        this.reviewsQueue = reviewsQueue;
        this.latch = latch;
        this.kafkaProducer = createKafkaProducer(appConfig);
        this.targetTopic = appConfig.getTopicName();
    }

    public KafkaProducer<Long, Review> createKafkaProducer(AppConfig appConfig) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl());
        return new KafkaProducer<>(properties);
    }

    @Override
    public void run() {
        int reviewCount = 0;
        try {
            while (latch.getCount() > 1 || reviewsQueue.size() > 0){
                Review review = reviewsQueue.poll();
                if (review == null) {
                    Thread.sleep(200);
                } else {
                    reviewCount += 1;
                    log.info("Sending review " + reviewCount + ": " + review);
                    kafkaProducer.send(new ProducerRecord<>(targetTopic, review));
                    // sleeping to slow down the pace a bit
                    Thread.sleep(appConfig.getProducerFrequencyMs());
                }
            }
        } catch (InterruptedException e) {
            log.warn("Avro Producer interrupted");
        } finally {
            close();
        }
    }

    public void close() {
        log.info("Closing Producer");
        kafkaProducer.close();
        latch.countDown();
    }
}
