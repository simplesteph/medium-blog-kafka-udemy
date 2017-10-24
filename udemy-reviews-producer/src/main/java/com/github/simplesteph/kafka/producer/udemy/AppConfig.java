package com.github.simplesteph.kafka.producer.udemy;

import com.typesafe.config.Config;

public class AppConfig {

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String topicName;
    private final Integer queueCapacity;
    private final Integer producerFrequencyMs;
    private final Integer udemyPageSize;
    private final String courseId;

    public AppConfig(Config config) {
        this.bootstrapServers = config.getString("kafka.bootstrap.servers");
        this.schemaRegistryUrl = config.getString("kafka.schema.registry.url");
        this.topicName = config.getString("kafka.topic.name");
        this.queueCapacity = config.getInt("app.queue.capacity");
        this.producerFrequencyMs = config.getInt("app.producer.frequency.ms");
        this.udemyPageSize = config.getInt("app.udemy.page.size");
        this.courseId = Long.toString(config.getLong("app.course.id"));
    }


    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public String getTopicName() {
        return topicName;
    }

    public Integer getQueueCapacity() {
        return queueCapacity;
    }

    public Integer getProducerFrequencyMs() {
        return producerFrequencyMs;
    }

    public Integer getUdemyPageSize() {
        return udemyPageSize;
    }

    public String getCourseId() {
        return courseId;
    }


}
