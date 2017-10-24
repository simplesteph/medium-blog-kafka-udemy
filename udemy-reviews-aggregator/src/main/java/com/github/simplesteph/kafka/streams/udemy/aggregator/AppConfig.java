package com.github.simplesteph.kafka.streams.udemy.aggregator;

import com.typesafe.config.Config;

public class AppConfig {

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String validTopicName;
    private final String recentStatsTopicName;
    private final String longTermStatsStatsTopicName;
    private final String applicationId;

    public AppConfig(Config config) {
        this.bootstrapServers = config.getString("kafka.bootstrap.servers");
        this.schemaRegistryUrl = config.getString("kafka.schema.registry.url");
        this.validTopicName = config.getString("kafka.valid.topic.name");
        this.applicationId = config.getString("kafka.streams.application.id");
        this.recentStatsTopicName = config.getString("kafka.recent.stats.topic.name");
        this.longTermStatsStatsTopicName = config.getString("kafka.long.term.stats.topic.name");
    }


    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public String getValidTopicName() {
        return validTopicName;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public String getRecentStatsTopicName() {
        return recentStatsTopicName;
    }

    public String getLongTermStatsStatsTopicName() {
        return longTermStatsStatsTopicName;
    }


}
