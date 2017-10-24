package com.github.simplesteph.kafka.streams.udemy.aggregator;

import com.github.simplesteph.avro.udemy.CourseStatistic;
import com.github.simplesteph.avro.udemy.Review;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class AggregatorMain {

    private Logger log = LoggerFactory.getLogger(AggregatorMain.class.getSimpleName());
    private AppConfig appConfig;

    public static void main(String[] args) {
        AggregatorMain aggregatorMain = new AggregatorMain();
        aggregatorMain.start();
    }

    private AggregatorMain() {
        appConfig = new AppConfig(ConfigFactory.load());
    }

    private void start() {
        Properties config = getKafkaStreamsConfig();
        KafkaStreams streams = createTopology(config);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private Properties getKafkaStreamsConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appConfig.getApplicationId());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        // Exactly once processing!!
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl());

        return config;
    }

    private KafkaStreams createTopology(Properties config) {

        // define a few serdes that will be useful to us later
        SpecificAvroSerde<Review> reviewSpecificAvroSerde = new SpecificAvroSerde<>();
        reviewSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);
        SpecificAvroSerde<CourseStatistic> courseStatisticSpecificAvroSerde = new SpecificAvroSerde<>();
        courseStatisticSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);
        Serdes.LongSerde longSerde = new Serdes.LongSerde();
        Serdes.StringSerde stringSerde = new Serdes.StringSerde();


        KStreamBuilder builder = new KStreamBuilder();


        // we build our stream with a timestamp extractor
        KStream<String, Review> validReviews = builder.stream( new ReviewTimestampExtractor(), longSerde, reviewSpecificAvroSerde,  appConfig.getValidTopicName())
                .selectKey(((key, review) -> review.getCourse().getId().toString()));

        // we build a long term topology (since inception)
        KTable<String, CourseStatistic> longTermCourseStats =
                validReviews.groupByKey().aggregate(
                        this::emptyStats,
                        this::reviewAggregator,
                        courseStatisticSpecificAvroSerde
                );
        longTermCourseStats.toStream().to(stringSerde, courseStatisticSpecificAvroSerde, appConfig.getLongTermStatsStatsTopicName());

        // we build a 90 days average

        // A hopping time window with a size of 91 days and an advance interval of 1 day.
        // the windows are aligned with epoch
        long windowSizeMs = TimeUnit.DAYS.toMillis(91);
        long advanceMs = TimeUnit.DAYS.toMillis(1);
        TimeWindows timeWindows = TimeWindows.of(windowSizeMs).advanceBy(advanceMs);

        KTable<Windowed<String>, CourseStatistic> windowedCourseStatisticKTable = validReviews
                .filter((k, review) -> !isReviewExpired(review, windowSizeMs)) //recent reviews
                .groupByKey().aggregate(
                        this::emptyStats,
                        this::reviewAggregator,
                        timeWindows,
                        courseStatisticSpecificAvroSerde
                );

        KStream<String, CourseStatistic> recentStats = windowedCourseStatisticKTable
                .toStream()
                // we keep the current window only
                .filter((window, courseStat) -> keepCurrentWindow(window, advanceMs))
                .peek(((key, value) -> log.info(value.toString())))
                .selectKey((k, v) -> k.key());

        recentStats.to(stringSerde, courseStatisticSpecificAvroSerde, appConfig.getRecentStatsTopicName());


        // for learning purposes: Using the lower level API (uncomment the code)
//        // Create a state store manually.
//        // It will contain only the most recent reviews
//        StateStoreSupplier recentReviewsStore = Stores.create("RecentReviewsStore")
//                .withKeys(Serdes.Long())
//                .withValues(reviewSpecificAvroSerde)
//                // back it up in RocksDB
//                .persistent()
//                .build();
//
//        // add the store to the topology so it can be referenced
//        builder.addStateStore(recentReviewsStore);
//
//        Long timeToKeepAReview = TimeUnit.DAYS.toMillis(90);
//        KStream<String, Review> recentReviews =
//                validReviews.transform(new RecentReviewsTransformerSupplier(timeToKeepAReview, recentReviewsStore.name()),
//                                        recentReviewsStore.name());
//
//
//        // we build a long term topology (since inception)
//        KTable<String, CourseStatistic> recentCourseStats = recentReviews.groupByKey().aggregate(
//                this::emptyStats,
//                this::reviewAggregator,
//                courseStatisticSpecificAvroSerde
//        );
//
//        recentCourseStats.toStream()
//                .peek(((key, value) -> log.info(value.toString())))
//                .to(stringSerde, courseStatisticSpecificAvroSerde, appConfig.getRecentStatsTopicName() + "-low-api");

        return new KafkaStreams(builder, config);
    }

    private boolean keepCurrentWindow(Windowed<String> window, long advanceMs) {
        long now = System.currentTimeMillis();
        return window.window().end() > now &&
                window.window().end() < now + advanceMs;
    }

    private Boolean isReviewExpired(Review review, Long maxTime) {
        return review.getCreated().getMillis() + maxTime < System.currentTimeMillis();
    }

    private CourseStatistic emptyStats() {
        return CourseStatistic.newBuilder().setLastReviewTime(new DateTime(0L)).build();
    }

    private CourseStatistic reviewAggregator(String courseId, Review newReview, CourseStatistic currentStats) {
        CourseStatistic.Builder courseStatisticBuilder = CourseStatistic.newBuilder(currentStats);
        courseStatisticBuilder.setCourseId(newReview.getCourse().getId());
        courseStatisticBuilder.setCourseTitle(newReview.getCourse().getTitle());
        String reviewRating = newReview.getRating().toString();
        // increase or decrease?
        Integer incOrDec = (reviewRating.contains("-")) ? -1 : 1;
        switch (reviewRating.replace("-", "")) {
            case "0.5":
                courseStatisticBuilder.setCountZeroStar(courseStatisticBuilder.getCountZeroStar() + incOrDec);
                break;
            case "1.0":
            case "1.5":
                courseStatisticBuilder.setCountOneStar(courseStatisticBuilder.getCountOneStar() + incOrDec);
                break;
            case "2.0":
            case "2.5":
                courseStatisticBuilder.setCountTwoStars(courseStatisticBuilder.getCountTwoStars() + incOrDec);
                break;
            case "3.0":
            case "3.5":
                courseStatisticBuilder.setCountThreeStars(courseStatisticBuilder.getCountThreeStars() + incOrDec);
                break;
            case "4.0":
            case "4.5":
                courseStatisticBuilder.setCountFourStars(courseStatisticBuilder.getCountFourStars() + incOrDec);
                break;
            case "5.0":
                courseStatisticBuilder.setCountFiveStars(courseStatisticBuilder.getCountFiveStars() + incOrDec);
                break;
        }
        Long newCount = courseStatisticBuilder.getCountReviews() + incOrDec;
        Double newSumRating = courseStatisticBuilder.getSumRating() + new Double(newReview.getRating().toString());
        Double newAverageRating = newSumRating / newCount;

        courseStatisticBuilder.setCountReviews(newCount);
        courseStatisticBuilder.setSumRating(newSumRating);
        courseStatisticBuilder.setAverageRating(newAverageRating);
        courseStatisticBuilder.setLastReviewTime(
                latest(courseStatisticBuilder.getLastReviewTime(), newReview.getCreated()));

        return courseStatisticBuilder.build();
    }

    private DateTime latest(DateTime a, DateTime b)
    {
        return a.isAfter(b) ? a : b;
    }


}
