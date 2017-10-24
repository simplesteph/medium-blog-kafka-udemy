package com.github.simplesteph.kafka.streams.udemy.aggregator;

import com.github.simplesteph.avro.udemy.Review;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class ReviewTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        long timestamp = -1;
        final Review review = (Review) record.value();
        if (review != null) {
            timestamp = review.getCreated().getMillis();
        }
        if (timestamp < 0) {
            // Invalid timestamp!  Attempt to estimate a new timestamp,
            // otherwise fall back to wall-clock time (processing-time).
            if (previousTimestamp >= 0) {
                return previousTimestamp;
            } else {
                return System.currentTimeMillis();
            }
        } else {
            return timestamp;
        }
    }
}
