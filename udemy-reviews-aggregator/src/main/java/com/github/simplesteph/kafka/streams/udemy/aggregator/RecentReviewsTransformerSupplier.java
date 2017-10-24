package com.github.simplesteph.kafka.streams.udemy.aggregator;

import com.github.simplesteph.avro.udemy.Review;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;

public class RecentReviewsTransformerSupplier implements TransformerSupplier<String, Review, KeyValue<String, Review>> {

    private Long timeToKeepAReview;
    private String stateStoreName;
    private RecentReviewsTransformer recentReviewsTransformer;

    public RecentReviewsTransformerSupplier(Long timeToKeepAReview, String stateStoreName){
        this.timeToKeepAReview = timeToKeepAReview;
        this.stateStoreName = stateStoreName;
        this.recentReviewsTransformer = new RecentReviewsTransformer(timeToKeepAReview, stateStoreName);
    }

    @Override
    public Transformer<String, Review, KeyValue<String, Review>> get() {
        return recentReviewsTransformer;
    }
}
