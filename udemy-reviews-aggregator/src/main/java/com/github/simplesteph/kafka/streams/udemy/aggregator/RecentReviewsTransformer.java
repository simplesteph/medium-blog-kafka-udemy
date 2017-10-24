package com.github.simplesteph.kafka.streams.udemy.aggregator;

import com.github.simplesteph.avro.udemy.Review;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RecentReviewsTransformer implements Transformer<String, Review, KeyValue<String, Review>> {

    private Logger log = LoggerFactory.getLogger(RecentReviewsTransformer.class.getSimpleName());

    private ProcessorContext context;
    private KeyValueStore<Long, Review> reviewStore;

    private Long timeToKeepAReview;
    private String stateStoreName;

    private Long minTimestampInStore = -1L;

    public RecentReviewsTransformer(Long timeToKeepAReview, String stateStoreName){
        this.timeToKeepAReview = timeToKeepAReview;
        this.stateStoreName = stateStoreName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        // keep the processor context locally because we need it in punctuate() and commit()
        this.context = context;

        // call this processor's punctuate() method every 10 minutes to clean up for old data
        this.context.schedule(TimeUnit.MINUTES.toMillis(10));

        // retrieve the key-value store named "Counts"
        reviewStore = (KeyValueStore<Long, Review>) this.context.getStateStore(stateStoreName);
    }

    // we push new reviews as long as we haven't seen them yet, and that they're not too old.
    @Override
    public KeyValue<String, Review> transform(String courseId, Review review) {
        Long reviewId = review.getId();
        Long now = System.currentTimeMillis();
        if (reviewStore.get(reviewId) == null && !isReviewExpired(review, now, timeToKeepAReview)) {
            reviewStore.put(review.getId(), review);
            updateMinTimestamp(review);
            return KeyValue.pair(courseId, review);
        } else {
            return null;
        }
    }

    private void updateMinTimestamp(Review review) {
        minTimestampInStore = Math.min(minTimestampInStore, review.getCreated().getMillis());
    }

    // every punctuate we expire old reviews
    @Override
    public KeyValue<String, Review> punctuate(long currentTime) {
        if (minTimestampInStore + timeToKeepAReview < currentTime && reviewStore.approximateNumEntries() > 0) {
            log.info("let's expire data!");
            // invalidate the min timestamp in store as we're going to re-compute it
            minTimestampInStore = System.currentTimeMillis();
            KeyValueIterator<Long, Review> it = reviewStore.all();
            List<Long> keysToRemove = new ArrayList<>();
            while (it.hasNext()) {
                KeyValue<Long, Review> next = it.next();
                Review review = next.value;
                Long courseId = review.getCourse().getId();
                if (isReviewExpired(review, currentTime, timeToKeepAReview)) {
                    Long reviewId = next.key;
                    keysToRemove.add(reviewId);
                    // we push an opposite review event to remove data from the average
                    Review reverseReview = reverseReview(review);
                    this.context.forward(courseId, reverseReview);
                } else {
                    // update the min timestamp in store as we know the data is staying
                    updateMinTimestamp(review);
                }
            }
            for (Long key : keysToRemove) {
                reviewStore.delete(key);
            }
        }
        // this is okay because we called this.context.forward() multiple times before
        return null;
    }

    private Boolean isReviewExpired(Review review, Long currentTime, Long maxTime) {
        return review.getCreated().getMillis() + maxTime < currentTime;
    }

    private Review reverseReview(Review review) {
        return Review.newBuilder(review)
                .setRating("-" + review.getRating())
                .build();
    }

    @Override
    public void close() {
        // do nothing
    }

}
