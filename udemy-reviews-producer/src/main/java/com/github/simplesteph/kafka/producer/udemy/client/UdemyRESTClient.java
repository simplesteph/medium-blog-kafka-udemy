package com.github.simplesteph.kafka.producer.udemy.client;

import com.github.simplesteph.avro.udemy.Course;
import com.github.simplesteph.avro.udemy.Review;
import com.github.simplesteph.avro.udemy.User;
import com.github.simplesteph.kafka.producer.udemy.model.ReviewApiResponse;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.http.HttpException;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class UdemyRESTClient {

    private Integer count;
    private String courseId;
    private Integer nextPage;
    private final Integer pageSize;

    public UdemyRESTClient(String courseId, Integer pageSize) {
        this.pageSize = pageSize;
        this.courseId = courseId;
    }

    private void init() throws HttpException {
        count = reviewApi(1, 1).getCount();
        // we fetch from the last page
        nextPage = count / pageSize + 1;
    }

    public List<Review> getNextReviews() throws HttpException {
        if (nextPage == null) init();
        if (nextPage >= 1) {
            List<Review> result = reviewApi(pageSize, nextPage).getReviewList();
            nextPage -= 1;
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    public ReviewApiResponse reviewApi(Integer pageSize, Integer page) throws HttpException {
        String url = "https://www.udemy.com/api-2.0/courses/" + courseId + "/reviews";
        HttpResponse<JsonNode> jsonResponse = null;
        try {
            jsonResponse = Unirest.get(url)
                    .queryString("page", page)
                    .queryString("page_size", pageSize)
                    .queryString("fields[course_review]", "@default,course")
                    .asJson();
        } catch (UnirestException e) {
            throw new HttpException(e.getMessage());
        }

        if (jsonResponse.getStatus() == 200) {
            JSONObject body = jsonResponse.getBody().getObject();
            Integer count = body.getInt("count");
            String next = body.optString("next");
            String previous = body.optString("previous");
            List<Review> reviews = this.convertResults(body.getJSONArray("results"));
            ReviewApiResponse reviewApiResponse = new ReviewApiResponse(count, next, previous, reviews);
            return reviewApiResponse;
        } else {
            throw new HttpException("Udemy API Unavailable");
        }
    }

    public List<Review> convertResults(JSONArray resultsJsonArray) {
        List<Review> results = new ArrayList<>();
        for (int i = 0; i < resultsJsonArray.length(); i++) {
            JSONObject reviewJson = resultsJsonArray.getJSONObject(i);
            Review review = jsonToReview(reviewJson);
            results.add(review);
        }
        results.sort(Comparator.comparing(Review::getCreated));
        return results;
    }

    public Review jsonToReview(JSONObject reviewJson) {
        Review.Builder reviewBuilder = Review.newBuilder();
        reviewBuilder.setContent(reviewJson.getString("content"));
        reviewBuilder.setId(reviewJson.getLong("id"));
        reviewBuilder.setRating(reviewJson.getBigDecimal("rating").toPlainString());
        reviewBuilder.setTitle(reviewJson.getString("content"));
        reviewBuilder.setCreated(DateTime.parse(reviewJson.getString("created")));
        reviewBuilder.setModified(DateTime.parse(reviewJson.getString("modified")));
        reviewBuilder.setUser(jsonToUser(reviewJson.getJSONObject("user")));
        reviewBuilder.setCourse(jsonToCourse(reviewJson.getJSONObject("course")));
        return reviewBuilder.build();
    }

    public User jsonToUser(JSONObject userJson) {
        User.Builder userBuilder = User.newBuilder();
        userBuilder.setDisplayName(userJson.getString("display_name"));
        userBuilder.setName(userJson.getString("name"));
        userBuilder.setId(userJson.getLong("id"));
        userBuilder.setTitle(userJson.getString("title"));
        return userBuilder.build();
    }

    public Course jsonToCourse(JSONObject courseJson) {
        Course.Builder courseBuilder = Course.newBuilder();
        courseBuilder.setId(courseJson.getLong("id"));
        courseBuilder.setTitle(courseJson.getString("title"));
        courseBuilder.setUrl(courseJson.getString("url"));
        return courseBuilder.build();
    }

    public void close() {
        try {
            Unirest.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
