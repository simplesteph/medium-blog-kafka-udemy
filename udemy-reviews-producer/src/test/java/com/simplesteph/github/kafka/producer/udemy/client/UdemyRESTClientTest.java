package com.simplesteph.github.kafka.producer.udemy.client;

import com.github.simplesteph.avro.udemy.Course;
import com.github.simplesteph.avro.udemy.Review;
import com.github.simplesteph.avro.udemy.User;
import com.github.simplesteph.kafka.producer.udemy.client.UdemyRESTClient;
import com.github.simplesteph.kafka.producer.udemy.model.ReviewApiResponse;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.http.HttpException;
import org.joda.time.DateTime;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;

public class UdemyRESTClientTest {

    private UdemyRESTClient udemyRESTClient;

    @Before
    public void setUp(){
        udemyRESTClient = new UdemyRESTClient("1075642", 30);
    }

    @Test
    public void apiCallIsSuccessful() throws HttpException {
        ReviewApiResponse response = udemyRESTClient.reviewApi( 3, 2);
        assertTrue(response.getCount() > 0);
        assertTrue(response.getNext() != null);
        assertTrue(response.getPrevious() != null);
        List<Review> reviews = response.getReviewList();
        assertTrue(reviews.size() == 3);
        // here we test the sort order
        assertTrue(reviews.get(0).getCreated().isBefore(reviews.get(1).getCreated()));
    }

    @Test
    public void canParseReviewJson(){
        JsonNode json = new JsonNode("{\n" +
                "\"_class\": \"course_review\",\n" +
                "\"id\": 6225498,\n" +
                "\"title\": \"\",\n" +
                "\"content\": \"\",\n" +
                "\"rating\": 4.0,\n" +
                "\"created\": \"2017-03-30T20:13:54Z\",\n" +
                "\"modified\": \"2017-03-31T21:23:04Z\",\n" +
                "\"user\": {\n" +
                "\"_class\": \"user\",\n" +
                "\"id\": 30105149,\n" +
                "\"title\": \"Renato Dias Santana\",\n" +
                "\"name\": \"Renato Dias\",\n" +
                "\"display_name\": \"Renato Dias Santana\"\n" +
                "},\n" +
                "\"course\": {\n" +
                "\"_class\": \"course\",\n" +
                "\"id\": 1075642,\n" +
                "\"title\": \"Apache Kafka Series - Learn Apache Kafka for Beginners\",\n" +
                "\"url\": \"/apache-kafka-series-kafka-from-beginner-to-intermediate/\"\n" +
                "}\n" +
                "}");

        Review review = udemyRESTClient.jsonToReview(json.getObject());
        assertTrue(review.getId() == 6225498L);
        assertEquals(review.getTitle(), "");
        assertEquals(review.getContent(), "");
        assertEquals(review.getRating(), "4.0");
        assertEquals(review.getCreated(), DateTime.parse("2017-03-30T20:13:54Z"));
        assertEquals(review.getModified(), DateTime.parse("2017-03-31T21:23:04Z"));
        assertNotNull(review.getUser());
    }

    @Test
    public void canParseUserJson(){
        JsonNode json = new JsonNode("{\n" +
                "\"_class\": \"user\",\n" +
                "\"id\": 30660398,\n" +
                "\"title\": \"Tintu Pathrose\",\n" +
                "\"name\": \"Tintu\",\n" +
                "\"display_name\": \"Tintu Pathrose\"\n" +
                "}");

        User user = udemyRESTClient.jsonToUser(json.getObject());
        assertEquals(user.getDisplayName(), "Tintu Pathrose");
        assertEquals(user.getId(), (Long) 30660398L);
        assertEquals(user.getName(), "Tintu");
        assertEquals(user.getTitle(), "Tintu Pathrose");
    }

    @Test
    public void canParseCourseJson(){
        JsonNode json = new JsonNode("{\n" +
                "\"_class\": \"course\",\n" +
                "\"id\": 1075642,\n" +
                "\"title\": \"Apache Kafka Series - Learn Apache Kafka for Beginners\",\n" +
                "\"url\": \"/apache-kafka-series-kafka-from-beginner-to-intermediate/\"\n" +
                "}");

        Course course = udemyRESTClient.jsonToCourse(json.getObject());
        assertEquals(course.getId(), (Long) 1075642L);
        assertEquals(course.getTitle(), "Apache Kafka Series - Learn Apache Kafka for Beginners");
        assertEquals(course.getUrl(), "/apache-kafka-series-kafka-from-beginner-to-intermediate/");
    }

    @Test
    public void twoApiCallsReturnTwoDifferentSetsOfResults() throws HttpException {
        List<Review> reviews1 = udemyRESTClient.getNextReviews();
        List<Review> reviews2 = udemyRESTClient.getNextReviews();
        // the first review of the new batch is after the last review of the old batch
        assertTrue(reviews2.get(0).getCreated().isAfter(reviews1.get(reviews1.size() - 1).getCreated()));
    }
}
