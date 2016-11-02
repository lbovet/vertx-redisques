package org.swisspush.redisques.handler;

import com.jayway.restassured.RestAssured;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import org.junit.*;
import org.swisspush.redisques.AbstractTestCase;
import org.swisspush.redisques.RedisQues;
import org.swisspush.redisques.util.RedisquesConfiguration;
import redis.clients.jedis.Jedis;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.when;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.swisspush.redisques.util.RedisquesAPI.buildEnqueueOperation;
import static org.swisspush.redisques.util.RedisquesAPI.buildPutLockOperation;

/**
 * Tests for the {@link RedisquesHttpRequestHandler} class
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class RedisquesHttpRequestHandlerTest extends AbstractTestCase {

    private final String queueItemValid = "{\n" +
            "  \"method\": \"PUT\",\n" +
            "  \"uri\": \"/some/url/123/456\",\n" +
            "  \"headers\": [\n" +
            "    [\n" +
            "      \"Accept\",\n" +
            "      \"text/plain, */*; q=0.01\"\n" +
            "    ],\n" +
            "    [\n" +
            "      \"Content-Type\",\n" +
            "      \"application/json\"\n" +
            "    ],\n" +
            "    [\n" +
            "      \"Accept-Charset\",\n" +
            "      \"utf-8, iso-8859-1, utf-16, *;q=0.7\"\n" +
            "    ]\n" +
            "  ],\n" +
            "  \"queueTimestamp\": 1477983671291,\n" +
            "  \"payloadObject\": {\n" +
            "    \"actionTime\": \"2016-11-01T08:00:02.024+01:00\",\n" +
            "    \"type\": 2\n" +
            "  }\n" +
            "}";

    private final String queueItemInvalid = "\n" +
            "  \"method\": \"PUT\",\n" +
            "  \"uri\": \"/some/url/123/456\",\n" +
            "  \"headers\": [\n" +
            "    [\n" +
            "      \"Accept\",\n" +
            "      \"text/plain, */*; q=0.01\"\n" +
            "    ],\n" +
            "    [\n" +
            "      \"Content-Type\",\n" +
            "      \"application/json\"\n" +
            "    ],\n" +
            "    [\n" +
            "      \"Accept-Charset\",\n" +
            "      \"utf-8, iso-8859-1, utf-16, *;q=0.7\"\n" +
            "    ]\n" +
            "  ],\n" +
            "  \"queueTimestamp\": 1477983671291,\n" +
            "  \"payloadObject\": {\n" +
            "    \"actionTime\": \"2016-11-01T08:00:02.024+01:00\",\n" +
            "    \"type\": 2\n" +
            "  }\n" +
            "}";

    @Rule
    public Timeout rule = Timeout.seconds(15);

    @Before
    public void deployRedisques(TestContext context) {
        Async async = context.async();
        vertx = Vertx.vertx();

        RestAssured.baseURI = "http://localhost";
        RestAssured.port = Integer.getInteger("http.port", 7070);

        JsonObject config = RedisquesConfiguration.with()
                .processorAddress("processor-address")
                .redisEncoding("ISO-8859-1")
                .refreshPeriod(2)
                .httpRequestHandlerEnabled(true)
                .httpRequestHandlerPort(7070)
                .build()
                .asJsonObject();

        RedisQues redisQues = new RedisQues();
        vertx.deployVerticle(redisQues, new DeploymentOptions().setConfig(config), context.asyncAssertSuccess(event -> {
            deploymentId = event;
            log.info("vert.x Deploy - " + redisQues.getClass().getSimpleName() + " was successful.");
            jedis = new Jedis("localhost", 6379, 5000);
            async.complete();
        }));
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testUnknownRequestUrl(TestContext context) {
        Async async = context.async();
        when()
                .get("/an/unknown/path/")
                .then().assertThat()
                .statusCode(405);
        async.complete();
    }

    @Test
    public void getQueuesCount(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queue_1", "item1_1"), m1 -> {
            eventBusSend(buildEnqueueOperation("queue_2", "item2_1"), m2 -> {
                eventBusSend(buildEnqueueOperation("queue_3", "item3_1"), m3 -> {
                    when()
                            .get("/queuing/queues/?count")
                            .then().assertThat()
                            .statusCode(200)
                            .body("count", equalTo(3));
                    async.complete();
                });
            });
        });
    }

    @Test
    public void getQueuesCountNoQueues(TestContext context) {
        Async async = context.async();
        flushAll();
        when()
                .get("/queuing/queues/?count")
                .then().assertThat()
                .statusCode(200)
                .body("count", equalTo(0));
        async.complete();
    }

    @Test
    public void listQueues(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queue_1", "item1_1"), m1 -> {
            eventBusSend(buildEnqueueOperation("queue_2", "item2_1"), m2 -> {
                eventBusSend(buildEnqueueOperation("queue_3", "item3_1"), m3 -> {
                    when()
                            .get("/queuing/queues/")
                            .then().assertThat()
                            .statusCode(200)
                            .body(
                                    "queues", hasItems("queue_1", "queue_2", "queue_3")
                            );
                    async.complete();
                });
            });
        });
    }

    @Test
    public void addQueueItemValidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, QUEUES_PREFIX, 0);
        assertKeyCount(context, QUEUES_PREFIX + queueName, 0);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, QUEUES_PREFIX, 1);
        context.assertEquals(1L, jedis.llen(QUEUES_PREFIX + queueName));

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, QUEUES_PREFIX, 1);
        context.assertEquals(2L, jedis.llen(QUEUES_PREFIX + queueName));

        async.complete();
    }

    @Test
    public void addQueueItemInvalidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, QUEUES_PREFIX, 0);
        assertKeyCount(context, QUEUES_PREFIX + queueName, 0);

        given().body(queueItemInvalid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(400);
        assertKeyCount(context, QUEUES_PREFIX, 0);
        context.assertEquals(0L, jedis.llen(QUEUES_PREFIX + queueName));

        async.complete();
    }

    @Test
    public void getSingleQueueItemWithNonNumericIndex(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, QUEUES_PREFIX, 0);
        assertKeyCount(context, QUEUES_PREFIX + queueName, 0);

        // try to get with non-numeric index
        String nonnumericIndex = "xx";
        when().get("/queuing/queues/"+queueName+"/"+nonnumericIndex).then().assertThat().statusCode(405);

        async.complete();
    }

    @Test
    public void getSingleQueueItemWithNonExistingIndex(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, QUEUES_PREFIX, 0);
        assertKeyCount(context, QUEUES_PREFIX + queueName, 0);

        // try to get with not existing index
        String notExistingIndex = "10";
        when().get("/queuing/queues/"+queueName+"/"+notExistingIndex).then().assertThat().statusCode(404).body(containsString("Not Found"));

        async.complete();
    }

    @Test
    public void getSingleQueueItem(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, QUEUES_PREFIX, 0);
        assertKeyCount(context, QUEUES_PREFIX + queueName, 0);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, QUEUES_PREFIX, 1);
        context.assertEquals(1L, jedis.llen(QUEUES_PREFIX + queueName));

        String numericIndex = "0";
        when().get("/queuing/queues/"+queueName+"/"+numericIndex).then().assertThat()
                .statusCode(200)
                .header("content-type", "application/json")
                .body(equalTo(new JsonObject(queueItemValid).toString()));

        async.complete();
    }

    @Test
    public void deleteQueueItemWithNonNumericIndex(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, QUEUES_PREFIX, 0);
        assertKeyCount(context, QUEUES_PREFIX + queueName, 0);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, QUEUES_PREFIX, 1);
        context.assertEquals(1L, jedis.llen(QUEUES_PREFIX + queueName));

        // try to delete with non-numeric index
        String nonnumericIndex = "xx";
        when().delete("/queuing/queues/"+queueName+"/"+nonnumericIndex).then().assertThat().statusCode(405);
        assertKeyCount(context, QUEUES_PREFIX, 1);
        context.assertEquals(1L, jedis.llen(QUEUES_PREFIX + queueName));

        async.complete();
    }

    @Test
    public void deleteQueueItemOfUnlockedQueue(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, QUEUES_PREFIX, 0);
        assertKeyCount(context, QUEUES_PREFIX + queueName, 0);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, QUEUES_PREFIX, 1);
        context.assertEquals(1L, jedis.llen(QUEUES_PREFIX + queueName));

        String numericIndex = "22";
        when().delete("/queuing/queues/"+queueName+"/"+numericIndex).then().assertThat().statusCode(409).body(containsString("Queue must be locked to perform this operation"));
        assertKeyCount(context, QUEUES_PREFIX, 1);
        context.assertEquals(1L, jedis.llen(QUEUES_PREFIX + queueName));

        async.complete();
    }

    @Test
    public void deleteQueueItemNonExistingIndex(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, QUEUES_PREFIX, 0);
        assertKeyCount(context, QUEUES_PREFIX + queueName, 0);

        // lock queue
        given().body("{}").when().put("/queuing/locks/" + queueName).then().assertThat().statusCode(200);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, QUEUES_PREFIX, 1);
        context.assertEquals(1L, jedis.llen(QUEUES_PREFIX + queueName));

        String numericIndex = "22";
        when().delete("/queuing/queues/"+queueName+"/"+numericIndex).then().assertThat().statusCode(404).body(containsString("Not Found"));
        assertKeyCount(context, QUEUES_PREFIX, 1);
        context.assertEquals(1L, jedis.llen(QUEUES_PREFIX + queueName));

        async.complete();
    }

    @Test
    public void deleteQueueItem(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, QUEUES_PREFIX, 0);
        assertKeyCount(context, QUEUES_PREFIX + queueName, 0);

        // lock queue
        given().body("{}").when().put("/queuing/locks/" + queueName).then().assertThat().statusCode(200);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, QUEUES_PREFIX, 1);
        context.assertEquals(1L, jedis.llen(QUEUES_PREFIX + queueName));

        String numericIndex = "0";
        when().delete("/queuing/queues/"+queueName+"/"+numericIndex).then().assertThat().statusCode(200);
        assertKeyCount(context, QUEUES_PREFIX, 0);
        context.assertEquals(0L, jedis.llen(QUEUES_PREFIX + queueName));

        // try to delete again
        when().delete("/queuing/queues/"+queueName+"/"+numericIndex).then().assertThat().statusCode(404).body(containsString("Not Found"));

        async.complete();
    }

    @Test
    public void getQueueItemsCount(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
            eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue2"), message2 -> {
                when()
                        .get("/queuing/queues/queueEnqueue?count")
                        .then().assertThat()
                        .statusCode(200)
                        .body("count", equalTo(2));
                async.complete();
            });
        });
    }

    @Test
    public void getQueueItemsCountOfUnknownQueue(TestContext context) {
        Async async = context.async();
        flushAll();
        when()
                .get("/queuing/queues/unknownQueue?count")
                .then().assertThat()
                .statusCode(200)
                .body("count", equalTo(0));
        async.complete();
    }

    @Test
    public void listQueueItems(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
            eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue2"), message2 -> {
                when().get("/queuing/queues/queueEnqueue")
                        .then().assertThat()
                        .statusCode(200)
                        .body("queueEnqueue", hasItems("helloEnqueue", "helloEnqueue2"));
                async.complete();
            });
        });
    }

    @Test
    public void listQueueItemsWithLimitParameter(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue1"), m1 -> {
            eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue2"), m2 -> {
                eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue3"), m3 -> {
                    eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue4"), m4 -> {
                        given()
                                .param("limit", 3)
                                .when()
                                .get("/queuing/queues/queueEnqueue")
                                .then().assertThat()
                                .statusCode(200)
                                .body(
                                        "queueEnqueue", hasItems("helloEnqueue1", "helloEnqueue2", "helloEnqueue3"),
                                        "queueEnqueue", not(hasItem("helloEnqueue4"))
                                );
                        async.complete();
                    });
                });
            });
        });
    }

    @Test
    public void listQueueItemsOfUnknownQueue(TestContext context) {
        Async async = context.async();
        when().get("/queuing/queues/unknownQueue")
                .then().assertThat()
                .statusCode(200)
                .body("unknownQueue", empty());
        async.complete();
    }

    @Test
    public void deleteAllQueueItems(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildEnqueueOperation("queueEnqueue", "helloEnqueue"), message -> {
            assertKeyCount(context, QUEUES_PREFIX, 1);

            // delete all queue items
            when().delete("/queuing/queues/queueEnqueue")
                    .then().assertThat()
                    .statusCode(200);
            assertKeyCount(context, QUEUES_PREFIX, 0);

            // delete all queue items again
            when().delete("/queuing/queues/queueEnqueue")
                    .then().assertThat()
                    .statusCode(200);
            assertKeyCount(context, QUEUES_PREFIX, 0);

            async.complete();
        });
    }

    @Test
    public void deleteAllQueueItemsOfNonExistingQueue(TestContext context) {
        Async async = context.async();
        flushAll();
        when().delete("/queuing/queues/notExistingQueue_" + System.currentTimeMillis())
                .then().assertThat()
                .statusCode(200);
        async.complete();
    }

    @Test
    public void getAllLocksWhenNoLocksPresent(TestContext context) {
        Async async = context.async();
        flushAll();
        when().get("/queuing/locks/")
                .then().assertThat()
                .statusCode(200)
                .body("locks", empty());
        async.complete();
    }

    @Test
    public void getAllLocks(TestContext context) {
        Async async = context.async();
        flushAll();
        eventBusSend(buildPutLockOperation("queue1", "someuser"), message -> {
            eventBusSend(buildPutLockOperation("queue2", "someuser"), message2 -> {
                when().get("/queuing/locks/")
                        .then().assertThat()
                        .statusCode(200)
                        .body("locks", hasItems("queue1", "queue2"));
                async.complete();
            });
        });
    }

    @Test
    public void getSingleLockNotExisting(TestContext context) {
        Async async = context.async();
        flushAll();
        when().get("/queuing/locks/notExisiting_" + System.currentTimeMillis())
                .then().assertThat()
                .statusCode(404)
                .body(containsString("No such lock"));
        async.complete();
    }

    @Test
    public void getSingleLock(TestContext context) {
        Async async = context.async();
        flushAll();
        long ts = System.currentTimeMillis();
        String lock = "myLock_" + ts;
        String requestedBy = "someuser_" + ts;
        eventBusSend(buildPutLockOperation(lock, requestedBy), message -> {
            when().get("/queuing/locks/" + lock)
                    .then().assertThat()
                    .statusCode(200)
                    .body(
                            "requestedBy", equalTo(requestedBy),
                            "timestamp", greaterThan(ts)
                    );
            async.complete();
        });
    }

    @Test
    public void addLock(TestContext context) {
        Async async = context.async();
        flushAll();
        long ts = System.currentTimeMillis();
        String lock = "myLock_" + ts;
        String requestedBy = "someuser_" + ts;

        context.assertFalse(jedis.hexists(REDISQUES_LOCKS, lock));

        given()
                .header("x-rp-usr", requestedBy)
                .body("{}")
                .when()
                .put("/queuing/locks/" + lock).then().assertThat().statusCode(200);

        context.assertTrue(jedis.hexists(REDISQUES_LOCKS, lock));
        assertLockContent(context, lock, requestedBy);

        async.complete();
    }

    @Test
    public void addLockWrongUserHeader(TestContext context) {
        Async async = context.async();
        flushAll();
        long ts = System.currentTimeMillis();
        String lock = "myLock_" + ts;
        String requestedBy = "someuser_" + ts;

        context.assertFalse(jedis.hexists(REDISQUES_LOCKS, lock));

        given()
                .header("wrong-user-header", requestedBy)
                .body("{}")
                .when()
                .put("/queuing/locks/" + lock).then().assertThat().statusCode(200);

        context.assertTrue(jedis.hexists(REDISQUES_LOCKS, lock));
        assertLockContent(context, lock, "Unknown");

        async.complete();
    }

    @Test
    public void addLockNoUserHeader(TestContext context) {
        Async async = context.async();
        flushAll();
        String lock = "myLock_" + System.currentTimeMillis();

        context.assertFalse(jedis.hexists(REDISQUES_LOCKS, lock));

        given()
                .body("{}")
                .when()
                .put("/queuing/locks/" + lock).then().assertThat().statusCode(200);

        context.assertTrue(jedis.hexists(REDISQUES_LOCKS, lock));
        assertLockContent(context, lock, "Unknown");

        async.complete();
    }

    @Test
    public void deleteSingleLockNotExisting(TestContext context) {
        Async async = context.async();
        flushAll();
        when().delete("/queuing/locks/notExisiting_" + System.currentTimeMillis()).then().assertThat().statusCode(200);
        async.complete();
    }

    @Test
    public void deleteSingleLock(TestContext context) {
        Async async = context.async();
        flushAll();
        long ts = System.currentTimeMillis();
        String lock = "myLock_" + ts;
        String requestedBy = "someuser_" + ts;
        eventBusSend(buildPutLockOperation(lock, requestedBy), message -> {

            context.assertTrue(jedis.hexists(REDISQUES_LOCKS, lock));

            when().delete("/queuing/locks/" + lock)
                    .then().assertThat()
                    .statusCode(200);

            context.assertFalse(jedis.hexists(REDISQUES_LOCKS, lock));

            async.complete();
        });
    }
}
