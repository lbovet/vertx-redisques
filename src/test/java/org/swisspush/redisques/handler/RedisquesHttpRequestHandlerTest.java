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

    private final String queueItemValid2 = "{\n" +
            "  \"method\": \"PUT\",\n" +
            "  \"uri\": \"/some/url/333/555\",\n" +
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
                .address(getRedisquesAddress())
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
    public void enqueueValidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemValid).when().put("/queuing/enqueue/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        given().body(queueItemValid).when().put("/queuing/enqueue/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(2L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        context.assertFalse(jedis.hexists(getLocksRedisKey(), queueName));

        async.complete();
    }

    @Test
    public void enqueueInvalidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemInvalid).when().put("/queuing/enqueue/"+queueName+"/").then().assertThat().statusCode(400);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        context.assertEquals(0L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        context.assertFalse(jedis.hexists(getLocksRedisKey(), queueName));

        async.complete();
    }

    @Test
    public void lockedEnqueueValidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);
        context.assertFalse(jedis.hexists(getLocksRedisKey(), queueName));

        given().body(queueItemValid).when().put("/queuing/enqueue/"+queueName+"/?locked").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));
        context.assertTrue(jedis.hexists(getLocksRedisKey(), queueName));
        assertLockContent(context, queueName, "Unknown");

        given().body(queueItemValid).when().put("/queuing/enqueue/"+queueName+"/?locked").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(2L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        async.complete();
    }

    @Test
    public void lockedEnqueueValidBodyRequestedByHeader(TestContext context) {
        Async async = context.async();
        flushAll();
        long ts = System.currentTimeMillis();
        String queueName = "queue_" + ts;
        String requestedBy = "user_" + ts;
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);
        context.assertFalse(jedis.hexists(getLocksRedisKey(), queueName));

        given()
                .header("x-rp-usr", requestedBy)
                .body(queueItemValid)
                .when()
                .put("/queuing/enqueue/"+queueName+"/?locked")
                .then().assertThat().statusCode(200);

        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));
        context.assertTrue(jedis.hexists(getLocksRedisKey(), queueName));
        assertLockContent(context, queueName, requestedBy);

        async.complete();
    }

    @Test
    public void lockedEnqueueInvalidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);
        context.assertFalse(jedis.hexists(getLocksRedisKey(), queueName));

        given().body(queueItemInvalid).when().put("/queuing/enqueue/"+queueName+"/?locked").then().assertThat().statusCode(400);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        context.assertEquals(0L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        context.assertFalse(jedis.hexists(getLocksRedisKey(), queueName));

        async.complete();
    }

    @Test
    public void addQueueItemValidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(2L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        async.complete();
    }

    @Test
    public void addQueueItemInvalidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemInvalid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(400);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        context.assertEquals(0L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        async.complete();
    }

    @Test
    public void getSingleQueueItemWithNonNumericIndex(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

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
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

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
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        String numericIndex = "0";
        when().get("/queuing/queues/"+queueName+"/"+numericIndex).then().assertThat()
                .statusCode(200)
                .header("content-type", "application/json")
                .body(equalTo(new JsonObject(queueItemValid).toString()));

        async.complete();
    }

    @Test
    public void replaceSingleQueueItemOfUnlockedQueue(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        given().body(queueItemValid2).when().put("/queuing/queues/"+queueName+"/0").then().assertThat().statusCode(409).body(containsString("Queue must be locked to perform this operation"));
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        // check queue item has not been replaced
        when().get("/queuing/queues/"+queueName+"/0").then().assertThat()
                .statusCode(200)
                .header("content-type", "application/json")
                .body(equalTo(new JsonObject(queueItemValid).toString()));

        // replacing with an invalid resource
        given().body(queueItemInvalid).when().put("/queuing/queues/"+queueName+"/0").then().assertThat().statusCode(409).body(containsString("Queue must be locked to perform this operation"));
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        async.complete();
    }

    @Test
    public void replaceSingleQueueItemWithInvalidBody(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        // lock queue
        given().body("{}").when().put("/queuing/locks/" + queueName).then().assertThat().statusCode(200);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        given().body(queueItemInvalid).when().put("/queuing/queues/"+queueName+"/0").then().assertThat().statusCode(400);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        // check queue item has not been replaced
        when().get("/queuing/queues/"+queueName+"/0").then().assertThat()
                .statusCode(200)
                .header("content-type", "application/json")
                .body(equalTo(new JsonObject(queueItemValid).toString()));

        async.complete();
    }

    @Test
    public void replaceSingleQueueItemWithNotExistingIndex(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        // lock queue
        given().body("{}").when().put("/queuing/locks/" + queueName).then().assertThat().statusCode(200);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        given().body(queueItemValid2).when().put("/queuing/queues/"+queueName+"/10").then().assertThat().statusCode(404).body(containsString("Not Found"));
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        // check queue item has not been replaced
        when().get("/queuing/queues/"+queueName+"/0").then().assertThat()
                .statusCode(200)
                .header("content-type", "application/json")
                .body(equalTo(new JsonObject(queueItemValid).toString()));

        async.complete();
    }

    @Test
    public void replaceSingleQueueItem(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        // lock queue
        given().body("{}").when().put("/queuing/locks/" + queueName).then().assertThat().statusCode(200);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        given().body(queueItemValid2).when().put("/queuing/queues/"+queueName+"/0").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        // check queue item has not been replaced
        when().get("/queuing/queues/"+queueName+"/0").then().assertThat()
                .statusCode(200)
                .header("content-type", "application/json")
                .body(equalTo(new JsonObject(queueItemValid2).toString()));

        async.complete();
    }

    @Test
    public void deleteQueueItemWithNonNumericIndex(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        // try to delete with non-numeric index
        String nonnumericIndex = "xx";
        when().delete("/queuing/queues/"+queueName+"/"+nonnumericIndex).then().assertThat().statusCode(405);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        async.complete();
    }

    @Test
    public void deleteQueueItemOfUnlockedQueue(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        String numericIndex = "22";
        when().delete("/queuing/queues/"+queueName+"/"+numericIndex).then().assertThat().statusCode(409).body(containsString("Queue must be locked to perform this operation"));
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        async.complete();
    }

    @Test
    public void deleteQueueItemNonExistingIndex(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        // lock queue
        given().body("{}").when().put("/queuing/locks/" + queueName).then().assertThat().statusCode(200);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        String numericIndex = "22";
        when().delete("/queuing/queues/"+queueName+"/"+numericIndex).then().assertThat().statusCode(404).body(containsString("Not Found"));
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        async.complete();
    }

    @Test
    public void deleteQueueItem(TestContext context) {
        Async async = context.async();
        flushAll();
        String queueName = "queue_" + System.currentTimeMillis();
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        assertKeyCount(context, getQueuesRedisKeyPrefix() + queueName, 0);

        // lock queue
        given().body("{}").when().put("/queuing/locks/" + queueName).then().assertThat().statusCode(200);

        given().body(queueItemValid).when().post("/queuing/queues/"+queueName+"/").then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);
        context.assertEquals(1L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

        String numericIndex = "0";
        when().delete("/queuing/queues/"+queueName+"/"+numericIndex).then().assertThat().statusCode(200);
        assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);
        context.assertEquals(0L, jedis.llen(getQueuesRedisKeyPrefix() + queueName));

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
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 1);

            // delete all queue items
            when().delete("/queuing/queues/queueEnqueue")
                    .then().assertThat()
                    .statusCode(200);
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);

            // delete all queue items again
            when().delete("/queuing/queues/queueEnqueue")
                    .then().assertThat()
                    .statusCode(200);
            assertKeyCount(context, getQueuesRedisKeyPrefix(), 0);

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

        context.assertFalse(jedis.hexists(getLocksRedisKey(), lock));

        given()
                .header("x-rp-usr", requestedBy)
                .body("{}")
                .when()
                .put("/queuing/locks/" + lock).then().assertThat().statusCode(200);

        context.assertTrue(jedis.hexists(getLocksRedisKey(), lock));
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

        context.assertFalse(jedis.hexists(getLocksRedisKey(), lock));

        given()
                .header("wrong-user-header", requestedBy)
                .body("{}")
                .when()
                .put("/queuing/locks/" + lock).then().assertThat().statusCode(200);

        context.assertTrue(jedis.hexists(getLocksRedisKey(), lock));
        assertLockContent(context, lock, "Unknown");

        async.complete();
    }

    @Test
    public void addLockNoUserHeader(TestContext context) {
        Async async = context.async();
        flushAll();
        String lock = "myLock_" + System.currentTimeMillis();

        context.assertFalse(jedis.hexists(getLocksRedisKey(), lock));

        given()
                .body("{}")
                .when()
                .put("/queuing/locks/" + lock).then().assertThat().statusCode(200);

        context.assertTrue(jedis.hexists(getLocksRedisKey(), lock));
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

            context.assertTrue(jedis.hexists(getLocksRedisKey(), lock));

            when().delete("/queuing/locks/" + lock)
                    .then().assertThat()
                    .statusCode(200);

            context.assertFalse(jedis.hexists(getLocksRedisKey(), lock));

            async.complete();
        });
    }

    @Test
    public void getMonitorInformation(TestContext context) {
        Async async = context.async();
        flushAll();

        when().get("/queuing/monitor").then().assertThat().statusCode(200)
                .body("queues", empty());

        // prepare
        eventBusSend(buildEnqueueOperation("queue_1", "item1_1"), null);
        eventBusSend(buildEnqueueOperation("queue_1", "item1_2"), null);
        eventBusSend(buildEnqueueOperation("queue_1", "item1_3"), null);

        eventBusSend(buildEnqueueOperation("queue_2", "item2_1"), null);
        eventBusSend(buildEnqueueOperation("queue_2", "item2_2"), null);

        eventBusSend(buildEnqueueOperation("queue_3", "item3_1"), null);

        // lock queue
        given().body("{}").when().put("/queuing/locks/queue_3").then().assertThat().statusCode(200);
        when().delete("/queuing/queues/queue_3/0").then().assertThat().statusCode(200);

        String expectedNoEmptyQueuesNoLimit = "{\n" +
                "  \"queues\": [\n" +
                "    {\n" +
                "      \"name\": \"queue_1\",\n" +
                "      \"size\": 3\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"queue_2\",\n" +
                "      \"size\": 2\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        when().get("/queuing/monitor").then().assertThat().statusCode(200)
                .body(equalTo(new JsonObject(expectedNoEmptyQueuesNoLimit).toString()));

        String expectedWithEmptyQueuesNoLimit = "{\n" +
                "  \"queues\": [\n" +
                "    {\n" +
                "      \"name\": \"queue_1\",\n" +
                "      \"size\": 3\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"queue_2\",\n" +
                "      \"size\": 2\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"queue_3\",\n" +
                "      \"size\": 0\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        when().get("/queuing/monitor?emptyQueues").then().assertThat().statusCode(200)
                .body(equalTo(new JsonObject(expectedWithEmptyQueuesNoLimit).toString()));

        String expectedNoEmptyQueuesAndLimit3 = "{\n" +
                "  \"queues\": [\n" +
                "    {\n" +
                "      \"name\": \"queue_1\",\n" +
                "      \"size\": 3\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"queue_2\",\n" +
                "      \"size\": 2\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        when().get("/queuing/monitor?limit=3").then().assertThat().statusCode(200)
                .body(equalTo(new JsonObject(expectedNoEmptyQueuesAndLimit3).toString()));

        String expectedWithEmptyQueuesAndLimit3 = "{\n" +
                "  \"queues\": [\n" +
                "    {\n" +
                "      \"name\": \"queue_1\",\n" +
                "      \"size\": 3\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"queue_2\",\n" +
                "      \"size\": 2\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"queue_3\",\n" +
                "      \"size\": 0\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        when().get("/queuing/monitor?limit=3&emptyQueues").then().assertThat().statusCode(200)
                .body(equalTo(new JsonObject(expectedWithEmptyQueuesAndLimit3).toString()));

        String expectedWithEmptyQueuesAndInvalidLimit = "{\n" +
                "  \"queues\": [\n" +
                "    {\n" +
                "      \"name\": \"queue_1\",\n" +
                "      \"size\": 3\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"queue_2\",\n" +
                "      \"size\": 2\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"queue_3\",\n" +
                "      \"size\": 0\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        when().get("/queuing/monitor?limit=xx99xx&emptyQueues").then().assertThat().statusCode(200)
                .body(equalTo(new JsonObject(expectedWithEmptyQueuesAndInvalidLimit).toString()));

        async.complete();
    }
}
