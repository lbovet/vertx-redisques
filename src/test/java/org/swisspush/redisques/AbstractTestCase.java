package org.swisspush.redisques;

import com.google.common.collect.ImmutableMap;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import redis.clients.jedis.Jedis;

import java.util.Map;

import static org.swisspush.redisques.util.RedisquesAPI.REQUESTED_BY;

@RunWith(VertxUnitRunner.class)
public abstract class AbstractTestCase {

    protected static final String QUEUES_PREFIX = "redisques:queues:";
    protected static final String TIMESTAMP = "timestamp";
    protected static final String REDISQUES_LOCKS = "redisques:locks";
    protected static final String PROCESSOR_ADDRESS = "processor-address";

    protected static Logger log = LoggerFactory.getLogger(AbstractTestCase.class);

    protected static Vertx vertx;
    protected static Jedis jedis;

    protected static String deploymentId = "";

    protected void flushAll(){
        if(jedis != null){
            jedis.flushAll();
        }
    }

    protected void assertKeyCount(TestContext context, int keyCount){
        assertKeyCount(context, "", keyCount);
    }

    protected void assertKeyCount(TestContext context, String prefix, int keyCount){
        context.assertEquals(keyCount, jedis.keys(prefix+"*").size());
    }

    protected void assertLockContent(TestContext context, String queuename, String expectedRequestedByValue){
        String item = jedis.hget(REDISQUES_LOCKS, queuename);
        context.assertNotNull(item);
        if(item != null){
            JsonObject lockInfo = new JsonObject(item);
            context.assertNotNull(lockInfo.getString(REQUESTED_BY), "Property '"+REQUESTED_BY+"' missing");
            context.assertNotNull(lockInfo.getLong(TIMESTAMP), "Property '"+TIMESTAMP+"' missing");
            context.assertEquals(expectedRequestedByValue, lockInfo.getString(REQUESTED_BY), "Property '"+REQUESTED_BY+"' has wrong value");
        }
    }

    @BeforeClass
    public static void config(TestContext context) {
        if(!RedisEmbeddedConfiguration.useExternalRedis()) {
            RedisEmbeddedConfiguration.redisServer.start();
        }
    }

    @AfterClass
    public static void stopRedis(TestContext context) {
        if(!RedisEmbeddedConfiguration.useExternalRedis()) {
            RedisEmbeddedConfiguration.redisServer.stop();
        }
        jedis.close();
    }

    @Before
    public void cleanDB() {
        flushAll();
    }

    protected void eventBusSend(JsonObject operation, Handler<AsyncResult<Message<JsonObject>>> handler){
        vertx.eventBus().send("redisques", operation, handler);
    }

    protected void lockQueue(String queue){
        JsonObject lockInfo = new JsonObject();
        lockInfo.put(REQUESTED_BY, "unit_test");
        lockInfo.put("timestamp", System.currentTimeMillis());
        Map<String,String> values = ImmutableMap.of(queue, lockInfo.encode());
        jedis.hmset("redisques:locks", values);
    }
}