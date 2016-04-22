package org.swisspush.redisques.util;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.swisspush.redisques.util.RedisquesConfiguration.*;

/**
 * Tests for {@link RedisquesConfiguration} class.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class RedisquesConfigurationTest {
    @Test
    public void testDefaultConfiguration(TestContext testContext){
        RedisquesConfiguration config = new RedisquesConfiguration();
        testContext.assertEquals(config.getAddress(), "redisques");
        testContext.assertEquals(config.getRedisPrefix(), "redisques:");
        testContext.assertEquals(config.getProcessorAddress(), "redisques-processor");
        testContext.assertEquals(config.getRefreshPeriod(), 10);
        testContext.assertEquals(config.getRedisHost(), "localhost");
        testContext.assertEquals(config.getRedisPort(), 6379);
        testContext.assertEquals(config.getRedisEncoding(), "UTF-8");
    }

    @Test
    public void testOverrideConfiguration(TestContext testContext){
        RedisquesConfiguration config = with()
                .address("new_address")
                .redisHost("anotherhost")
                .redisPort(1234)
                .build();

        // default values
        testContext.assertEquals(config.getRedisPrefix(), "redisques:");
        testContext.assertEquals(config.getProcessorAddress(), "redisques-processor");
        testContext.assertEquals(config.getRefreshPeriod(), 10);
        testContext.assertEquals(config.getRedisEncoding(), "UTF-8");

        // overriden values
        testContext.assertEquals(config.getAddress(), "new_address");
        testContext.assertEquals(config.getRedisHost(), "anotherhost");
        testContext.assertEquals(config.getRedisPort(), 1234);
    }

    @Test
    public void testGetDefaultAsJsonObject(TestContext testContext){
        RedisquesConfiguration config = new RedisquesConfiguration();
        JsonObject json = config.asJsonObject();

        testContext.assertEquals(json.getString(PROP_ADDRESS), "redisques");
        testContext.assertEquals(json.getString(PROP_REDIS_PREFIX), "redisques:");
        testContext.assertEquals(json.getString(PROP_PROCESSOR_ADDRESS), "redisques-processor");
        testContext.assertEquals(json.getInteger(PROP_REFRESH_PERIOD), 10);
        testContext.assertEquals(json.getString(PROP_REDIS_HOST), "localhost");
        testContext.assertEquals(json.getInteger(PROP_REDIS_PORT), 6379);
        testContext.assertEquals(json.getString(PROP_REDIS_ENCODING), "UTF-8");
    }

    @Test
    public void testGetOverridenAsJsonObject(TestContext testContext){

        RedisquesConfiguration config = with()
                .address("new_address")
                .redisHost("anotherhost")
                .redisPort(1234)
                .build();

        JsonObject json = config.asJsonObject();

        // default values
        testContext.assertEquals(json.getString(PROP_REDIS_PREFIX), "redisques:");
        testContext.assertEquals(json.getString(PROP_PROCESSOR_ADDRESS), "redisques-processor");
        testContext.assertEquals(json.getInteger(PROP_REFRESH_PERIOD), 10);
        testContext.assertEquals(json.getString(PROP_REDIS_ENCODING), "UTF-8");

        // overriden values
        testContext.assertEquals(json.getString(PROP_ADDRESS), "new_address");
        testContext.assertEquals(json.getString(PROP_REDIS_HOST), "anotherhost");
        testContext.assertEquals(json.getInteger(PROP_REDIS_PORT), 1234);
    }

    @Test
    public void testGetDefaultFromJsonObject(TestContext testContext){
        JsonObject json  = new RedisquesConfiguration().asJsonObject();
        RedisquesConfiguration config = fromJsonObject(json);

        testContext.assertEquals(config.getAddress(), "redisques");
        testContext.assertEquals(config.getRedisPrefix(), "redisques:");
        testContext.assertEquals(config.getProcessorAddress(), "redisques-processor");
        testContext.assertEquals(config.getRefreshPeriod(), 10);
        testContext.assertEquals(config.getRedisHost(), "localhost");
        testContext.assertEquals(config.getRedisPort(), 6379);
        testContext.assertEquals(config.getRedisEncoding(), "UTF-8");
    }

    @Test
    public void testGetOverridenFromJsonObject(TestContext testContext){

        JsonObject json = new JsonObject();
        json.put(PROP_ADDRESS, "new_address");
        json.put(PROP_REDIS_PREFIX, "new_redis-prefix");
        json.put(PROP_PROCESSOR_ADDRESS, "new_processor-address");
        json.put(PROP_REFRESH_PERIOD, 99);
        json.put(PROP_REDIS_HOST, "newredishost");
        json.put(PROP_REDIS_PORT, 4321);
        json.put(PROP_REDIS_ENCODING, "new_encoding");

        RedisquesConfiguration config = fromJsonObject(json);
        testContext.assertEquals(config.getAddress(), "new_address");
        testContext.assertEquals(config.getRedisPrefix(), "new_redis-prefix");
        testContext.assertEquals(config.getProcessorAddress(), "new_processor-address");
        testContext.assertEquals(config.getRefreshPeriod(), 99);
        testContext.assertEquals(config.getRedisHost(), "newredishost");
        testContext.assertEquals(config.getRedisPort(), 4321);
        testContext.assertEquals(config.getRedisEncoding(), "new_encoding");
    }

}
