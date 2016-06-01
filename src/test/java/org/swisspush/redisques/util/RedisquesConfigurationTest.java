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
        testContext.assertEquals(config.getCheckInterval(), 60);
        testContext.assertEquals(config.getProcessorTimeout(), 240000);
    }

    @Test
    public void testOverrideConfiguration(TestContext testContext){
        RedisquesConfiguration config = with()
                .address("new_address")
                .redisHost("anotherhost")
                .redisPort(1234)
                .checkInterval(5)
                .processorTimeout(10)
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
        testContext.assertEquals(config.getCheckInterval(), 5);
        testContext.assertEquals(config.getProcessorTimeout(), 10);
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
        testContext.assertEquals(json.getInteger(PROP_CHECK_INTERVAL), 60);
        testContext.assertEquals(json.getInteger(PROP_PROCESSOR_TIMEOUT), 240000);
    }

    @Test
    public void testGetOverridenAsJsonObject(TestContext testContext){

        RedisquesConfiguration config = with()
                .address("new_address")
                .redisHost("anotherhost")
                .redisPort(1234)
                .checkInterval(5)
                .processorTimeout(20)
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
        testContext.assertEquals(json.getInteger(PROP_CHECK_INTERVAL), 5);
        testContext.assertEquals(json.getInteger(PROP_PROCESSOR_TIMEOUT), 20);
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
        testContext.assertEquals(config.getCheckInterval(), 60);
        testContext.assertEquals(config.getProcessorTimeout(), 240000);
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
        json.put(PROP_CHECK_INTERVAL, 5);
        json.put(PROP_PROCESSOR_TIMEOUT, 30);

        RedisquesConfiguration config = fromJsonObject(json);
        testContext.assertEquals(config.getAddress(), "new_address");
        testContext.assertEquals(config.getRedisPrefix(), "new_redis-prefix");
        testContext.assertEquals(config.getProcessorAddress(), "new_processor-address");
        testContext.assertEquals(config.getRefreshPeriod(), 99);
        testContext.assertEquals(config.getRedisHost(), "newredishost");
        testContext.assertEquals(config.getRedisPort(), 4321);
        testContext.assertEquals(config.getRedisEncoding(), "new_encoding");
        testContext.assertEquals(config.getCheckInterval(), 5);
        testContext.assertEquals(config.getProcessorTimeout(), 30);
    }

    @Test
    public void testCleanupInterval(TestContext testContext){
        int additional = 500;
        RedisquesConfiguration config = with().checkInterval(5).build();
        testContext.assertEquals(5, config.getCheckInterval());
        testContext.assertEquals(add500ms(2500), config.getCheckIntervalTimerMs());

        config = with().checkInterval(1).build();
        testContext.assertEquals(1, config.getCheckInterval());
        testContext.assertEquals(add500ms(500), config.getCheckIntervalTimerMs());

        config = with().checkInterval(2).build();
        testContext.assertEquals(2, config.getCheckInterval());
        testContext.assertEquals(add500ms(1000), config.getCheckIntervalTimerMs());

        config = with().checkInterval(3).build();
        testContext.assertEquals(3, config.getCheckInterval());
        testContext.assertEquals(add500ms(1500), config.getCheckIntervalTimerMs());

        config = with().checkInterval(7).build();
        testContext.assertEquals(7, config.getCheckInterval());
        testContext.assertEquals(add500ms(3500), config.getCheckIntervalTimerMs());

        config = with().checkInterval(0).build();
        testContext.assertEquals(60, config.getCheckInterval());
        testContext.assertEquals(add500ms(30000), config.getCheckIntervalTimerMs());

        config = with().checkInterval(-5).build();
        testContext.assertEquals(60, config.getCheckInterval());
        testContext.assertEquals(add500ms(30000), config.getCheckIntervalTimerMs());

        config = with().checkInterval(60).build();
        testContext.assertEquals(60, config.getCheckInterval());
        testContext.assertEquals(add500ms(30000), config.getCheckIntervalTimerMs());

        JsonObject json = new JsonObject();
        json.put(PROP_CHECK_INTERVAL, 5);
        config = fromJsonObject(json);
        testContext.assertEquals(5, config.getCheckInterval());
        testContext.assertEquals(add500ms(2500), config.getCheckIntervalTimerMs());
    }

    private int add500ms(int interval){
        return interval + 500;
    }
}
