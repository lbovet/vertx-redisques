package org.swisspush.redisques.util;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Utility class to configure the Redisques module.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class RedisquesConfiguration {
    private String address;
    private String redisPrefix;
    private String processorAddress;
    private int refreshPeriod;
    private String redisHost;
    private int redisPort;
    private String redisEncoding;
    private int checkInterval;

    private static final int DEFAULT_CHECK_INTERVAL = 60; // 60s

    public static final String PROP_ADDRESS = "address";
    public static final String PROP_REDIS_PREFIX = "redis-prefix";
    public static final String PROP_PROCESSOR_ADDRESS = "processor-address";
    public static final String PROP_REFRESH_PERIOD = "refresh-period";
    public static final String PROP_REDIS_HOST = "redisHost";
    public static final String PROP_REDIS_PORT = "redisPort";
    public static final String PROP_REDIS_ENCODING = "redisEncoding";
    public static final String PROP_CHECK_INTERVAL = "checkInterval";

    private Logger log = LoggerFactory.getLogger(RedisquesConfiguration.class);

    /**
     * Constructor with default values. Use the {@link RedisquesConfigurationBuilder} class
     * for simplyfied custom configuration.
     */
    public RedisquesConfiguration(){
        this(new RedisquesConfigurationBuilder());
    }

    public RedisquesConfiguration(String address, String redisPrefix, String processorAddress, int refreshPeriod,
                                  String redisHost, int redisPort, String redisEncoding) {
        this(address, redisPrefix, processorAddress, refreshPeriod, redisHost, redisPort, redisEncoding, DEFAULT_CHECK_INTERVAL);
    }

    public RedisquesConfiguration(String address, String redisPrefix, String processorAddress, int refreshPeriod,
                                  String redisHost, int redisPort, String redisEncoding, int checkInterval) {
        this.address = address;
        this.redisPrefix = redisPrefix;
        this.processorAddress = processorAddress;
        this.refreshPeriod = refreshPeriod;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisEncoding = redisEncoding;

        if(checkInterval > 0){
            this.checkInterval = checkInterval;
        } else {
            log.warn("Overriden checkInterval of " + checkInterval + "s is not valid. Using default value of " + DEFAULT_CHECK_INTERVAL + "s instead.");
            this.checkInterval = DEFAULT_CHECK_INTERVAL;
        }
    }

    public static RedisquesConfigurationBuilder with(){
        return new RedisquesConfigurationBuilder();
    }

    private RedisquesConfiguration(RedisquesConfigurationBuilder builder){
        this(builder.address, builder.redisPrefix, builder.processorAddress, builder.refreshPeriod,
                builder.redisHost, builder.redisPort, builder.redisEncoding, builder.checkInterval);
    }

    public JsonObject asJsonObject(){
        JsonObject obj = new JsonObject();
        obj.put(PROP_ADDRESS, getAddress());
        obj.put(PROP_REDIS_PREFIX, getRedisPrefix());
        obj.put(PROP_PROCESSOR_ADDRESS, getProcessorAddress());
        obj.put(PROP_REFRESH_PERIOD, getRefreshPeriod());
        obj.put(PROP_REDIS_HOST, getRedisHost());
        obj.put(PROP_REDIS_PORT, getRedisPort());
        obj.put(PROP_REDIS_ENCODING, getRedisEncoding());
        obj.put(PROP_CHECK_INTERVAL, getCheckInterval());
        return obj;
    }

    public static RedisquesConfiguration fromJsonObject(JsonObject json){
        RedisquesConfigurationBuilder builder = RedisquesConfiguration.with();
        if(json.containsKey(PROP_ADDRESS)){
            builder.address(json.getString(PROP_ADDRESS));
        }
        if(json.containsKey(PROP_REDIS_PREFIX)){
            builder.redisPrefix(json.getString(PROP_REDIS_PREFIX));
        }
        if(json.containsKey(PROP_PROCESSOR_ADDRESS)){
            builder.processorAddress(json.getString(PROP_PROCESSOR_ADDRESS));
        }
        if(json.containsKey(PROP_REFRESH_PERIOD)){
            builder.refreshPeriod(json.getInteger(PROP_REFRESH_PERIOD));
        }
        if(json.containsKey(PROP_REDIS_HOST)){
            builder.redisHost(json.getString(PROP_REDIS_HOST));
        }
        if(json.containsKey(PROP_REDIS_PORT)){
            builder.redisPort(json.getInteger(PROP_REDIS_PORT));
        }
        if(json.containsKey(PROP_REDIS_ENCODING)){
            builder.redisEncoding(json.getString(PROP_REDIS_ENCODING));
        }
        if(json.containsKey(PROP_CHECK_INTERVAL)){
            builder.checkInterval(json.getInteger(PROP_CHECK_INTERVAL));
        }
        return builder.build();
    }

    public String getAddress() {
        return address;
    }

    public String getRedisPrefix() {
        return redisPrefix;
    }

    public String getProcessorAddress() {
        return processorAddress;
    }

    public int getRefreshPeriod() {
        return refreshPeriod;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public int getRedisPort() { return redisPort; }

    public int getCheckInterval() { return checkInterval; }

    /**
     * Gets the value for the vertx periodic timer.
     * This value is half of {@link RedisquesConfiguration#getCheckInterval()} in ms plus an additional 500ms.
     * @return the interval for the vertx periodic timer
     */
    public int getCheckIntervalTimerMs() {
        return ((checkInterval * 1000) / 2) + 500;
    }

    public String getRedisEncoding() {
        return redisEncoding;
    }

    @Override
    public String toString() {
        return asJsonObject().toString();
    }

    /**
     * RedisquesConfigurationBuilder class for simplyfied configuration.
     *
     * <pre>Usage:</pre>
     * <pre>
     * RedisquesConfiguration config = RedisquesConfiguration.with()
     *      .redisHost("anotherhost")
     *      .redisPort(1234)
     *      .build();
     * </pre>
     */
    public static class RedisquesConfigurationBuilder {
        private String address;
        private String redisPrefix;
        private String processorAddress;
        private int refreshPeriod;
        private String redisHost;
        private int redisPort;
        private String redisEncoding;
        private int checkInterval;

        public RedisquesConfigurationBuilder(){
            this.address = "redisques";
            this.redisPrefix = "redisques:";
            this.processorAddress = "redisques-processor";
            this.refreshPeriod = 10;
            this.redisHost = "localhost";
            this.redisPort = 6379;
            this.redisEncoding = "UTF-8";
            this.checkInterval = DEFAULT_CHECK_INTERVAL; //60s
        }

        public RedisquesConfigurationBuilder address(String address){
            this.address = address;
            return this;
        }

        public RedisquesConfigurationBuilder redisPrefix(String redisPrefix){
            this.redisPrefix = redisPrefix;
            return this;
        }

        public RedisquesConfigurationBuilder processorAddress(String processorAddress){
            this.processorAddress = processorAddress;
            return this;
        }

        public RedisquesConfigurationBuilder refreshPeriod(int refreshPeriod){
            this.refreshPeriod = refreshPeriod;
            return this;
        }

        public RedisquesConfigurationBuilder redisHost(String redisHost){
            this.redisHost = redisHost;
            return this;
        }

        public RedisquesConfigurationBuilder redisPort(int redisPort){
            this.redisPort = redisPort;
            return this;
        }

        public RedisquesConfigurationBuilder redisEncoding(String redisEncoding){
            this.redisEncoding = redisEncoding;
            return this;
        }

        public RedisquesConfigurationBuilder checkInterval(int checkInterval){
            this.checkInterval = checkInterval;
            return this;
        }

        public RedisquesConfiguration build(){
            return new RedisquesConfiguration(this);
        }
    }

}
