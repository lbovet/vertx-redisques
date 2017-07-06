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
    private int processorTimeout;
    private int processorDelay;
    private boolean httpRequestHandlerEnabled;
    private String httpRequestHandlerPrefix;
    private Integer httpRequestHandlerPort;
    private String httpRequestHandlerUserHeader;

    private static final int DEFAULT_CHECK_INTERVAL = 60; // 60s
    private static final int DEFAULT_PROCESSOR_DELAY = 0;

    public static final String PROP_ADDRESS = "address";
    public static final String PROP_REDIS_PREFIX = "redis-prefix";
    public static final String PROP_PROCESSOR_ADDRESS = "processor-address";
    public static final String PROP_REFRESH_PERIOD = "refresh-period";
    public static final String PROP_REDIS_HOST = "redisHost";
    public static final String PROP_REDIS_PORT = "redisPort";
    public static final String PROP_REDIS_ENCODING = "redisEncoding";
    public static final String PROP_CHECK_INTERVAL = "checkInterval";
    public static final String PROP_PROCESSOR_TIMEOUT = "processorTimeout";
    public static final String PROP_PROCESSOR_DELAY = "processorDelay";
    public static final String PROP_HTTP_REQUEST_HANDLER_ENABLED = "httpRequestHandlerEnabled";
    public static final String PROP_HTTP_REQUEST_HANDLER_PREFIX = "httpRequestHandlerPrefix";
    public static final String PROP_HTTP_REQUEST_HANDLER_PORT = "httpRequestHandlerPort";
    public static final String PROP_HTTP_REQUEST_HANDLER_USER_HEADER = "httpRequestHandlerUserHeader";

    /**
     * Constructor with default values. Use the {@link RedisquesConfigurationBuilder} class
     * for simplified custom configuration.
     */
    public RedisquesConfiguration(){
        this(new RedisquesConfigurationBuilder());
    }

    public RedisquesConfiguration(String address, String redisPrefix, String processorAddress, int refreshPeriod,
                                  String redisHost, int redisPort, String redisEncoding, int processorTimeout) {

        this(address, redisPrefix, processorAddress, refreshPeriod, redisHost, redisPort, redisEncoding, DEFAULT_CHECK_INTERVAL, processorTimeout);
    }

    public RedisquesConfiguration(String address, String redisPrefix, String processorAddress, int refreshPeriod,
                                  String redisHost, int redisPort, String redisEncoding, int checkInterval, int processorTimeout) {
        this(address, redisPrefix, processorAddress, refreshPeriod, redisHost, redisPort, redisEncoding, checkInterval,
                processorTimeout, false, "", null, null);
    }

    public RedisquesConfiguration(String address, String redisPrefix, String processorAddress, int refreshPeriod,
                                  String redisHost, int redisPort, String redisEncoding, int checkInterval,
                                  int processorTimeout, boolean httpRequestHandlerEnabled,
                                  String httpRequestHandlerPrefix, Integer httpRequestHandlerPort,
                                  String httpRequestHandlerUserHeader) {
        this(address, redisPrefix, processorAddress, refreshPeriod, redisHost, redisPort, redisEncoding,
                checkInterval, processorTimeout, DEFAULT_PROCESSOR_DELAY, httpRequestHandlerEnabled, httpRequestHandlerPrefix,
                httpRequestHandlerPort, httpRequestHandlerUserHeader);
    }

    public RedisquesConfiguration(String address, String redisPrefix, String processorAddress, int refreshPeriod,
                                  String redisHost, int redisPort, String redisEncoding, int checkInterval,
                                  int processorTimeout, int processorDelay, boolean httpRequestHandlerEnabled,
                                  String httpRequestHandlerPrefix, Integer httpRequestHandlerPort,
                                  String httpRequestHandlerUserHeader) {
        this.address = address;
        this.redisPrefix = redisPrefix;
        this.processorAddress = processorAddress;
        this.refreshPeriod = refreshPeriod;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisEncoding = redisEncoding;

        Logger log = LoggerFactory.getLogger(RedisquesConfiguration.class);

        if(checkInterval > 0){
            this.checkInterval = checkInterval;
        } else {
            log.warn("Overridden checkInterval of " + checkInterval + "s is not valid. Using default value of " + DEFAULT_CHECK_INTERVAL + "s instead.");
            this.checkInterval = DEFAULT_CHECK_INTERVAL;
        }

        this.processorTimeout = processorTimeout;

        if(processorDelay >= 0){
            this.processorDelay = processorDelay;
        } else {
            log.warn("Overridden processorDelay of " + processorDelay + " is not valid. Using default value of " + DEFAULT_PROCESSOR_DELAY + " instead.");
            this.processorDelay = DEFAULT_PROCESSOR_DELAY;
        }

        this.httpRequestHandlerEnabled = httpRequestHandlerEnabled;
        this.httpRequestHandlerPrefix = httpRequestHandlerPrefix;
        this.httpRequestHandlerPort = httpRequestHandlerPort;
        this.httpRequestHandlerUserHeader = httpRequestHandlerUserHeader;
    }

    public static RedisquesConfigurationBuilder with(){
        return new RedisquesConfigurationBuilder();
    }

    private RedisquesConfiguration(RedisquesConfigurationBuilder builder){
        this(builder.address, builder.redisPrefix, builder.processorAddress, builder.refreshPeriod,
                builder.redisHost, builder.redisPort, builder.redisEncoding, builder.checkInterval,
                builder.processorTimeout, builder.processorDelay, builder.httpRequestHandlerEnabled, builder.httpRequestHandlerPrefix,
                builder.httpRequestHandlerPort, builder.httpRequestHandlerUserHeader);
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
        obj.put(PROP_PROCESSOR_TIMEOUT, getProcessorTimeout());
        obj.put(PROP_PROCESSOR_DELAY, getProcessorDelay());
        obj.put(PROP_HTTP_REQUEST_HANDLER_ENABLED, getHttpRequestHandlerEnabled());
        obj.put(PROP_HTTP_REQUEST_HANDLER_PREFIX, getHttpRequestHandlerPrefix());
        obj.put(PROP_HTTP_REQUEST_HANDLER_PORT, getHttpRequestHandlerPort());
        obj.put(PROP_HTTP_REQUEST_HANDLER_USER_HEADER, getHttpRequestHandlerUserHeader());
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
        if(json.containsKey(PROP_PROCESSOR_TIMEOUT)){
            builder.processorTimeout(json.getInteger(PROP_PROCESSOR_TIMEOUT));
        }
        if(json.containsKey(PROP_PROCESSOR_DELAY)){
            builder.processorDelay(json.getInteger(PROP_PROCESSOR_DELAY));
        }
        if(json.containsKey(PROP_HTTP_REQUEST_HANDLER_ENABLED)){
            builder.httpRequestHandlerEnabled(json.getBoolean(PROP_HTTP_REQUEST_HANDLER_ENABLED));
        }
        if(json.containsKey(PROP_HTTP_REQUEST_HANDLER_PREFIX)){
            builder.httpRequestHandlerPrefix(json.getString(PROP_HTTP_REQUEST_HANDLER_PREFIX));
        }
        if(json.containsKey(PROP_HTTP_REQUEST_HANDLER_PORT)){
            builder.httpRequestHandlerPort(json.getInteger(PROP_HTTP_REQUEST_HANDLER_PORT));
        }
        if(json.containsKey(PROP_HTTP_REQUEST_HANDLER_USER_HEADER)){
            builder.httpRequestHandlerUserHeader(json.getString(PROP_HTTP_REQUEST_HANDLER_USER_HEADER));
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

    public int getProcessorTimeout() { return processorTimeout; }

    public int getProcessorDelay() { return processorDelay; }

    public boolean getHttpRequestHandlerEnabled() { return  httpRequestHandlerEnabled; }

    public String getHttpRequestHandlerPrefix() { return httpRequestHandlerPrefix; }

    public Integer getHttpRequestHandlerPort() { return httpRequestHandlerPort; }

    public String getHttpRequestHandlerUserHeader() { return httpRequestHandlerUserHeader; }

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
     * RedisquesConfigurationBuilder class for simplified configuration.
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
        private int processorTimeout;
        private int processorDelay;
        private boolean httpRequestHandlerEnabled;
        private String httpRequestHandlerPrefix;
        private Integer httpRequestHandlerPort;
        private String httpRequestHandlerUserHeader;

        public RedisquesConfigurationBuilder(){
            this.address = "redisques";
            this.redisPrefix = "redisques:";
            this.processorAddress = "redisques-processor";
            this.refreshPeriod = 10;
            this.redisHost = "localhost";
            this.redisPort = 6379;
            this.redisEncoding = "UTF-8";
            this.checkInterval = DEFAULT_CHECK_INTERVAL; //60s
            this.processorTimeout = 240000;
            this.processorDelay = 0;
            this.httpRequestHandlerEnabled = false;
            this.httpRequestHandlerPrefix = "/queuing";
            this.httpRequestHandlerPort = 7070;
            this.httpRequestHandlerUserHeader = "x-rp-usr";
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

        public RedisquesConfigurationBuilder processorTimeout(int processorTimeout){
            this.processorTimeout = processorTimeout;
            return this;
        }

        public RedisquesConfigurationBuilder processorDelay(int processorDelay){
            this.processorDelay = processorDelay;
            return this;
        }

        public RedisquesConfigurationBuilder httpRequestHandlerEnabled(boolean httpRequestHandlerEnabled){
            this.httpRequestHandlerEnabled = httpRequestHandlerEnabled;
            return this;
        }

        public RedisquesConfigurationBuilder httpRequestHandlerPrefix(String httpRequestHandlerPrefix){
            this.httpRequestHandlerPrefix = httpRequestHandlerPrefix;
            return this;
        }

        public RedisquesConfigurationBuilder httpRequestHandlerPort(Integer httpRequestHandlerPort){
            this.httpRequestHandlerPort = httpRequestHandlerPort;
            return this;
        }

        public RedisquesConfigurationBuilder httpRequestHandlerUserHeader(String httpRequestHandlerUserHeader){
            this.httpRequestHandlerUserHeader = httpRequestHandlerUserHeader;
            return this;
        }

        public RedisquesConfiguration build(){
            return new RedisquesConfiguration(this);
        }
    }

}
