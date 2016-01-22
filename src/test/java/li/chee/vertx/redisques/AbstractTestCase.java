package li.chee.vertx.redisques;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;
import redis.clients.jedis.Jedis;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

public abstract class AbstractTestCase extends TestVerticle {

    public static final int NUM_QUEUES = 15;
    public static final String OK = "ok";
    public static final String ERROR = "error";
    public static final String VALUE = "value";
    public static final String STATUS = "status";
    public static final String INDEX = "index";
    public static final String BUFFER = "buffer";
    public static final String MESSAGE = "message";
    public static final String PAYLOAD = "payload";
    public static final String REQUESTEDBY = "requestedBy";
    public static final String TIMESTAMP = "timestamp";
    public static final String QUEUENAME = "queuename";
    public static final String QUEUES_PREFIX = "redisques:queues:";
    public static final String REDISQUES_LOCKS = "redisques:locks";

    public EventBus eb;
    Logger log = null;
    private static final String address = "redis-client";
    protected Jedis jedis;

    public enum Operation{
        addItem, deleteItem, deleteLock, getAllLocks, getItem, getLock, enqueue, getListRange, deleteAllQueueItems, putLock, replaceItem
    }

    private void appReady() {
        super.start();
    }

    protected void flushAll(){
        if(jedis != null){
            jedis.flushAll();
        }
    }

    protected void assertKeyCount(int keyCount){
        assertKeyCount("", keyCount);
    }

    protected void assertKeyCount(String prefix, int keyCount){
        assertEquals(keyCount, jedis.keys(prefix+"*").size());
    }

    @BeforeClass
    public static void config() {
        if(!RedisEmbeddedConfiguration.useExternalRedis()) {
            RedisEmbeddedConfiguration.redisServer.start();
        }
    }

    @AfterClass
    public static void stopRedis() {
        if(!RedisEmbeddedConfiguration.useExternalRedis()) {
            RedisEmbeddedConfiguration.redisServer.stop();
        }
    }

    @Override
    public void start() {
        log = container.logger();
        VertxAssert.initialize(vertx);
        eb = vertx.eventBus();

        JsonObject redisConfig = new JsonObject();
        redisConfig.putString("address", address);
        redisConfig.putString("host", "localhost");
        redisConfig.putNumber("port", 6379);
        redisConfig.putString("encoding", "ISO-8859-1");
        container.deployModule("io.vertx~mod-redis~1.1.3", redisConfig,1, new AsyncResultHandler<String>() {
            public void handle(AsyncResult<String> event) {
                log.info("vert.x Deploy - io.vertx~mod-redis~1.1.3 successful: " + event.succeeded());
                JsonObject redisquesConfig = new JsonObject();
                redisquesConfig.putString("redis-address", address);
                redisquesConfig.putString("processor-address", "processor-address");
                container.deployModule(System.getProperty("vertx.modulename"), redisquesConfig,1, new AsyncResultHandler<String>() {
                    @Override
                    public void handle(AsyncResult<String> event) {
                        if (event.failed()) {
                            log.error("Could not load main redis module", event.cause());
                            return;
                        }
                        log.info("vert.x Deploy - " + System.getProperty("vertx.modulename") + " successful: " + event.succeeded());
                        jedis = new Jedis("localhost", 6379, 5000);
                        flushAll();
                        appReady();
                    }
                });
            }
        });
    }

    public JsonObject enqueueOperation(String queueName, String message){
        JsonObject operation = buildOperation(Operation.enqueue);
        operation.putObject(PAYLOAD, new JsonObject().putString(QUEUENAME, queueName));
        operation.putString(MESSAGE, message);
        return operation;
    }

    public JsonObject putLockOperation(String queueName, String requestedBy){
        JsonObject operation = buildOperation(Operation.putLock);
        operation.putObject(PAYLOAD, new JsonObject().putString(QUEUENAME, queueName).putString(REQUESTEDBY, requestedBy));
        return operation;
    }

    public JsonObject buildOperation(Operation operation){
        JsonObject op = new JsonObject();
        op.putString("operation", operation.name());
        return op;
    }

    public JsonObject buildOperation(Operation operation, JsonObject payload){
        JsonObject op = buildOperation(operation);
        op.putObject(PAYLOAD, payload);
        return op;
    }

    @Override
    public void stop() {
        super.stop();
        flushAll();
        jedis.close();
    }

    int numMessages = 50;
    int finished = 0;

    /**
     * Sender Class
     */
    class Sender {
        final String queue;
        int messageCount;
        MessageDigest signature;

        Sender(final String queue) {
            this.queue = queue;
            try {
                signature = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }

            eb.registerHandler("digest-" + queue, new Handler<Message<String>>() {
                @Override
                public void handle(Message<String> event) {
                    System.out.println("Received signature for " + queue + ": " + event.body());
                    assertEquals("Signatures differ", event.body(), DatatypeConverter.printBase64Binary(signature.digest()));
                    finished++;
                    if (finished == NUM_QUEUES) {
                        testComplete();
                    }
                }
            });
        }

        void send(final String m) {
            if (messageCount < numMessages) {
                final String message;
                if(m==null) {
                    message = Double.toString(Math.random());
                } else {
                    message = m;
                }
                System.out.println("SENDING [" + messageCount + "] " + message + " to " + queue+".");
                signature.update(message.getBytes());
                JsonObject operation = buildOperation(Operation.enqueue, new JsonObject().putString(QUEUENAME, queue));
                operation.putString(MESSAGE, message);
                eb.send("redisques", operation, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> event) {
                        if(event.body().getString(STATUS).equals(OK)) {
                            send(null);
                        } else {
                            System.out.println("ERROR sending "+message+" to "+queue);
                            send(message);
                        }
                    }
                });
                messageCount++;
            } else {
                JsonObject operation = buildOperation(Operation.enqueue, new JsonObject().putString(QUEUENAME, queue));
                operation.putString(MESSAGE, "STOP");
                eb.send("redisques", operation, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> reply) {
                        assertEquals(OK, reply.body().getString(STATUS));
                    }
                });
            }
        }
    }
}