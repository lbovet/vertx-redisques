package org.swisspush.redisques;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.swisspush.redisques.util.RedisquesAPI.OK;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;
import static org.swisspush.redisques.util.RedisquesAPI.buildEnqueueOperation;

/**
 * Created by kammermannf on 31.05.2016.
 */
public class RedisQuesProcessorTest extends AbstractTestCase {

    public static final int NUM_QUEUES = 10;

    @Test
    public void test10Queues(TestContext context) throws Exception {

        final Map<String, MessageDigest> signatures = new HashMap<>();

        vertx.eventBus().consumer("processor-address", new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> message) {
                final String queue = message.body().getString("queue");
                final String payload = message.body().getString("payload");

                if ("STOP".equals(payload)) {
                    message.reply(new JsonObject() {
                        {
                            put("status", "ok");
                        }
                    });
                    vertx.eventBus().send("digest-" + queue, DatatypeConverter.printBase64Binary(signatures.get(queue).digest()));
                } else {
                    MessageDigest signature = signatures.get(queue);
                    if (signature == null) {
                        try {
                            signature = MessageDigest.getInstance("MD5");
                            signatures.put(queue, signature);
                        } catch (NoSuchAlgorithmException e) {
                            throw new RuntimeException();
                        }
                    }
                    signature.update(payload.getBytes());
                }

                vertx.setTimer(new Random().nextLong() % 1 + 1, event -> {
                    log.debug("Processed message " + payload);
                    message.reply(new JsonObject() {
                        {
                            put("status", "ok");
                        }
                    });
                });
            }
        });
        Async async = context.async();
        flushAll();
        assertKeyCount(context, 0);
        for (int i = 0; i < NUM_QUEUES; i++) {
            new Sender(context, async, "queue_" + i).send(null);
        }
    }

    int numMessages = 5;
    AtomicInteger finished = new AtomicInteger();

    /**
     * Sender Class
     */
    class Sender {
        final String queue;
        int messageCount;
        MessageDigest signature;
        TestContext context;
        Async async;

        Sender(TestContext context, Async async, final String queue) {
            this.context = context;
            this.async = async;
            this.queue = queue;
            try {
                signature = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }

            vertx.eventBus().consumer("digest-" + queue, new Handler<Message<String>>() {
                @Override
                public void handle(Message<String> event) {
                    log.info("Received signature for " + queue + ": " + event.body());
                    context.assertEquals(event.body(), DatatypeConverter.printBase64Binary(signature.digest()), "Signatures differ");
                    if (finished.incrementAndGet() == NUM_QUEUES) {
                        async.complete();
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
                signature.update(message.getBytes());
                vertx.eventBus().send("redisques", buildEnqueueOperation(queue, message), new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> event) {
                        if(event.result().body().getString(STATUS).equals(OK)) {
                            send(null);
                        } else {
                            log.error("ERROR sending "+message+" to "+queue);
                            send(message);
                        }
                    }
                });
                messageCount++;
            } else {
                vertx.eventBus().send("redisques", buildEnqueueOperation(queue, "STOP"), new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> reply) {
                        context.assertEquals(OK, reply.result().body().getString(STATUS));
                    }
                });
            }
        }
    }
}
