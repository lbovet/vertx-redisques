package li.chee.vertx.redisques;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import javax.xml.bind.DatatypeConverter;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import static org.vertx.testtools.VertxAssert.*;

public class IntegrationTest extends TestVerticle {
    private EventBus eb;

    @Override
    public void start() {
        initialize();
        eb = vertx.eventBus();
        JsonObject redisConfig = new JsonObject();
        redisConfig.putString("address", "redis-client");
        redisConfig.putString("host", "localhost");
        redisConfig.putNumber("port", 6379);
        container.deployModule("io.vertx~mod-redis~1.1.3", redisConfig, 2, new AsyncResultHandler<String>() {
            public void handle(AsyncResult<String> event) {
                System.out.println(event.failed());
                container.deployModule(System.getProperty("vertx.modulename"), new JsonObject(), 4, new AsyncResultHandler<String>() {
                    public void handle(AsyncResult<String> event) {

                        final Map<String, Integer> counters = new HashMap<>();
                        final Map<String, MessageDigest> signatures = new HashMap<String, MessageDigest>();

                        eb.registerHandler("redisques-processor", new Handler<Message<JsonObject>>() {
                            public void handle(final Message<JsonObject> message) {
                                final String queue = message.body().getString("queue");
                                final String payload = message.body().getString("payload");

                                if(!counters.containsKey(queue)) {
                                    counters.put(queue, 0);
                                }

                                System.out.println("GOT ["+counters.get(queue)+"] " + payload + " in " + queue+".");

                                counters.put(queue, counters.get(queue)+1);

                                if ("STOP".equals(payload)) {
                                    message.reply(new JsonObject() {
                                        {
                                            putString("status", "ok");
                                        }
                                    });
                                    eb.send("digest-" + queue, DatatypeConverter.printBase64Binary(signatures.get(queue).digest()));
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

                                container.logger().info("Processing message " + payload);
                                vertx.setTimer(new Random().nextLong() % 1 +1, new Handler<Long>() {
                                    public void handle(Long event) {
                                        container.logger().debug("Processed message " + payload);
                                        message.reply(new JsonObject() {
                                            {
                                                putString("status", "ok");
                                            }
                                        });
                                    }
                                });
                            }
                        });


                        // Clean
                        JsonObject command = new JsonObject();
                        command.putString("command", "keys");
                        command.putArray("args", new JsonArray().add("redisques:*"));
                        eb.send("redis-client", command, new Handler<Message<JsonObject>>() {
                            @Override
                            public void handle(Message<JsonObject> event) {
                                final JsonArray keys = event.body().getArray("value");
                                JsonObject command = new JsonObject();
                                command.putString("command", "del");
                                command.putArray("args", keys);
                                eb.send("redis-client", command, new Handler<Message<JsonObject>>() {
                                    public void handle(Message<JsonObject> event) {
                                        System.out.println("deleted "+Arrays.asList(keys.toArray()));
                                        startTests();
                                    }
                                });
                            }
                        });
                    }
                });
            }
        });
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Test
    public void testSimple() throws Exception {
        final JsonObject operation = new JsonObject();
        operation.putString("operation", "enqueue");
        operation.putString("queue", "queue1");
        operation.putString("message", "hello");

        eb.registerHandler("digest-queue1", new Handler<Message<String>>() {
            @Override
            public void handle(Message<String> event) {
                testComplete();
            }
        });

        eb.send("redisques", operation, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> reply) {
                assertEquals("ok", reply.body().getString("status"));

                operation.putString("message", "STOP");
                eb.send("redisques", operation, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> reply) {
                        assertEquals("ok", reply.body().getString("status"));
                    }
                });
            }
        });
    }

    int numQueues = 15;
    int numMessages = 50;
    int finished = 0;

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
                    if (finished == numQueues) {
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
                JsonObject operation = new JsonObject();
                operation.putString("operation", "enqueue");
                operation.putString("queue", queue);
                operation.putString("message", message);
                eb.send("redisques", operation, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> event) {
                        if(event.body().getString("status").equals("ok")) {
                            send(null);
                        } else {
                            System.out.println("ERROR sending "+message+" to "+queue);
                            send(message);
                        }
                    }
                });
                messageCount++;
            } else {
                JsonObject operation = new JsonObject();
                operation.putString("operation", "enqueue");
                operation.putString("queue", queue);
                operation.putString("message", "STOP");
                eb.send("redisques", operation, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> reply) {
                        assertEquals("ok", reply.body().getString("status"));
                    }
                });
            }
        }
    }

    @Test
    public void testMore() throws Exception {
        for (int i = 0; i < numQueues; i++) {
            new Sender("queue" + i).send(null);
        }
    }
}
