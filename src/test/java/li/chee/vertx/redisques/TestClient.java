package li.chee.vertx.redisques;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.xml.bind.DatatypeConverter;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.framework.TestClientBase;

public class TestClient extends TestClientBase {

    private EventBus eb;

    @Override
    public void start() {
        super.start();
        eb = vertx.eventBus();
        JsonObject redisConfig = new JsonObject();
        redisConfig.putString("address", "redis-client");
        redisConfig.putString("host", "localhost");
        redisConfig.putNumber("port", 6379);
        container.deployModule("de.marx-labs.redis-client-v0.4", redisConfig, 2, new Handler<String>() {
            public void handle(String res) {
                container.deployModule("li.chee.redisques-v0.4", new JsonObject(), 4, new Handler<String>() {
                    public void handle(String event) {
                        // Clean
                        JsonObject command = new JsonObject();
                        command.putString("command", "keys");
                        command.putString("pattern", "redisques:*");
                        eb.send("redis-client", command, new Handler<Message<JsonObject>>() {
                            @Override
                            public void handle(Message<JsonObject> event) {
                                final JsonArray keys = event.body.getArray("value");
                                JsonObject command = new JsonObject();
                                command.putString("command", "del");                                
                                command.putArray("keys", keys);
                                eb.send("redis-client", command, new Handler<Message<JsonObject>>() {
                                    public void handle(Message<JsonObject> event) {
                                        System.out.println("deleted "+Arrays.asList(keys.toArray()));
                                        tu.appReady();
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

    public void testSimple() throws Exception {
        final JsonObject operation = new JsonObject();
        operation.putString("operation", "enqueue");
        operation.putString("queue", "queue1");
        operation.putString("message", "hello");

        eb.registerHandler("digest-queue1", new Handler<Message<String>>() {
            @Override
            public void handle(Message<String> event) {
                tu.testComplete();
            }
        });

        eb.send("redisques", operation, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> reply) {
                tu.azzert("ok".equals(reply.body.getString("status")), "Error: " + reply.body.getString("status"));

                operation.putString("message", "STOP");
                eb.send("redisques", operation, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> reply) {
                        tu.azzert("ok".equals(reply.body.getString("status")), "Error: " + reply.body.getString("status"));
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
                    System.out.println("Received signature for " + queue + ": " + event.body);
                    tu.azzert(event.body.equals(DatatypeConverter.printBase64Binary(signature.digest())), "Signatures differ");
                    finished++;
                    if (finished == numQueues) {
                        tu.testComplete();
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
                System.out.println("SENDING [" + messageCount + "] " + message + " to " + queue);
                signature.update(message.getBytes());
                JsonObject operation = new JsonObject();
                operation.putString("operation", "enqueue");
                operation.putString("queue", queue);
                operation.putString("message", message);
                eb.send("redisques", operation, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> event) {
                        if(event.body.getString("status").equals("ok")) {
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
                        tu.azzert("ok".equals(reply.body.getString("status")), "Error: " + reply.body.getString("status"));
                    }
                });
            }
        }
    }

    public void testMore() throws Exception {
        for (int i = 0; i < numQueues; i++) {
            new Sender("queue" + i).send(null);
        }
    }
}
