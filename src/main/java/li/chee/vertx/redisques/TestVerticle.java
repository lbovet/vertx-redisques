package li.chee.vertx.redisques;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.deploy.Verticle;

public class TestVerticle extends Verticle {

    @Override
    public void start() throws Exception {

        final EventBus eb = vertx.eventBus();

        // Configure redis and reset consumers
        JsonObject redisConfig = new JsonObject();
        redisConfig.putString("address", "redis-client");
        redisConfig.putString("host", "localhost");
        redisConfig.putNumber("port", 6379);
        container.deployModule("de.marx-labs.redis-client-v0.3", redisConfig, 1, new Handler<String>() {
            public void handle(String event) {
                container.deployVerticle(RedisQuesVerticle.class.getName(), new JsonObject(), 4, new Handler<String>() {
                    public void handle(String event) {
                        JsonObject message = new JsonObject();
                        message.putString("operation", "wake");
                        eb.send("redisques", message);
                    }
                });
            }
        });

        // Handle requests to queue
        vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest req) {

                if (req.path.equals("/_reset")) {
                    JsonObject operation = new JsonObject();
                    operation.putString("operation", "reset");
                    eb.send("redisques", operation, new Handler<Message<JsonObject>>() {
                        public void handle(Message<JsonObject> reply) {
                            if ("error".equals(reply.body.getString("status"))) {
                                req.response.statusCode = 500;
                            } else {
                                req.response.statusCode = 202;
                            }
                            req.response.end();
                        }
                    });
                    return;
                }

                if (req.path.equals("/_stop")) {
                    JsonObject operation = new JsonObject();
                    operation.putString("operation", "stop");
                    eb.send("redisques", operation, new Handler<Message<JsonObject>>() {
                        public void handle(Message<JsonObject> reply) {
                            if ("error".equals(reply.body.getString("status"))) {
                                req.response.statusCode = 500;
                            } else {
                                req.response.statusCode = 202;
                            }
                            req.response.end();
                        }
                    });
                    return;
                }

                req.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        // Prepare message to queue
                        final String queue = req.path;
                        final String message = body.toString();
                        JsonObject operation = new JsonObject();
                        operation.putString("operation", "enqueue");
                        operation.putString("queue", queue);
                        operation.putString("message", message);
                        eb.send("redisques", operation, new Handler<Message<JsonObject>>() {
                            public void handle(Message<JsonObject> reply) {
                                if ("error".equals(reply.body.getString("status"))) {
                                    req.response.statusCode = 500;
                                } else {
                                    req.response.statusCode = 202;
                                }
                                req.response.end();
                            }
                        });
                    }
                });
            }
        }).listen(8888);

        // Message processor

        final Logger log = container.getLogger();

        eb.registerHandler("redisques-processor", new Handler<Message<String>>() {
            public void handle(final Message<String> message) {
                final String payload = message.body;
                log.info("Processing message " + payload);
                vertx.setTimer(new Integer(payload), new Handler<Long>() {
                    public void handle(Long event) {
                        log.debug("Processed message " + payload);
                        message.reply();
                    }
                });
            }
        });
    }

}
