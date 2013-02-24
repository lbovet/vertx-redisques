package li.chee.vertx.redisques;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;

public class RedisQues extends BusModBase {

    // State of each queue. Consuming means there is a message being processed.
    private enum QueueState {
        READY, CONSUMING
    };

    // Identifies the consumer
    private String uid = UUID.randomUUID().toString();

    // The queues this verticle is listening to
    private Map<String, QueueState> myQueues = new HashMap<>();

    private Logger log;

    private Handler<Void> stoppedHandler = null;

    // Configuration

    // Address of this redisques. Also used as prefix for consumer broadcast address.
    private String address = "redisques";
    
    // Address of the redis mod
    private String redisAddress = "redis-client";

    // Prefix for redis keys holding queues and consumers.
    private String redisPrefix = "redisques:";

    // Prefix for queues
    private String queuesPrefix = "queues:";

    // Prefix for consumers
    private String consumersPrefix = "consumers:";

    // Address of message processors
    private String processorAddress = "redisques-processor";

    // Consumers periodically refresh their subscription while they are
    // consuming.
    private int refreshPeriod = 10;

    // Handler receiving registration requests when no consumer is registered
    // for a queue.
    private Handler<Message<String>> registrationRequestHandler = new Handler<Message<String>>() {

        public void handle(Message<String> event) {
            final EventBus eb = vertx.eventBus();
            final String queue = event.body;
            log.debug("Got registration request for queue " + queue);
            // Try to register for this queue
            JsonObject command = new JsonObject();
            command.putString("command", "setnx");
            command.putString("key", redisPrefix + consumersPrefix + queue);
            command.putString("value", uid);
            eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                public void handle(Message<JsonObject> jsonAnswer) {
                    Object value = jsonAnswer.body.getField("value");
                    
                    if ((value instanceof Number && value.equals(1)) || value instanceof Boolean && (Boolean)value) {
                        // I am now the registered consumer for this queue.
                        log.debug("Now registered for queue " + queue);
                        myQueues.put(queue, QueueState.READY);
                        consume(queue);
                    } else {
                        log.debug("Missed registration for queue " + queue);
                        // Someone else just became the registered consumer. I
                        // give up.
                    }
                }
            });
        }
    };

    @Override
    public void start() {
        log = container.getLogger();
        final EventBus eb = vertx.eventBus();
        log.info("Started with UID " + uid);

        JsonObject config = container.getConfig();

        address = config.getString("address") != null ? config.getString("address") : address;
        redisAddress = config.getString("redis-address") != null ? config.getString("redis-address") : redisAddress;
        redisPrefix = config.getString("redis-prefix") != null ? config.getString("redis-prefix") : redisPrefix;
        processorAddress = config.getString("processor-address") != null ? config.getString("processor-address") : processorAddress;
        refreshPeriod = config.getNumber("refresh-period") != null ? config.getNumber("refresh-period").intValue() : refreshPeriod;

        // Handles operations
        eb.registerHandler(address, new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> event) {
                String operation = event.body.getString("operation");
                switch (operation) {
                case "enqueue":
                    final String queue = event.body.getString("queue");
                    final String message = event.body.getString("message");
                    JsonObject command = new JsonObject();
                    command.putString("command", "rpush");
                    command.putString("key", redisPrefix + queuesPrefix + queue);
                    command.putArray("values", new JsonArray(new String[] { message }));
                    command.putString("value", message);
                    // Send it to the queue
                    eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                        public void handle(Message<JsonObject> jsonAnswer) {
                            Map<String, Object> answer = jsonAnswer.body.toMap();
                            JsonObject reply = new JsonObject();
                            if (!"ok".equals(answer.get("status"))) {
                                log.error("Error while enqueing message into queue " + queue + " : " + jsonAnswer.body.getString("message"));
                                reply.putString("status", "error");
                                reply.putString("message", jsonAnswer.body.getString("message"));
                                event.reply(reply);
                            } else {
                                log.debug("Enqueued message into queue " + queue);
                                notifyConsumer(queue);
                                reply.putString("status", "ok");
                                reply.putString("message", "enqueued");
                                event.reply(reply);
                            }
                        }
                    });
                    break;
                case "reset":
                    resetConsumers();
                    break;
                case "wake":
                    wakeConsumers();
                    break;
                case "stop":
                    gracefulStop(new Handler<Void>() {
                        public void handle(Void event) {
                            JsonObject reply = new JsonObject();
                            reply.putString("status", "ok");
                        }
                    });
                    break;
                }

            }

        });

        // Handles registration requests
        eb.registerHandler(address+"-consumers", registrationRequestHandler);

        // Handles notifications
        eb.registerHandler(uid, new Handler<Message<String>>() {
            public void handle(Message<String> event) {
                final String queue = event.body;
                log.debug("Got notification for queue " + queue);
                consume(queue);
            }
        });

        // Periodic refresh of my registrations on active queues.
        vertx.setPeriodic(refreshPeriod * 1000, new Handler<Long>() {
            public void handle(Long event) {
                for (final Map.Entry<String, QueueState> entry : myQueues.entrySet()) {
                    if (entry.getValue() == QueueState.CONSUMING) {
                        final String queue = entry.getKey();
                        // Check if I am still the registered consumer
                        JsonObject command = new JsonObject();
                        command.putString("command", "get");
                        command.putString("key", redisPrefix + consumersPrefix + queue);
                        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                            @Override
                            public void handle(Message<JsonObject> event) {
                                String consumer = event.body.getString("value");
                                if (uid.equals(consumer)) {
                                    log.debug("Periodic consumer refresh for active queue " + queue);
                                    refreshRegistration(queue, null);
                                } else {
                                    log.debug("Removing queue " + queue + " from the list");
                                    myQueues.remove(queue);
                                }
                            }
                        });
                    }
                }
            }
        });

        // Periodic wake up of all consumers
        vertx.setPeriodic(3 * refreshPeriod * 1000, new Handler<Long>() {
            public void handle(Long event) {
                wakeConsumers();
            }
        });

    }

    @Override
    public void stop() throws Exception {
        unregisterConsumers(true);
    }

    private void gracefulStop(final Handler<Void> doneHandler) {
        final EventBus eb = vertx.eventBus();
        eb.unregisterHandler(address+"-consumers", registrationRequestHandler, new AsyncResultHandler<Void>() {
            public void handle(AsyncResult<Void> event) {
                eb.unregisterHandler(uid, new Handler<Message<Void>>() {
                    public void handle(Message<Void> message) {
                        unregisterConsumers(false);
                        stoppedHandler = doneHandler;
                        if (myQueues.keySet().isEmpty()) {
                            doneHandler.handle(null);
                        }
                    }
                });
            }
        });
    }

    private void unregisterConsumers(boolean force) {
        final EventBus eb = vertx.eventBus();
        log.debug("Unregistering consumers");
        for (final Map.Entry<String, QueueState> entry : myQueues.entrySet()) {
            final String queue = entry.getKey();
            if (force || entry.getValue() == QueueState.READY) {
                refreshRegistration(queue, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> event) {
                        // Make sure that I am still the registered consumer
                        JsonObject command = new JsonObject();
                        command.putString("command", "get");
                        command.putString("key", redisPrefix + consumersPrefix + queue);
                        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                            public void handle(Message<JsonObject> event) {
                                String consumer = event.body.getString("value");
                                if (uid.equals(consumer)) {
                                    JsonObject command = new JsonObject();
                                    command.putString("command", "del");
                                    command.putString("key", consumersPrefix + queue);
                                    myQueues.remove(queue);
                                }
                            }
                        });
                    }
                });
            }
        }
    }

    /**
     * Caution: this may in some corner case violate the ordering for one
     * message.
     */
    private void resetConsumers() {
        log.debug("Resetting consumers");
        final EventBus eb = vertx.eventBus();
        JsonObject command = new JsonObject();
        command.putString("command", "keys");
        command.putString("pattern", redisPrefix + consumersPrefix + "*");
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                JsonObject command = new JsonObject();
                command.putString("command", "del");
                command.putArray("keys", event.body.getArray("value"));
                eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> event) {
                        wakeConsumers();
                    }
                });
            }
        });
    }

    /**
     * Check non-empty queues and notify their consumers.
     */
    private void wakeConsumers() {
        final EventBus eb = vertx.eventBus();
        JsonObject command = new JsonObject();
        command.putString("command", "keys");
        command.putString("pattern", redisPrefix + queuesPrefix + "*");
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                JsonArray list = event.body.getArray("value");
                if (list.size() > 0) {
                    log.debug("Waking up consumers for " + list.size() + " queue(s)");
                }
                for (Object queue : list) {
                    notifyConsumer(queue.toString().substring((redisPrefix + queuesPrefix).length()));
                }
            }
        });
    }

    private void consume(final String queue) {
        log.debug("Requested to consume queue " + queue);
        final EventBus eb = vertx.eventBus();

        refreshRegistration(queue, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> event) {
                // Make sure that I am still the registered consumer
                JsonObject command = new JsonObject();
                command.putString("command", "get");
                command.putString("key", redisPrefix + consumersPrefix + queue);
                eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> event) {
                        String consumer = event.body.getString("value");
                        if (uid.equals(consumer)) {
                            QueueState state = myQueues.get(queue);
                            // Get the next message only once the previous has
                            // been completely processed
                            if (state != QueueState.CONSUMING) {
                                myQueues.put(queue, QueueState.CONSUMING);
                                if (state == null) {
                                    // No previous state was stored. Maybe the
                                    // consumer was restarted
                                    log.warn("Received request to consume from a queue I did not know about: " + queue);
                                }
                                log.debug("Starting to consume queue " + queue);
                                readQueue(queue);

                            } else {
                                log.debug("Queue " + queue + " is already beeing consumed");
                            }
                        } else {
                            // Somehow registration changed. Let's renotify.
                            log.warn("Registration for queue " + queue + " has changed to " + consumer);
                            myQueues.remove(queue);
                            notifyConsumer(queue);
                        }
                    }

                });
            }
        });
    }

    private void readQueue(final String queue) {
        final EventBus eb = vertx.eventBus();
        JsonObject command = new JsonObject();
        command.putString("command", "lindex");
        command.putString("key", redisPrefix + queuesPrefix + queue);
        command.putNumber("index", 0);
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> answer) {
                if (answer.body.getString("value") != null) {
                    if (myQueues.get(queue) != QueueState.CONSUMING) {
                        myQueues.put(queue, QueueState.CONSUMING);
                        processMessage(queue, answer.body.getString("value"), new Handler<Boolean>() {
                            public void handle(Boolean success) {
                                if (success) {
                                    // Remove the processed message from the
                                    // queue
                                    JsonObject command = new JsonObject();
                                    command.putString("command", "lpop");
                                    command.putString("key", redisPrefix + queuesPrefix + queue);
                                    eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                                        public void handle(Message<JsonObject> jsonAnswer) {
                                            log.debug("Message removed, queue " + queue + " is ready again");
                                            myQueues.put(queue, QueueState.READY);
                                            // Notify that we are stopped in
                                            // case it
                                            // was the last active consumer
                                            if (stoppedHandler != null) {
                                                unregisterConsumers(false);
                                                if (myQueues.isEmpty()) {
                                                    stoppedHandler.handle(null);
                                                }
                                            }
                                            // Issue notification to consume
                                            // next
                                            // message if any
                                            JsonObject command = new JsonObject();
                                            command.putString("command", "llen");
                                            command.putString("key", redisPrefix + queuesPrefix + queue);
                                            eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                                                public void handle(Message<JsonObject> answer) {
                                                    if (answer.body.getNumber("value").intValue() > 0) {
                                                        notifyConsumer(queue);
                                                    }
                                                }
                                            });
                                        }
                                    });
                                } else {
                                    // Failed. Message will be kept in queue and retried at next wakeup.
                                    log.debug("Processing failed for queue "+queue);
                                }
                            }
                        });
                    }
                } else {
                    // There was nothing in the queue, weird.
                    log.warn("Got a request to consume from empty queue " + queue);
                }
            }
        });
    }

    private void processMessage(final String queue, final String payload, final Handler<Boolean> handler) {
        final EventBus eb = vertx.eventBus();
        JsonObject message = new JsonObject();
        message.putString("queue", queue);
        message.putString("payload", payload);
        eb.send(processorAddress, message, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> reply) {
                handler.handle(reply.body.getString("status").equals("ok"));
            }
        });
    }

    private void notifyConsumer(final String queue) {
        log.debug("Notifying consumer of queue " + queue);
        final EventBus eb = vertx.eventBus();

        // Find the consumer to notify
        JsonObject command = new JsonObject();
        command.putString("command", "get");
        command.putString("key", redisPrefix + consumersPrefix + queue);
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> jsonAnswer) {
                String consumer = jsonAnswer.body.getString("value");
                if (consumer == null) {
                    // No consumer for this queue, let's make a peer become
                    // consumer
                    log.debug("Sending registration request for queue " + queue);
                    eb.send(address+"-consumers", queue);
                } else {
                    // Notify the registered consumer
                    log.debug("Notifying consumer " + consumer + " to consume queue " + queue);
                    eb.send(consumer, queue);
                }
            }
        });
    }

    private void refreshRegistration(String queue, Handler<Message<JsonObject>> handler) {
        log.debug("Refreshing registration of queue " + queue);
        JsonObject command = new JsonObject();
        command.putString("command", "expire");
        command.putString("key", redisPrefix + consumersPrefix + queue);
        command.putNumber("seconds", 2 * refreshPeriod);
        if (handler != null) {
            vertx.eventBus().send(redisAddress, command, handler);
        } else {
            vertx.eventBus().send(redisAddress, command);
        }
    }
}
