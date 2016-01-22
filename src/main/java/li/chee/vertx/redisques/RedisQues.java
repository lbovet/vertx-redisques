package li.chee.vertx.redisques;

import li.chee.vertx.redisques.handler.*;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisQues extends Verticle {

    // State of each queue. Consuming means there is a message being processed.
    private enum QueueState {
        READY, CONSUMING
    }

    private enum RedisCmd {
        exists, keys, expire, del, get, hdel, hget, hmset, hkeys, lindex, llen, lset, lrange, lpop, lrem, rpush, setnx, zadd, zrangebyscore, zremrangebyscore
    }

    // Identifies the consumer
    private String uid = UUID.randomUUID().toString();

    // The queues this verticle is listening to
    private Map<String, QueueState> myQueues = new HashMap<>();

    private Logger log;

    private Handler<Void> stoppedHandler = null;

    // Configuration

    // Address of this redisques. Also used as prefix for consumer broadcast
    // address.
    private String address = "redisques";

    // Address of the redis mod
    private String redisAddress = "redis-client";

    // Prefix for redis keys holding queues and consumers.
    private String redisPrefix = "redisques:";

    // Prefix for queues
    private String queuesPrefix = redisPrefix + "queues:";

    // Prefix for consumers
    private String consumersPrefix = "consumers:";

    private String redisques_locks = "redisques:locks";

    // Address of message processors
    private String processorAddress = "redisques-processor";

    public static final String STATUS = "status";
    public static final String MESSAGE = "message";
    public static final String VALUE = "value";
    public static final String OK = "ok";
    public static final String ARGS = "args";
    public static final String COMMAND = "command";
    public static final String ERROR = "error";
    public static final String PAYLOAD = "payload";
    public static final String QUEUE_NAME = "queuename";
    public static final String REQUESTED_BY = "requestedBy";
    public static final String TIMESTAMP = "timestamp";
    public static final String LIMIT = "limit";
    public static final String INDEX = "index";
    public static final String BUFFER = "buffer";
    public static final String OPERATION = "operation";

    // Consumers periodically refresh their subscription while they are
    // consuming.
    private int refreshPeriod = 10;

    // the time we wait for the processor to answer, before we cancel processing
    private int processorTimeout = 240000;

    private static final int DEFAULT_MAX_QUEUEITEM_COUNT = 49;

    // Handler receiving registration requests when no consumer is registered
    // for a queue.
    private Handler<Message<String>> registrationRequestHandler = new Handler<Message<String>>() {

        public void handle(Message<String> event) {
            final EventBus eb = vertx.eventBus();
            final String queue = event.body();
            log.debug("RedisQues Got registration request for queue " + queue + " from consumer: " + uid);
            // Try to register for this queue
            JsonObject command = buildCommand(RedisCmd.setnx);
            command.putArray(ARGS, new JsonArray().add(redisPrefix + consumersPrefix + queue).add(uid));
            if(log.isTraceEnabled())  {
                log.trace("RedisQues setxn: " + command);
            }
            eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                public void handle(Message<JsonObject> jsonAnswer) {
                    long value = jsonAnswer.body().getLong(VALUE);
                    if(log.isTraceEnabled())  {
                        log.trace("RedisQues setxn result: " + value + " for queue: " + queue);
                    }
                    if (value == 1L) {
                        // I am now the registered consumer for this queue.
                        log.debug("RedisQues Now registered for queue " + queue);
                        myQueues.put(queue, QueueState.READY);
                        consume(queue);
                    } else {
                        log.debug("RedisQues Missed registration for queue " + queue);
                        // Someone else just became the registered consumer. I
                        // give up.
                    }
                }
            });
        }
    };

    @Override
    public void start() {
        log = container.logger();
        final EventBus eb = vertx.eventBus();
        log.info("Started with UID " + uid);

        JsonObject config = container.config();

        address = config.getString("address") != null ? config.getString("address") : address;
        redisAddress = config.getString("redis-address") != null ? config.getString("redis-address") : redisAddress;
        redisPrefix = config.getString("redis-prefix") != null ? config.getString("redis-prefix") : redisPrefix;
        processorAddress = config.getString("processor-address") != null ? config.getString("processor-address") : processorAddress;
        refreshPeriod = config.getNumber("refresh-period") != null ? config.getNumber("refresh-period").intValue() : refreshPeriod;

        // Handles operations
        eb.registerLocalHandler(address, new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> event) {
                String operation = event.body().getString(OPERATION);
                if (log.isTraceEnabled()) {
                    log.trace("RedisQues got operation:" + operation);
                }
                switch (operation) {
                    case "enqueue":
                        updateTimestamp(event.body().getObject(PAYLOAD).getString(QUEUE_NAME), null);
                        redisCall(eb, buildCommand(RedisCmd.rpush, new JsonArray()
                                        .add(queuesPrefix + event.body().getObject(PAYLOAD).getString(QUEUE_NAME))
                                        .add(event.body().getString(MESSAGE))),
                                new Handler<Message<JsonObject>>() {
                                    @Override
                                    public void handle(Message<JsonObject> jsonAnswer) {
                                        Map<String, Object> answer = jsonAnswer.body().toMap();
                                        JsonObject reply = new JsonObject();
                                        if (!OK.equals(answer.get(STATUS))) {
                                            log.error("RedisQues QUEUE_ERROR: Error while enqueing message into queue " + event.body().getObject(PAYLOAD).getString(QUEUE_NAME) + " : " + jsonAnswer.body().getString(MESSAGE));
                                            reply.putString(STATUS, ERROR);
                                            reply.putString(MESSAGE, jsonAnswer.body().getString(MESSAGE));
                                            event.reply(reply);
                                        } else {
                                            log.debug("RedisQues Enqueued message into queue " + event.body().getObject(PAYLOAD).getString(QUEUE_NAME));
                                            notifyConsumer(event.body().getObject(PAYLOAD).getString(QUEUE_NAME));
                                            reply.putString(STATUS, OK);
                                            reply.putString(MESSAGE, "enqueued");
                                            event.reply(reply);
                                        }
                                    }
                                });
                        break;
                    case "check":
                        checkQueues();
                        break;
                    case "reset":
                        resetConsumers();
                        break;
                    case "stop":
                        gracefulStop(new Handler<Void>() {
                            public void handle(Void event) {
                                JsonObject reply = new JsonObject();
                                reply.putString(STATUS, OK);
                            }
                        });
                        break;
                    case "getListRange":
                        redisCall(eb, buildCommand(RedisCmd.lrange, new JsonArray()
                                        .add(queuesPrefix + event.body().getObject(PAYLOAD).getString(QUEUE_NAME))
                                        .add(0)
                                        .add(getMaxQueueItemCountIndex(event.body().getObject(PAYLOAD).getString(LIMIT)))),
                                new GetListRangeHandler(event));
                        break;
                    case "addItem":
                        redisCall(eb, buildCommand(RedisCmd.rpush, new JsonArray()
                                        .add(queuesPrefix + event.body().getObject(PAYLOAD).getString(QUEUE_NAME))
                                        .add(event.body().getObject(PAYLOAD).getString(BUFFER))),
                                new AddItemHandler(event));
                        break;
                    case "deleteItem":
                        redisCall(eb, buildCommand(RedisCmd.lset, new JsonArray()
                                        .add(queuesPrefix + event.body().getObject(PAYLOAD).getString(QUEUE_NAME))
                                        .add(event.body().getObject(PAYLOAD).getInteger(INDEX))
                                        .add("TO_DELETE")),
                                new Handler<Message<JsonObject>>() {
                                    public void handle(Message<JsonObject> reply) {
                                        if (OK.equals(reply.body().getString(STATUS))) {
                                            redisCall(eb, buildCommand(RedisCmd.lrem, new JsonArray()
                                                            .add(queuesPrefix + event.body().getObject(PAYLOAD).getString(QUEUE_NAME))
                                                            .add(0)
                                                            .add("TO_DELETE")),
                                                    new Handler<Message<JsonObject>>() {
                                                        public void handle(Message<JsonObject> reply) {
                                                            event.reply(new JsonObject()
                                                                            .putString(STATUS, OK)
                                                            );
                                                        }
                                                    });
                                        } else {
                                            event.reply(new JsonObject()
                                                            .putString(STATUS, ERROR)
                                            );
                                        }
                                    }
                                });
                        break;
                    case "getItem":
                        redisCall(eb, buildCommand(RedisCmd.lindex, new JsonArray()
                                        .add(queuesPrefix + event.body().getObject(PAYLOAD).getString(QUEUE_NAME))
                                        .add(event.body().getObject(PAYLOAD).getInteger(INDEX))),
                                new GetItemHandler(event));
                        break;
                    case "replaceItem":
                        redisCall(eb, buildCommand(RedisCmd.lset, new JsonArray()
                                        .add(queuesPrefix + event.body().getObject(PAYLOAD).getString(QUEUE_NAME))
                                        .add(event.body().getObject(PAYLOAD).getInteger(INDEX))
                                        .add(event.body().getObject(PAYLOAD).getString(BUFFER))),
                                new ReplaceItemHandler(event));
                        break;
                    case "deleteAllQueueItems":
                        redisCall(eb, buildCommand(RedisCmd.del, new JsonArray()
                                        .add(queuesPrefix + event.body().getObject(PAYLOAD).getString(QUEUE_NAME))),
                                new DeleteAllQueueItems(event));
                        break;
                    case "getAllLocks":
                        redisCall(eb, buildCommand(RedisCmd.hkeys, new JsonArray()
                                        .add(redisques_locks)),
                                new GetAllLocksHandler(event));
                        break;
                    case "putLock":
                        JsonObject lockInfo = extractLockInfo(event.body().getObject(PAYLOAD).getString(REQUESTED_BY));
                        if(lockInfo != null) {
                            redisCall(eb, buildCommand(RedisCmd.hmset, new JsonArray()
                                    .add(redisques_locks)
                                    .add(event.body().getObject(PAYLOAD).getString(QUEUE_NAME)).add(lockInfo.encode())
                            ), new PutLockHandler(event));
                        } else {
                            event.reply(new JsonObject().putString(STATUS, ERROR).putString(MESSAGE, "Property '"+REQUESTED_BY+"' missing"));
                        }
                        break;
                    case "getLock":
                        redisCall(eb, buildCommand(RedisCmd.hget, new JsonArray()
                                        .add(redisques_locks)
                                        .add(event.body().getObject(PAYLOAD).getString(QUEUE_NAME))),
                                new GetLockHandler(event));
                        break;
                    case "deleteLock":
                        redisCall(eb, buildCommand(RedisCmd.hdel, new JsonArray()
                                        .add(redisques_locks)
                                        .add(event.body().getObject(PAYLOAD).getString(QUEUE_NAME))),
                                new DeleteLockHandler(event));
                        break;
                }
            }
        });

        // Handles registration requests
        eb.registerHandler(address + "-consumers", registrationRequestHandler);

        // Handles notifications
        eb.registerHandler(uid, new Handler<Message<String>>() {
            public void handle(Message<String> event) {
                final String queue = event.body();
                log.debug("RedisQues Got notification for queue " + queue);
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
                        JsonObject command = buildCommand(RedisCmd.get);
                        command.putArray(ARGS, new JsonArray().add(redisPrefix + consumersPrefix + queue));
                        if(log.isTraceEnabled())  {
                            log.trace("RedisQues refresh queues get: " + command);
                        }
                        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                            @Override
                            public void handle(Message<JsonObject> event) {
                                String consumer = event.body().getString(VALUE);
                                if (uid.equals(consumer)) {
                                    log.debug("RedisQues Periodic consumer refresh for active queue " + queue);
                                    refreshRegistration(queue, null);
                                    updateTimestamp(queue, null);
                                } else {
                                    log.debug("RedisQues Removing queue " + queue + " from the list");
                                    myQueues.remove(queue);
                                }
                            }
                        });
                    }
                }
            }
        });
    }

    private JsonObject extractLockInfo(String requestedBy) {
        if(requestedBy == null){
            return null;
        }
        JsonObject lockInfo = new JsonObject();
        lockInfo.putString(REQUESTED_BY, requestedBy);
        lockInfo.putNumber(TIMESTAMP,System.currentTimeMillis());
        return lockInfo;
    }

    private void redisCall(EventBus eb, JsonObject command, Handler<Message<JsonObject>> handler) {
        eb.send(redisAddress, command, handler);
    }

    @Override
    public void stop() {
        unregisterConsumers(true);
    }

    private void gracefulStop(final Handler<Void> doneHandler) {
        final EventBus eb = vertx.eventBus();
        eb.unregisterHandler(address + "-consumers", registrationRequestHandler, new AsyncResultHandler<Void>() {
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
        if(log.isTraceEnabled())  {
            log.trace("RedisQues unregister consumers force: " + force);
        }
        final EventBus eb = vertx.eventBus();
        log.debug("RedisQues Unregistering consumers");
        for (final Map.Entry<String, QueueState> entry : myQueues.entrySet()) {
            final String queue = entry.getKey();
            if (force || entry.getValue() == QueueState.READY) {
                if(log.isTraceEnabled())  {
                    log.trace("RedisQues unregister consumers queue: " + queue);
                }
                refreshRegistration(queue, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> event) {
                        // Make sure that I am still the registered consumer
                        JsonObject command = buildCommand(RedisCmd.get);
                        command.putArray(ARGS, new JsonArray().add(redisPrefix + consumersPrefix + queue));
                        if(log.isTraceEnabled())  {
                            log.trace("RedisQues unregister consumers get: " + command);
                        }
                        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                            public void handle(Message<JsonObject> event) {
                                String consumer = event.body().getString(VALUE);
                                if(log.isTraceEnabled())  {
                                    log.trace("RedisQues unregister consumers get result: " + consumer);
                                }
                                if (uid.equals(consumer)) {
                                    log.debug("RedisQues remove consumer: " + uid);
                                    JsonObject command = buildCommand(RedisCmd.del);
                                    command.putArray(ARGS, new JsonArray().add(consumersPrefix + queue));
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
        log.debug("RedisQues Resetting consumers");
        final EventBus eb = vertx.eventBus();
        JsonObject command = buildCommand(RedisCmd.keys, new JsonArray().add(redisPrefix + consumersPrefix + "*"));
        if(log.isTraceEnabled())  {
            log.trace("RedisQues reset consumers keys: " + command);
        }
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                JsonObject command = buildCommand(RedisCmd.del);
                command.putArray(ARGS, event.body().getArray(VALUE));
            }
        });
    }

    private void consume(final String queue) {
        log.debug(" RedisQues Requested to consume queue " + queue);
        final EventBus eb = vertx.eventBus();

        refreshRegistration(queue, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> event) {
                // Make sure that I am still the registered consumer
                JsonObject command = buildCommand(RedisCmd.get);
                command.putArray(ARGS, new JsonArray().add(redisPrefix + consumersPrefix + queue));
                if (log.isTraceEnabled()) {
                    log.trace("RedisQues consume get: " + command);
                }
                eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> event) {
                        String consumer = event.body().getString(VALUE);
                        if (log.isTraceEnabled()) {
                            log.trace("RedisQues refresh registration consumer: " + consumer);
                        }
                        if (uid.equals(consumer)) {
                            QueueState state = myQueues.get(queue);
                            if (log.isTraceEnabled()) {
                                log.trace("RedisQues consumer: " + consumer + " queue: " + queue + " state: " + state);
                            }
                            // Get the next message only once the previous has
                            // been completely processed
                            if (state != QueueState.CONSUMING) {
                                myQueues.put(queue, QueueState.CONSUMING);
                                if (state == null) {
                                    // No previous state was stored. Maybe the
                                    // consumer was restarted
                                    log.warn("Received request to consume from a queue I did not know about: " + queue);
                                }
                                log.debug("RedisQues Starting to consume queue " + queue);
                                readQueue(queue);

                            } else {
                                log.debug("RedisQues Queue " + queue + " is already beeing consumed");
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
        if(log.isTraceEnabled()) {
            log.trace("RedisQues read queue: " + queue);
        }
        final EventBus eb = vertx.eventBus();
        JsonObject command = buildCommand(RedisCmd.lindex, new JsonArray().add(queuesPrefix + queue).add(0));
        if(log.isTraceEnabled())  {
            log.trace("RedisQues read queue lindex: " + command);
        }
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> answer) {
                if (log.isTraceEnabled()) {
                    log.trace("RedisQues read queue lindex result: " + answer.body().getString(VALUE));
                }
                if (answer.body().getString(VALUE) != null) {
                    processMessageWithTimeout(queue, answer.body().getString(VALUE), new Handler<SendResult>() {
                        public void handle(final SendResult sendResult) {
                            if (sendResult.success) {
                                // Remove the processed message from the
                                // queue
                                JsonObject command = buildCommand(RedisCmd.lpop, new JsonArray().add(queuesPrefix + queue));
                                if (log.isTraceEnabled()) {
                                    log.trace("RedisQues read queue lpop: " + command);
                                }
                                eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                                    public void handle(Message<JsonObject> jsonAnswer) {
                                        log.debug("RedisQues Message removed, queue " + queue + " is ready again");
                                        myQueues.put(queue, QueueState.READY);
                                        vertx.cancelTimer(sendResult.timeoutId);
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
                                        JsonObject command = buildCommand(RedisCmd.llen, new JsonArray().add(queuesPrefix + queue));
                                        if (log.isTraceEnabled()) {
                                            log.trace("RedisQues read queue: " + command);
                                        }
                                        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
                                            public void handle(Message<JsonObject> answer) {
                                                if (answer.body().getNumber(VALUE).intValue() > 0) {
                                                    notifyConsumer(queue);
                                                }
                                            }
                                        });
                                    }
                                });
                            } else {
                                // Failed. Message will be kept in queue and
                                // retried at next wakeup.
                                log.debug("RedisQues Processing failed for queue " + queue);
                                myQueues.put(queue, QueueState.READY);
                                vertx.cancelTimer(sendResult.timeoutId);
                            }
                        }
                    });
                } else {
                    // This can happen when requests to consume happen at the same moment the queue is emptied.
                    log.debug("Got a request to consume from empty queue " + queue);
                    myQueues.put(queue, QueueState.READY);
                }
            }
        });
    }

    private void processMessageWithTimeout(final String queue, final String payload, final Handler<SendResult> handler) {
        final EventBus eb = vertx.eventBus();
        JsonObject message = new JsonObject();
        message.putString("queue", queue);
        message.putString(PAYLOAD, payload);
        if(log.isTraceEnabled())  {
            log.trace("RedisQues process message: " + message + " for queue: " + queue + " send it to processor: " + processorAddress);
        }

        // start a timer, which will cancel the processing, if the consumer didn't respond
        final long timeoutId = vertx.setTimer(processorTimeout, new Handler<Long>() {
            public void handle(Long timeoutId) {
                log.debug("RedisQues QUEUE_ERROR: Consumer timeout " + uid + " queue: " + queue);
                handler.handle(new SendResult(false, timeoutId));
            }
        });

        // send the message to the consumer
        eb.send(processorAddress, message, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> reply) {
                handler.handle(new SendResult(reply.body().getString(STATUS).equals(OK), timeoutId));
            }
        });
        updateTimestamp(queue, null);
    }

    private class SendResult {
        public final Boolean success;
        public final Long timeoutId;
        public SendResult(Boolean success, Long timeoutId) {
            this.success = success;
            this.timeoutId = timeoutId;
        }
    }

    private void notifyConsumer(final String queue) {
        log.debug("RedisQues Notifying consumer of queue " + queue);
        final EventBus eb = vertx.eventBus();

        // Find the consumer to notify
        JsonObject command = buildCommand(RedisCmd.get);
        command.putArray(ARGS, new JsonArray().add(redisPrefix + consumersPrefix + queue));
        if(log.isTraceEnabled())  {
            log.trace("RedisQues notify consumer get: " + command);
        }
        eb.send(redisAddress, command, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> jsonAnswer) {
                String consumer = jsonAnswer.body().getString(VALUE);
                if(log.isTraceEnabled())  {
                    log.trace("RedisQues got consumer: " + consumer);
                }
                if (consumer == null) {
                    // No consumer for this queue, let's make a peer become
                    // consumer
                    log.debug("RedisQues Sending registration request for queue " + queue);
                    eb.send(address + "-consumers", queue);
                } else {
                    // Notify the registered consumer
                    log.debug("RedisQues Notifying consumer " + consumer + " to consume queue " + queue);
                    eb.send(consumer, queue);
                }
            }
        });
    }

    private void refreshRegistration(String queue, Handler<Message<JsonObject>> handler) {
        log.debug("RedisQues Refreshing registration of queue " + queue + ", expire at " + (2 * refreshPeriod));
        JsonObject command = buildCommand(RedisCmd.expire, new JsonArray().add(redisPrefix + consumersPrefix + queue).add(2 * refreshPeriod));
        if(log.isTraceEnabled())  {
            log.trace("RedisQues refresh registration: " + command);
        }
        if (handler != null) {
            vertx.eventBus().send(redisAddress, command, handler);
        } else {
            vertx.eventBus().send(redisAddress, command);
        }
    }

    /**
     * Stores the queue name in a sorted set with the current date as score.
     *
     * @param queue
     * @param handler (optional) To get informed when done.
     */
    private void updateTimestamp(final String queue, Handler<Message<JsonObject>> handler) {
        JsonObject command = buildCommand(RedisCmd.zadd);
        command.putArray(ARGS, new JsonArray().add(redisPrefix + "queues")
                .add(System.currentTimeMillis())
                .add(queue));
        if(log.isTraceEnabled())  {
            log.trace("RedisQues update timestamp for queue: " + queue + " to: " + System.currentTimeMillis());
        }
        if(handler!=null) {
            vertx.eventBus().send(redisAddress, command, handler);
        } else {
            vertx.eventBus().send(redisAddress, command);
        }
    }

    /**
     * Notify not-active/not-empty queues to be processed (e.g. after a reboot).
     * Cleanup timestamps of not-active/empty queues.
     * This uses a sorted set of queue names scored by last update timestamp.
     */
    private void checkQueues() {
        log.debug("Checking queues timestamps");
        // List all queues that look inactive (i.e. that have not been updated since 3 periods).
        final long limit = System.currentTimeMillis() - 3*refreshPeriod*1000;
        JsonObject command = buildCommand(RedisCmd.zrangebyscore);
        command.putArray(ARGS, new JsonArray().add(redisPrefix + "queues")
                .add("-inf")
                .add(limit));
        vertx.eventBus().send(redisAddress, command, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> answer) {
                JsonArray queues = answer.body().getArray(VALUE);
                final AtomicInteger counter = new AtomicInteger(queues.size());
                if(log.isTraceEnabled())  {
                    log.trace("RedisQues update queues: " + counter);
                }
                for(Object queueObject : queues) {
                    // Check if the inactive queue is not empty (i.e. the key exists)
                    final String queue = (String)queueObject;
                    JsonObject command = buildCommand(RedisCmd.exists, new JsonArray().add(queuesPrefix + queue));
                    if(log.isTraceEnabled())  {
                        log.trace("RedisQues update queue: " + queue);
                    }
                    vertx.eventBus().send(redisAddress, command, new Handler<Message<JsonObject>>() {
                        public void handle(Message<JsonObject> answer) {
                            if(answer.body().getLong(VALUE) == 1) {
                                log.debug("Updating queue timestamp " + queue);
                                // If not empty, update the queue timestamp to keep it in the sorted set.
                                updateTimestamp(queue, new Handler<Message<JsonObject>>() {
                                    @Override
                                    public void handle(Message<JsonObject> message) {
                                        // Ensure we clean the old queues after having updated all timestamps
                                        if(counter.decrementAndGet()==0) {
                                            removeOldQueues(limit);
                                        }
                                    }
                                });
                                // Make sure its TTL is correctly set (replaces the previous orphan detection mechanism).
                                refreshRegistration(queue, null);
                                // And trigger its consumer.
                                notifyConsumer(queue);
                            } else {
                                // Ensure we clean the old queues also in the case of empty queue.
                                if(log.isTraceEnabled())  {
                                    log.trace("RedisQues remove old queue: " + queue);
                                }
                                if(counter.decrementAndGet()==0) {
                                    removeOldQueues(limit);
                                }
                            }
                        }
                    });
                }
            }
        });
    }

    /**
     * Remove queues from the sorted set that are timestamped before a limit time.
     *
     * @param limit
     */
    private void removeOldQueues(long limit) {
        log.debug("Cleaning old queues");
        JsonObject command = buildCommand(RedisCmd.zremrangebyscore);
        command.putArray(ARGS, new JsonArray().add(redisPrefix + "queues")
                .add("-inf")
                .add(limit));
        vertx.eventBus().send(redisAddress, command);
    }

    private int getMaxQueueItemCountIndex(String limit){
        int defaultMaxIndex = DEFAULT_MAX_QUEUEITEM_COUNT;
        if(limit != null){
            try {
                int maxIndex = Integer.parseInt(limit) - 1;
                if(maxIndex >= 0){
                    defaultMaxIndex = maxIndex;
                }
                log.info("use limit parameter " + maxIndex);
            } catch (NumberFormatException ex){
                log.warn("Invalid limit parameter '"+limit+"' configured for max queue item count. Using default " + DEFAULT_MAX_QUEUEITEM_COUNT);
            }
        }
        return defaultMaxIndex;
    }

    private JsonObject buildCommand(RedisCmd cmd){
        JsonObject command = new JsonObject();
        command.putString(COMMAND, cmd.name());
        return command;
    }

    private JsonObject buildCommand (RedisCmd cmd, JsonArray args) {
        JsonObject command = buildCommand(cmd);
        command.putArray(ARGS, args);
        return command;
    }
}