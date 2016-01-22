package li.chee.vertx.redisques;

import org.junit.Test;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import static org.hamcrest.Matchers.equalTo;
import static org.vertx.testtools.VertxAssert.*;

/**
 * Class RedisQueueBrowserTest.
 *
 * @author baldim
 */
public class RedisQuesTest extends AbstractTestCase {

    private String address = "redisques";

    @Test
    public void enqueueWithQueueProcessor() throws Exception {
        flushAll();
        assertKeyCount(0);
        final JsonObject operation = enqueueOperation("queue1", "hello");

        eb.registerHandler("digest-queue1", new Handler<Message<String>>() {
            @Override
            public void handle(Message<String> event) {
            }
        });

        eventBusSend(operation, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> reply) {
                assertEquals(OK, reply.body().getString(STATUS));
                operation.putString("message", "STOP");
                eventBusSend(operation, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> reply) {
                        assertEquals(OK, reply.body().getString(STATUS));
                        testComplete();
                    }
                });
            }
        });
    }

    @Test
    public void testMore() throws Exception {
        flushAll();
        assertKeyCount(0);
        for (int i = 0; i < NUM_QUEUES; i++) {
            new Sender("queue" + i).send(null);
        }
        testComplete();
    }

    @Test
    public void enqueue() {
        flushAll();
        eventBusSend(enqueueOperation("queueEnqueue", "helloEnqueue"), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals(OK, message.body().getString(STATUS));
                assertEquals("helloEnqueue", jedis.lindex("redisques:queues:queueEnqueue", 0));
                assertKeyCount(QUEUES_PREFIX, 1);
                testComplete();
            }
        });
    }

    @Test
    public void getListRange() {
        flushAll();
        eventBusSend(buildOperation(Operation.getListRange, new JsonObject().putString(QUEUENAME, "queue1")), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals(OK, message.body().getString(STATUS));
                assertThat(message.body().getArray(VALUE).size(), equalTo(0));
                eventBusSend(enqueueOperation("queue1", "a_queue_item"), new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> message) {
                        assertEquals(OK, message.body().getString(STATUS));
                        assertKeyCount(QUEUES_PREFIX, 1);
                        eventBusSend(buildOperation(Operation.getListRange, new JsonObject().putString(QUEUENAME, "queue1")), new Handler<Message<JsonObject>>() {
                            @Override
                            public void handle(Message<JsonObject> event) {
                                assertEquals(OK, event.body().getString(STATUS));
                                assertThat(event.body().getArray(VALUE).size(), equalTo(1));
                                assertEquals("a_queue_item", event.body().getArray(VALUE).get(0));
                                testComplete();
                            }
                        });
                    }
                });
            }
        });
    }

    @Test
    public void deleteAllQueueItems() {
        flushAll();
        assertKeyCount(QUEUES_PREFIX, 0);
        final String queue = "queue1";
        eventBusSend(enqueueOperation(queue, "some_val"), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals(OK, message.body().getString(STATUS));
                assertKeyCount(QUEUES_PREFIX, 1);
                eventBusSend(buildOperation(Operation.deleteAllQueueItems, new JsonObject().putString(QUEUENAME, queue)), new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> message) {
                        assertEquals(OK, message.body().getString(STATUS));
                        assertKeyCount(QUEUES_PREFIX, 0);
                        testComplete();
                    }
                });
            }
        });
    }

    @Test
    public void addItem() {
        flushAll();
        assertKeyCount(QUEUES_PREFIX, 0);
        eventBusSend(buildOperation(Operation.addItem, new JsonObject().putString(QUEUENAME, "queue2").putString(BUFFER, "fooBar")), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals(OK, message.body().getString(STATUS));
                assertKeyCount(QUEUES_PREFIX, 1);
                assertEquals("fooBar", jedis.lindex(QUEUES_PREFIX + "queue2", 0));
                testComplete();
            }
        });
    }

    @Test
    public void getItem() {
        flushAll();
        assertKeyCount(QUEUES_PREFIX, 0);
        eventBusSend(buildOperation(Operation.getItem, new JsonObject().putString(QUEUENAME, "queue1").putNumber(INDEX, 0)), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals(ERROR, message.body().getString(STATUS));
                assertNull(message.body().getString(VALUE));
                eventBusSend(buildOperation(Operation.addItem, new JsonObject().putString(QUEUENAME, "queue1").putString(BUFFER, "fooBar")), new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> message) {
                        assertEquals(OK, message.body().getString(STATUS));
                        assertKeyCount(QUEUES_PREFIX, 1);
                        assertEquals("fooBar", jedis.lindex(QUEUES_PREFIX + "queue1", 0));
                        eventBusSend(buildOperation(Operation.getItem, new JsonObject().putString(QUEUENAME, "queue1").putNumber(INDEX, 0)), new Handler<Message<JsonObject>>() {
                            @Override
                            public void handle(Message<JsonObject> event) {
                                assertEquals(OK, event.body().getString(STATUS));
                                assertEquals("fooBar", event.body().getString("value"));
                                testComplete();
                            }
                        });
                    }
                });
            }
        });
    }

    @Test
    public void replaceItem() {
        flushAll();
        eventBusSend(buildOperation(Operation.addItem, new JsonObject().putString(QUEUENAME, "queue1").putString(BUFFER, "foo")), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals(OK, message.body().getString(STATUS));
                assertKeyCount(QUEUES_PREFIX, 1);
                assertEquals("foo", jedis.lindex(QUEUES_PREFIX + "queue1", 0));
                eventBusSend(buildOperation(Operation.replaceItem, new JsonObject().putString(QUEUENAME, "queue1").putString(BUFFER, "bar").putNumber(INDEX, 0)), new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> message) {
                        assertEquals(OK, message.body().getString(STATUS));
                        assertEquals("bar", jedis.lindex(QUEUES_PREFIX + "queue1", 0));
                        testComplete();
                    }
                });
            }
        });
    }

    @Test
    public void deleteItem() {
        flushAll();
        assertKeyCount(QUEUES_PREFIX + "queue1", 0);
        eventBusSend(buildOperation(Operation.deleteItem, new JsonObject().putString(QUEUENAME, "queue1").putNumber(INDEX, 0)), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals(ERROR, message.body().getString(STATUS));
                eventBusSend(buildOperation(Operation.addItem, new JsonObject().putString(QUEUENAME, "queue1").putString(BUFFER, "foo")), new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> message) {
                        assertEquals(OK, message.body().getString(STATUS));
                        assertKeyCount(QUEUES_PREFIX + "queue1", 1);
                        eventBusSend(buildOperation(Operation.deleteItem, new JsonObject().putString(QUEUENAME, "queue1").putNumber(INDEX, 0)), new Handler<Message<JsonObject>>() {
                            @Override
                            public void handle(Message<JsonObject> message) {
                                assertEquals(OK, message.body().getString(STATUS));
                                assertKeyCount(QUEUES_PREFIX + "queue1", 0);
                                testComplete();
                            }
                        });
                    }
                });
            }
        });
    }

    @Test
    public void getAllLocks() {
        flushAll();
        eventBusSend(buildOperation(Operation.getAllLocks, new JsonObject()), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals(OK, message.body().getString(STATUS));
                JsonArray locksArray = message.body().getObject(VALUE).getArray("locks");
                assertNotNull("locks array should not be null", locksArray);
                assertEquals("locks array should be empty", 0, locksArray.size());
                eventBusSend(putLockOperation("testLock", "geronimo"), new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> message) {
                        assertEquals(OK, message.body().getString(STATUS));
                        assertTrue(jedis.hexists(REDISQUES_LOCKS, "testLock"));
                        eventBusSend(buildOperation(Operation.getAllLocks, new JsonObject()), new Handler<Message<JsonObject>>() {
                            @Override
                            public void handle(Message<JsonObject> message) {
                                assertEquals(OK, message.body().getString(STATUS));
                                JsonArray locksArray = message.body().getObject(VALUE).getArray("locks");
                                assertNotNull("locks array should not be null", locksArray);
                                assertTrue("locks array should not be empty", locksArray.size() > 0);
                                if(locksArray.size() > 0) {
                                    String result = locksArray.get(0);
                                    assertTrue(result.matches("testLock.*"));
                                }
                                testComplete();
                            }
                        });
                    }
                });
            }
        });
    }

    @Test
    public void putLock() {
        flushAll();
        eventBusSend(putLockOperation("queue1", "someuser"), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals(OK, message.body().getString(STATUS));
                assertTrue(jedis.hexists(REDISQUES_LOCKS, "queue1"));
                assertLockContent("queue1", "someuser");
                testComplete();
            }
        });
    }

    @Test
    public void putLockMissingRequestedBy() {
        flushAll();
        assertKeyCount(REDISQUES_LOCKS, 0);
        assertFalse(jedis.hexists(REDISQUES_LOCKS, "queue1"));
        eventBusSend(buildOperation(Operation.putLock, new JsonObject().putString(QUEUENAME, "queue1")), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals(ERROR, message.body().getString(STATUS));
                assertEquals("Property '"+REQUESTEDBY+"' missing", message.body().getString(MESSAGE));
                assertKeyCount(REDISQUES_LOCKS, 0);
                assertFalse(jedis.hexists(REDISQUES_LOCKS, "queue1"));
                testComplete();
            }
        });
    }

    @Test
    public void getLock() {
        flushAll();
        assertKeyCount(REDISQUES_LOCKS, 0);

        eventBusSend(putLockOperation("testLock1", "geronimo"), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals(OK, message.body().getString(STATUS));
                assertTrue(jedis.hexists(REDISQUES_LOCKS, "testLock1"));
                assertKeyCount(REDISQUES_LOCKS, 1);
                eventBusSend(buildOperation(Operation.getLock, new JsonObject().putString(QUEUENAME, "testLock1")), new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> message) {
                        assertEquals(OK, message.body().getString(STATUS));
                        assertTrue(jedis.hexists(REDISQUES_LOCKS, "testLock1"));
                        assertLockContent("testLock1", "geronimo");
                        testComplete();
                    }
                });
            }
        });
    }

    @Test
    public void getNotExistingLock() {
        flushAll();
        assertKeyCount(REDISQUES_LOCKS, 0);
        eventBusSend(buildOperation(Operation.getLock, new JsonObject().putString(QUEUENAME, "notExistingLock")), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals("No such lock", message.body().getString(STATUS));
                assertKeyCount(REDISQUES_LOCKS, 0);
                assertFalse(jedis.hexists(REDISQUES_LOCKS, "notExistingLock"));
                testComplete();
            }
        });
    }

    @Test
    public void deleteNotExistingLock() {
        flushAll();
        eventBusSend(buildOperation(Operation.deleteLock, new JsonObject().putString(QUEUENAME, "testLock1")), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals(OK, message.body().getString(STATUS));
                assertFalse(jedis.hexists(REDISQUES_LOCKS, "testLock1"));
                testComplete();
            }
        });
    }

    @Test
    public void deleteExistingLock() {
        flushAll();
        assertKeyCount(REDISQUES_LOCKS, 0);
        eventBusSend(putLockOperation("testLock1", "someuser"), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals(OK, message.body().getString(STATUS));
                assertTrue(jedis.hexists(REDISQUES_LOCKS, "testLock1"));
                assertKeyCount(REDISQUES_LOCKS, 1);
                eventBusSend(buildOperation(Operation.deleteLock, new JsonObject().putString(QUEUENAME, "testLock1")), new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> message) {
                        assertEquals(OK, message.body().getString(STATUS));
                        assertFalse(jedis.hexists(REDISQUES_LOCKS, "testLock1"));
                        assertKeyCount(REDISQUES_LOCKS, 0);
                        testComplete();
                    }
                });
            }
        });
    }

    @Test
    public void checkLimit() {
        flushAll();
        for (int i = 0; i < 250; i++) {
            jedis.rpush("redisques:queues:testLock1", "testItem"+i);
        }
        eventBusSend(buildOperation(Operation.getListRange, new JsonObject().putString(QUEUENAME, "testLock1").putString("limit", "178")), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
                assertEquals(OK, message.body().getString(STATUS));
                assertEquals(178, message.body().getArray(VALUE).size());
                testComplete();
            }
        });
    }

    private void assertLockContent(String queuename, String expectedRequestedByValue){
        String item = jedis.hget(REDISQUES_LOCKS, queuename);
        assertNotNull(item);
        if(item != null){
            JsonObject lockInfo = new JsonObject(item);
            assertNotNull("Property '"+REQUESTEDBY+"' missing", lockInfo.getString(REQUESTEDBY));
            assertNotNull("Property '"+TIMESTAMP+"' missing", lockInfo.getNumber(TIMESTAMP));
            assertEquals("Property '"+REQUESTEDBY+"' has wrong value", expectedRequestedByValue, lockInfo.getString(REQUESTEDBY));
        }
    }

    private void eventBusSend(JsonObject operation, Handler<Message<JsonObject>> handler){
        eb.send(address, operation, handler);
    }
}
