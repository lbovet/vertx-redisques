package org.swisspush.redisques.util;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Class RedisquesAPI listing the operations and response values which are supported in Redisques.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class RedisquesAPI {
    public static final String OK = "ok";
    public static final String INFO = "info";
    public static final String INDEX = "index";
    public static final String LIMIT = "limit";
    public static final String VALUE = "value";
    public static final String ERROR = "error";
    public static final String BUFFER = "buffer";
    public static final String STATUS = "status";
    public static final String MESSAGE = "message";
    public static final String PAYLOAD = "payload";
    public static final String QUEUENAME = "queuename";
    public static final String UNLOCK = "unlock";
    public static final String OPERATION = "operation";
    public static final String REQUESTED_BY = "requestedBy";
    public static final String NO_SUCH_LOCK = "No such lock";

    private static Logger log = LoggerFactory.getLogger(RedisquesAPI.class);

    public enum QueueOperation {
        enqueue(null),
        lockedEnqueue(null),
        check(null),
        reset(null),
        stop(null),
        getQueueItems("getListRange"),
        addQueueItem("addItem"),
        deleteQueueItem("deleteItem"),
        getQueueItem("getItem"),
        replaceQueueItem("replaceItem"),
        deleteAllQueueItems(null),
        getAllLocks(null),
        putLock(null),
        getLock(null),
        deleteLock(null),
        getQueues(null),
        getQueuesCount(null),
        getQueueItemsCount(null);

        private final String legacyName;

        QueueOperation(String legacyName){
            this.legacyName = legacyName;
        }

        public String getLegacyName() {
            return legacyName;
        }

        public boolean hasLegacyName(){
            return legacyName != null;
        }

        public static QueueOperation fromString(String op){
            for (QueueOperation queueOperation : values()) {
                if(queueOperation.name().equalsIgnoreCase(op)){
                    return queueOperation;
                } else if(queueOperation.hasLegacyName() && queueOperation.getLegacyName().equalsIgnoreCase(op)){
                    log.warn("Legacy queue operation used. This may be removed in future releases. Use '"+queueOperation.name()+"' instead of '" + queueOperation.getLegacyName() + "'");
                    return queueOperation;
                }
            }
            return null;
        }
    }

    public static JsonObject buildOperation(QueueOperation queueOperation){
        JsonObject op = new JsonObject();
        op.put(OPERATION, queueOperation.name());
        return op;
    }

    public static JsonObject buildOperation(QueueOperation queueOperation, JsonObject payload){
        JsonObject op = buildOperation(queueOperation);
        op.put(PAYLOAD, payload);
        return op;
    }

    public static JsonObject buildCheckOperation(){
        return buildOperation(QueueOperation.check);
    }

    public static JsonObject buildEnqueueOperation(String queueName, String message){
        JsonObject operation = buildOperation(QueueOperation.enqueue, new JsonObject().put(QUEUENAME, queueName));
        operation.put(MESSAGE, message);
        return operation;
    }

    public static JsonObject buildLockedEnqueueOperation(String queueName, String message, String lockRequestedBy){
        JsonObject operation = buildOperation(QueueOperation.lockedEnqueue, new JsonObject().put(QUEUENAME, queueName).put(REQUESTED_BY, lockRequestedBy));
        operation.put(MESSAGE, message);
        return operation;
    }

    public static JsonObject buildGetQueueItemsOperation(String queueName, String limit){
        return buildOperation(QueueOperation.getQueueItems, new JsonObject().put(QUEUENAME, queueName).put("limit", limit));
    }

    public static JsonObject buildAddQueueItemOperation(String queueName, String buffer){
        return buildOperation(QueueOperation.addQueueItem, new JsonObject().put(QUEUENAME, queueName).put("buffer", buffer));
    }

    public static JsonObject buildGetQueueItemOperation(String queueName, int index){
        return buildOperation(QueueOperation.getQueueItem, new JsonObject().put(QUEUENAME, queueName).put("index", index));
    }

    public static JsonObject buildReplaceQueueItemOperation(String queueName, int index, String buffer){
        return buildOperation(QueueOperation.replaceQueueItem, new JsonObject().put(QUEUENAME, queueName).put("index", index).put("buffer", buffer));
    }

    public static JsonObject buildDeleteQueueItemOperation(String queueName, int index){
        return buildOperation(QueueOperation.deleteQueueItem, new JsonObject().put(QUEUENAME, queueName).put("index", index));
    }

    public static JsonObject buildDeleteAllQueueItemsOperation(String queueName){
        return buildDeleteAllQueueItemsOperation(queueName, false);
    }

    public static JsonObject buildDeleteAllQueueItemsOperation(String queueName, boolean unlock){
        return buildOperation(QueueOperation.deleteAllQueueItems, new JsonObject().put(QUEUENAME, queueName).put("unlock", unlock));
    }

    public static JsonObject buildGetQueuesOperation(){
        return buildOperation(QueueOperation.getQueues);
    }

    public static JsonObject buildGetQueuesCountOperation(){
        return buildOperation(QueueOperation.getQueuesCount);
    }

    public static JsonObject buildGetQueueItemsCountOperation(String queueName){
        return buildOperation(QueueOperation.getQueueItemsCount, new JsonObject().put(QUEUENAME, queueName));
    }

    public static JsonObject buildGetLockOperation(String queueName){
        return buildOperation(QueueOperation.getLock, new JsonObject().put(QUEUENAME, queueName));
    }

    public static JsonObject buildDeleteLockOperation(String queueName){
        return buildOperation(QueueOperation.deleteLock, new JsonObject().put(QUEUENAME, queueName));
    }

    public static JsonObject buildPutLockOperation(String queueName, String user){
        return buildOperation(QueueOperation.putLock, new JsonObject().put(QUEUENAME, queueName).put(REQUESTED_BY, user));
    }

    public static JsonObject buildGetAllLocksOperation(){
        return buildOperation(QueueOperation.getAllLocks);
    }
}
