package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Class DeleteAllQueueItems.
 *
 * @author baldim
 */
public class DeleteAllQueueItems implements Handler<AsyncResult<Long>> {
    private Message<JsonObject> event;

    public DeleteAllQueueItems(Message<JsonObject> event) {
        this.event = event;
    }

    @Override
    public void handle(AsyncResult<Long> reply) {
        if (reply.result() > 0) {
            event.reply(new JsonObject().put(STATUS, OK));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
