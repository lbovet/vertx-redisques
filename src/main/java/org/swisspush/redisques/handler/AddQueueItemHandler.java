package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Class AddQueueItemHandler.
 *
 * @author baldim, https://github.com/mcweba [Marc-Andre Weber]
 */
public class AddQueueItemHandler implements Handler<AsyncResult<Long>> {
    private Message<JsonObject> event;

    public AddQueueItemHandler(Message<JsonObject> event) {
        this.event = event;
    }

    @Override
    public void handle(AsyncResult<Long> reply) {
        if(reply.succeeded()){
            event.reply(new JsonObject().put(STATUS, OK));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
