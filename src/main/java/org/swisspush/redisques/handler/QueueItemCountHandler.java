package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Class QueueItemCountHandler.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class QueueItemCountHandler implements Handler<AsyncResult<Long>> {
    private Message<JsonObject> event;

    public QueueItemCountHandler(Message<JsonObject> event) {
        this.event = event;
    }

    @Override
    public void handle(AsyncResult<Long> reply) {
        if(reply.succeeded()){
            Long queueItemCount = reply.result();
            event.reply(new JsonObject().put(STATUS, OK).put(VALUE, queueItemCount));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
