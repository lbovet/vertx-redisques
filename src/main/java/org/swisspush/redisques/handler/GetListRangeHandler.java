package org.swisspush.redisques.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.json.JsonArray;
import static org.swisspush.redisques.util.RedisquesAPI.*;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * Class GetListRangeHandler.
 *
 * @author baldim, webermarca
 */
public class GetListRangeHandler implements Handler<AsyncResult<JsonArray>> {
    private Message<JsonObject> event;
    private Long queueItemCount;

    public GetListRangeHandler(Message<JsonObject> event, Long queueItemCount) {
        this.event = event;
        this.queueItemCount = queueItemCount;
    }

    @Override
    public void handle(AsyncResult<JsonArray> reply) {
        if(reply.succeeded()){
            JsonArray resultArray = reply.result();
            JsonArray countInfo = new JsonArray();
            countInfo.add(resultArray.size());
            countInfo.add(queueItemCount);
            event.reply(new JsonObject().put(STATUS, OK).put(VALUE, resultArray).put(INFO, countInfo));
        } else {
            event.reply(new JsonObject().put(STATUS, ERROR));
        }
    }
}
