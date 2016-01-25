package li.chee.vertx.redisques.handler;

import io.vertx.core.AsyncResult;
import li.chee.vertx.redisques.RedisQues;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

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
            event.reply(new JsonObject().put(RedisQues.STATUS, RedisQues.OK));
        } else {
            event.reply(new JsonObject().put(RedisQues.STATUS, RedisQues.ERROR));
        }
    }
}
