package li.chee.vertx.redisques.handler;

import io.vertx.core.AsyncResult;
import li.chee.vertx.redisques.RedisQues;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * Class GetItemHandler.
 *
 * @author baldim
 */
public class GetItemHandler implements Handler<AsyncResult<String>> {
    private Message<JsonObject> event;

    public GetItemHandler(Message<JsonObject> event) {
        this.event = event;
    }

    @Override
    public void handle(AsyncResult<String> reply) {
        if (reply.result() != null) {
            event.reply(new JsonObject()
                    .put(RedisQues.STATUS, RedisQues.OK)
                    .put(RedisQues.VALUE, reply.result())
            );
        } else {
            event.reply(new JsonObject()
                    .put(RedisQues.STATUS, RedisQues.ERROR)
            );
        }
    }
}
