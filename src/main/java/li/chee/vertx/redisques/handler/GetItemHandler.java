package li.chee.vertx.redisques.handler;

import li.chee.vertx.redisques.RedisQues;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 * Class GetItemHandler.
 *
 * @author baldim
 */
public class GetItemHandler implements Handler<Message<JsonObject>> {
    private Message<JsonObject> event;

    public GetItemHandler(Message<JsonObject> event) {
        this.event = event;
    }

    public void handle(Message<JsonObject> reply) {
        if (reply.body().getString(RedisQues.VALUE) != null) {
            event.reply(new JsonObject()
                            .putString(RedisQues.STATUS, RedisQues.OK)
                            .putString(RedisQues.VALUE, reply.body().getString(RedisQues.VALUE))
            );
        } else {
            event.reply(new JsonObject()
                            .putString(RedisQues.STATUS, RedisQues.ERROR)
            );
        }
    }
}
