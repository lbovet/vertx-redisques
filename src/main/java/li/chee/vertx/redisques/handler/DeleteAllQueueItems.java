package li.chee.vertx.redisques.handler;

import li.chee.vertx.redisques.RedisQues;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 * Class DeleteAllQueueItems.
 *
 * @author baldim
 */
public class DeleteAllQueueItems implements Handler<Message<JsonObject>> {
    private Message<JsonObject> event;

    public DeleteAllQueueItems(Message<JsonObject> event) {
        this.event = event;
    }

    public void handle(Message<JsonObject> reply) {
        if (reply.body().getInteger(RedisQues.VALUE) > 0) {
            event.reply(new JsonObject().putString(RedisQues.STATUS, RedisQues.OK));
        } else {
            event.reply(new JsonObject().putString(RedisQues.STATUS, RedisQues.ERROR));
        }
    }
}
