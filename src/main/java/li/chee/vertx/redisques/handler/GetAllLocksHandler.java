package li.chee.vertx.redisques.handler;

import li.chee.vertx.redisques.RedisQues;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.Arrays;
import java.util.List;

/**
 * Class GetAllLocksHandler.
 *
 * @author baldim
 */
public class GetAllLocksHandler implements Handler<Message<JsonObject>> {

    private Message<JsonObject> event;

    public GetAllLocksHandler(Message<JsonObject> event) {
        this.event = event;
    }

    public void handle(Message<JsonObject> reply) {
        List<Object> list = Arrays.asList(reply.body().getArray(RedisQues.VALUE).toArray());
        JsonObject result = new JsonObject();
        JsonArray items = new JsonArray();
        for (Object item : list.toArray()) {
            items.addString((String) item);
        }
        result.putArray("locks", items);
        if (RedisQues.OK.equals(reply.body().getString(RedisQues.STATUS))) {
            event.reply(new JsonObject()
                            .putString(RedisQues.STATUS, RedisQues.OK)
                            .putObject(RedisQues.VALUE, result)
            );
        } else {
            event.reply(new JsonObject()
                            .putString(RedisQues.STATUS, RedisQues.ERROR)
            );
        }
    }
}