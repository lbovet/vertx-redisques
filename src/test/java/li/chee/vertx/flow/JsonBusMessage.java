package li.chee.vertx.flow;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class JsonBusMessage extends BusMessage<JsonObject> {
    protected boolean isSuccess(Message<JsonObject> result) {        
        return result != null && result.body.getString("error") == null;
    }

    public void run() {
        eb.send(address, body, this);
    }

}