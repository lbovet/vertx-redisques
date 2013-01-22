package li.chee.vertx.flow;

import org.vertx.java.core.eventbus.Message;

public abstract class ValueBusMessage<T> extends BusMessage<T> {
    protected boolean isSuccess(Message<T> result) {
        return result != null;
    }
}