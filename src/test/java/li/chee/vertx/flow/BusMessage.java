package li.chee.vertx.flow;

import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;

/**
 * A base class for event bus message sending steps.
 * 
 * @author Laurent Bovet <laurent.bovet@windmaster.ch>
 */
public abstract class BusMessage<T> extends ResultHandlingStep<Message<T>> {
    protected EventBus eb;
    protected String address;
    protected T body;

    public BusMessage<T> send(EventBus eb, String address, T body) {
        this.address = address;
        this.body = body;
        this.eb = eb;
        return this;
    }
}