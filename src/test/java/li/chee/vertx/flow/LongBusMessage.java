package li.chee.vertx.flow;

public class LongBusMessage extends ValueBusMessage<Long> {
    public void run() {
        eb.send(address, body, this);
    }
}