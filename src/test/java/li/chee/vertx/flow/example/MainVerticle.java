package li.chee.vertx.flow.example;

import li.chee.vertx.flow.JsonBusMessage;
import li.chee.vertx.flow.LongBusMessage;
import li.chee.vertx.flow.SimpleStep;
import li.chee.vertx.flow.Step;

import org.vertx.java.core.Handler;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.framework.TestClientBase;

/**
 * Tests and demonstrate the flow API.
 * 
 * @author Laurent Bovet <laurent.bovet@windmaster.ch>
 */
public class MainVerticle extends TestClientBase {

    public void start() {
        super.start();
        tu.appReady();
    }

    public void testIt() {

        final EventBus eb = vertx.eventBus();
        final JsonObject body = new JsonObject();
        
        new Step<String>() {
            public void run() {
                System.out.println("Deploying slave");
                container.deployVerticle(SlaveVerticle.class.getName(), null, 1, this);
            }
        }.

        // Test the normal case (success)
        then(new SimpleStep() {
            protected void run() {
                body.putString("command", "success");
                System.out.println("success request prepared");
            }
        }).

        then(new JsonBusMessage()).send(eb, "slave-json", body). /**/
        success(new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> event) {
                System.out.println("success");
            }
        }). /**/
        
        // Error case
        then(new SimpleStep() {
            protected void run() {
                body.putString("command", "error");
                System.out.println("error request prepared");
            }
        }).

        then(new JsonBusMessage()).send(eb, "slave-json", body). /**/
        success(new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> event) {
                System.out.println("Unexpected success");
                tu.azzert(false, "Unexpected success");
            }
        }). /**/
        error(new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> event) {
                System.out.println("Expected error");
            }
        }). /**/
        continueAfterError(). /**/
        timeout(vertx, 500L, new SimpleHandler() {
            protected void handle() {
                System.out.println("Got unexpected timeout");
                tu.azzert(false, "Unexpected timeout");
            }
        }).

        // Timeout
        then(new SimpleStep() {
            protected void run() {
                body.putString("command", "wait");
                body.putNumber("delay", 600);
                System.out.println("wait request prepared");
            }
        }).

        then(new JsonBusMessage()).send(eb, "slave-json", body). /**/
        success(new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> event) {
                System.out.println("Got unexpected success");
                tu.azzert(false, "Unexpected success");
            }
        }). /**/
        timeout(vertx, 500L, new SimpleHandler() {
            protected void handle() {
                System.out.println("Got expected timeout 1");
            }
        }). 
        continueAfterTimeout().
        
        // Another type of bus message
        then(new LongBusMessage()).send(eb, "slave-long", 34L). /**/
        timeout(vertx, 500L, new SimpleHandler() {
            protected void handle() {
                System.out.println("Got expected timeout 2");
            }
        }). /**/
        continueAfterTimeout().

        then(new SimpleStep() {
            protected void run() {
                System.out.println("completed");
                tu.testComplete();
            }
        }).

        go();

    }
}
