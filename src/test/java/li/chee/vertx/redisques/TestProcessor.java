package li.chee.vertx.redisques;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.xml.bind.DatatypeConverter;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.deploy.Verticle;
import org.vertx.java.framework.TestUtils;

public class TestProcessor extends Verticle {

    private TestUtils tu;

    private Map<String, MessageDigest> signatures = new HashMap<String, MessageDigest>();

    @Override
    public void start() throws Exception {
        final Logger log = container.getLogger();

        final EventBus eb = vertx.eventBus();

        tu = new TestUtils(vertx);

        final Map<String, Integer> counters = new HashMap<>();
        
        eb.registerHandler("redisques-processor", new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> message) {
                final String queue = message.body.getString("queue");
                final String payload = message.body.getString("payload");

                if(!counters.containsKey(queue)) {
                    counters.put(queue, 0);
                }
                
                System.out.println("GOT ["+counters.get(queue)+"] " + payload + " in " + queue+".");
                
                counters.put(queue, counters.get(queue)+1);
                
                if ("STOP".equals(payload)) {
                    message.reply(new JsonObject() {
                        {
                            putString("status", "ok");
                        }
                    });
                    eb.send("digest-" + queue, DatatypeConverter.printBase64Binary(signatures.get(queue).digest()));
                } else {
                    MessageDigest signature = signatures.get(queue);
                    if (signature == null) {
                        try {
                            signature = MessageDigest.getInstance("MD5");
                            signatures.put(queue, signature);
                        } catch (NoSuchAlgorithmException e) {
                            throw new RuntimeException();
                        }
                    }
                    signature.update(payload.getBytes());
                }

                log.info("Processing message " + payload);
                vertx.setTimer(new Random().nextLong() % 1, new Handler<Long>() {
                    public void handle(Long event) {
                        log.debug("Processed message " + payload);
                        message.reply(new JsonObject() {
                            {
                                putString("status", "ok");
                            }
                        });
                    }
                });
            }
        });

        tu.appReady();
    }

    @Override
    public void stop() throws Exception {
        tu.appStopped();
    }

}
