package li.chee.vertx.flow.example;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.deploy.Verticle;

public class SlaveVerticle extends Verticle{

    @Override
    public void start() throws Exception {
    
        vertx.eventBus().registerHandler("slave-json", new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> event) {   
                System.out.println("slave got message "+event.body.toString());
                switch(event.body.getString("command")) {
                    case "success" : event.reply(new JsonObject("{ \"response\" : \"okay\" }"));
                    break;
                    
                    case "error" : event.reply(new JsonObject("{ \"error\" : \"failed\" }"));
                    break;
                    
                    case "wait" : 
                        vertx.setTimer(event.body.getLong("delay"), new Handler<Long>() {
                            public void handle(Long timerId) {
                                event.reply(new JsonObject("{ \"response\" : \"late\" }"));
                            }
                        });                                                
                    break;
                }
                
            }
        });
        
        vertx.eventBus().registerHandler("slave-long", new Handler<Message<Long>>() {
            public void handle(Message<Long> event) {
                System.out.println("slave got long "+event.body);
            }            
        });
            
            
        System.out.println("slave started");
        
    }
    
    

}
