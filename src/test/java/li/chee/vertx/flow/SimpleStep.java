package li.chee.vertx.flow;

public abstract class SimpleStep extends Step<Void> {
    protected void doRun() {
        run();
        next();
    }
    
}