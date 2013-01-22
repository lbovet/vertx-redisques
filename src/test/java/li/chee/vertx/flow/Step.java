package li.chee.vertx.flow;

import org.vertx.java.core.Handler;

/**
 * A chainable step that guarantees order of execution.
 * 
 * @author Laurent Bovet <laurent.bovet@windmaster.ch>
 */
public abstract class Step<T> implements Handler<T> {
    private Step<?> next;
    Step<?> previous;

    /**
     * Runs the next step.
     * 
     * @see org.vertx.java.core.Handler#handle(java.lang.Object)
     */
    public void handle(T result) {
        next();
    }

    /**
     * Chains a step after this one. It will be run after handling the
     * previous step.
     */
    public <X extends Step<?>> X then(X next) {
        this.next = next;
        next.previous = this;

        return next;
    }

    /**
     * Always call this method at the end of the chain to start it.
     */
    public void go() {
        if (previous==null) {
            doRun();
        } else {
            previous.go();
        }
    }

    protected void doRun() {
        run();
    }

    protected void next() {
        if (next != null) {
            next.doRun();
        }            
    }
    
    abstract protected void run();
}