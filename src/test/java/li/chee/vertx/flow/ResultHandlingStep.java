package li.chee.vertx.flow;

import org.vertx.java.core.Handler;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.Vertx;

/**
 * A step with additional behavior according to the asynchronous result or
 * timeout.
 * 
 * @author Laurent Bovet <laurent.bovet@windmaster.ch>
 */
public abstract class ResultHandlingStep<T> extends Step<T> {
    private Handler<T> successHandler;
    private Handler<T> errorHandler;
    private Vertx vertx;
    private long timeout;
    private SimpleHandler timeoutHandler;
    private Long timerId;
    private boolean timedOut;
    private boolean continueAfterTimeout = false;
    private boolean continueAfterError = false;

    public void handle(T result) {
        if (timerId != null) {
            vertx.cancelTimer(timerId);
        }
        if(!timedOut) {
            if (isSuccess(result)) {
                if (successHandler != null) {
                    successHandler.handle(result);
                }
                next();
            } else {
                if (errorHandler != null) {            
                    errorHandler.handle(result);
                }
                if (continueAfterError) {
                    next();                
                }
            }
        }
    }

    abstract protected boolean isSuccess(T result);

    public ResultHandlingStep<T> success(Handler<T> handler) {
        successHandler = handler;
        return this;
    }

    public ResultHandlingStep<T> error(Handler<T> handler) {
        errorHandler = handler;
        return this;
    }

    public ResultHandlingStep<T> timeout(Vertx vertx, long milliseconds, SimpleHandler simpleHandler) {
        this.vertx = vertx;
        timeout = milliseconds;
        timeoutHandler = simpleHandler;
        return this;
    }

    public ResultHandlingStep<T> continueAfterTimeout() {
        continueAfterTimeout = true;
        return this;
    }

    public ResultHandlingStep<T> continueAfterError() {
        continueAfterError = true;
        return this;
    }

    public void doRun() {
        if (timeout > 0) {
            timerId = vertx.setTimer(timeout, new Handler<Long>() {
                public void handle(Long event) {
                    timedOut = true;
                    timeoutHandler.handle(null);
                    if (continueAfterTimeout) {
                        next();
                    } 
                }
            });
        }
        run();
    }
}