package org.swisspush.redisques.util;

import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Utility class for the vertx timer functionalities.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class Timer {
    private final Vertx vertx;

    public Timer(Vertx vertx) {
        this.vertx = vertx;
    }

    /**
     * Delay an operation by providing a delay in milliseconds. This method completes the {@link Future} after
     * the delay. When 0 provided as delay, the {@link Future} is resolved immediately.
     *
     * @param delayMs the delay in milliseconds
     * @return A {@link Future} which completes after the delay
     */
    public Future<Void> executeDelayed(int delayMs){
        Future<Void> future = Future.future();

        if(delayMs > 0){
            vertx.setTimer(delayMs, delayed -> future.complete());
        } else {
            future.complete();
        }

        return future;
    }
}
