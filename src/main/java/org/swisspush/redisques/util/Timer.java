package org.swisspush.redisques.util;

import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.Random;

/**
 * Utility class for the vertx timer functionalities.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class Timer {
    private final Vertx vertx;
    private Random random;

    public Timer(Vertx vertx) {
        this.vertx = vertx;
        this.random = new Random();
    }

    /**
     * Delay an operation by providing a delay in milliseconds. This method completes the {@link Future} any time
     * between immediately and the delay. When 0 provided as delay, the {@link Future} is resolved immediately.
     *
     * @param delayMs the delay in milliseconds
     * @return A {@link Future} which completes after the delay
     */
    public Future<Void> executeDelayedMax(int delayMs) {
        Future<Void> future = Future.future();

        if (delayMs > 0) {
            vertx.setTimer(random.nextInt(delayMs + 1) + 1, delayed -> future.complete());
        } else {
            future.complete();
        }

        return future;
    }
}
