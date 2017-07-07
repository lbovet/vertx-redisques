package org.swisspush.redisques.util;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests for {@link Timer} class.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class TimerTest {

    @Test
    public void testExecuteDelayedLong(TestContext context){
        Async async = context.async();
        Timer timer = new Timer(Vertx.vertx());
        final int delayMs = 1500;
        final long start = System.currentTimeMillis();

        timer.executeDelayed(delayMs).setHandler(delayed -> {
            context.assertTrue(delayed.succeeded());

            long end = System.currentTimeMillis();
            long duration = end - start;
            assertDuration(context, duration, delayMs);
            assertMaxDuration(context, duration, delayMs);
            async.complete();
        });
    }

    @Test
    public void testExecuteDelayedShort(TestContext context){
        Async async = context.async();
        Timer timer = new Timer(Vertx.vertx());
        final int delayMs = 10;
        final long start = System.currentTimeMillis();

        timer.executeDelayed(delayMs).setHandler(delayed -> {
            context.assertTrue(delayed.succeeded());

            long end = System.currentTimeMillis();
            long duration = end - start;
            assertDuration(context, duration, delayMs);
            assertMaxDuration(context, duration, delayMs);
            async.complete();
        });
    }

    @Test
    public void testExecuteDelayedZero(TestContext context){
        Async async = context.async();
        Timer timer = new Timer(Vertx.vertx());
        final int delayMs = 0;
        final long start = System.currentTimeMillis();

        timer.executeDelayed(delayMs).setHandler(delayed -> {
            context.assertTrue(delayed.succeeded());

            long end = System.currentTimeMillis();
            long duration = end - start;
            assertDuration(context, duration, delayMs);
            assertMaxDuration(context, duration, delayMs);
            async.complete();
        });
    }

    private void assertDuration(TestContext context, long duration, int delayMs){
        context.assertTrue(duration >= delayMs, "Future completed after " + duration + "ms. " +
                "This should be greater or equal than the delay of " + delayMs + "ms");
    }

    private void assertMaxDuration(TestContext context, long duration, int delayMs){
        if(delayMs <= 0){
            delayMs = 1;
        }
        double delayPlus10Percent = delayMs * 1.2;
        context.assertTrue(duration < delayPlus10Percent, "Future completed after " + duration + "ms. " +
                "However it should not have taken more than the delay + 20% which would be " + delayPlus10Percent + "ms");
    }
}
