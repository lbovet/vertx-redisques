package org.swisspush.redisques;

import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import org.junit.Rule;
import org.junit.Test;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Created by florian.kammermann on 31.05.2016.
 */
public class RedisQuesProcessorRedeployTest extends AbstractTestCase {

    @Rule
    public Timeout rule = Timeout.seconds(10);

    /**
     *  This test checks if existing queues are processed after a restart of the redisque verticle.
     *  The important point is, that we wait long enough, that the consumer key expires.
     *  So there will be no consumer registered in redis and because of the clean state of redisques,
     *  there is no consumer registered in the redisques verticle too.
     *  We have to rely onto the check function of RediesQues.
     */
    @Test
    public void notActiveQueueActivatedThroughCheckAfterRedeploy(TestContext context) throws Exception {
        Async async = context.async();
        flushAll();

        final MessageConsumer<JsonObject> queueProcessor = vertx.eventBus().consumer("processor-address");

        queueProcessor.handler(message -> {

            // assert the values we sent too
            context.assertEquals("check-queue", message.body().getString("queue"));
            context.assertEquals("hello", message.body().getString("payload"));

            // assert the value is still in the redis store
            String queueValueInRedis = jedis.lindex("redisques:queues:check-queue", 0);
            context.assertEquals("hello", queueValueInRedis);

            // assert that there is a consumer assigned
            String consumer = jedis.get("redisques:consumers:check-queue");
            context.assertNotNull(consumer);

            // assert that value is still in the queue
            queueValueInRedis = jedis.lindex("redisques:queues:check-queue", 0);
            context.assertEquals("hello", queueValueInRedis);

            // undeploy redisques to simulate a server restart
            vertx.undeploy(deploymentId, res -> {
                if (! res.succeeded()) {
                    throw new IllegalStateException("couldn't redeploy redisques", res.cause());
                }

                // we have to wait long enough, that the redisques consumer key expires
                sleep(2000);
                AbstractTestCase.deployRedisques(context, 1);
                sleep(3000);

                // reregister the processor
                final MessageConsumer<JsonObject>  queueProcessorAfterRedeploy = vertx.eventBus().consumer("processor-address");
                queueProcessorAfterRedeploy.handler(messageAfterRedeploy -> {
                    message.reply(new JsonObject().put(STATUS, OK));
                    // end the test
                    async.complete();
                });

                // assert that the consumer is null
                String consumerAfterRedeploy = jedis.get("redisques:consumers:check-queue");
                context.assertNull(consumerAfterRedeploy);

                // execute check operation, which registers a new consumer to the existing queue and process the queue
                final JsonObject operation = buildCheckOperation();
                eventBusSend(operation, reply -> {
                    context.assertEquals(OK, reply.result().body().getString(STATUS));
                });
            });

        });

        final JsonObject operation = buildEnqueueOperation("check-queue", "hello");
        eventBusSend(operation, reply -> {
            context.assertEquals(OK, reply.result().body().getString(STATUS));
        });
    }

    private void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new IllegalStateException("can not handle interrups on sleeps");
        }
    }
}
