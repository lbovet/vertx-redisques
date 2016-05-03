package org.swisspush.redisques.util;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.swisspush.redisques.util.RedisquesAPI.*;

/**
 * Tests for {@link RedisquesAPI} class.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
@RunWith(VertxUnitRunner.class)
public class RedisquesAPITest {

    @Test
    public void testQueueOperationFromString(TestContext context){
        context.assertNull(QueueOperation.fromString("abc"));
        context.assertNull(QueueOperation.fromString("dummy"));
        context.assertNull(QueueOperation.fromString("doEnqueueThisItemPlease"));

        context.assertEquals(QueueOperation.check, QueueOperation.fromString("check"));
        context.assertEquals(QueueOperation.check, QueueOperation.fromString("CHECK"));

        context.assertEquals(QueueOperation.getAllLocks, QueueOperation.fromString("getAllLocks"));
        context.assertEquals(QueueOperation.getAllLocks, QueueOperation.fromString("getallLOCKS"));

        context.assertEquals(QueueOperation.getQueueItems, QueueOperation.fromString("getListRange")); // legacy
        context.assertEquals(QueueOperation.getQueueItems, QueueOperation.fromString("GETLISTRANGE")); // legacy
        context.assertEquals(QueueOperation.getQueueItems, QueueOperation.fromString("getQueueItems"));

        context.assertEquals(QueueOperation.addQueueItem, QueueOperation.fromString("addItem")); // legacy
        context.assertEquals(QueueOperation.addQueueItem, QueueOperation.fromString("addITEM")); // legacy
        context.assertEquals(QueueOperation.addQueueItem, QueueOperation.fromString("addQueueItem"));
        context.assertEquals(QueueOperation.addQueueItem, QueueOperation.fromString("addQUEUEItem"));

        context.assertEquals(QueueOperation.deleteQueueItem, QueueOperation.fromString("deleteItem")); // legacy
        context.assertEquals(QueueOperation.deleteQueueItem, QueueOperation.fromString("DELETEItem")); // legacy
        context.assertEquals(QueueOperation.deleteQueueItem, QueueOperation.fromString("deleteQueueItem"));
        context.assertEquals(QueueOperation.deleteQueueItem, QueueOperation.fromString("DELETEQueueItem"));

        context.assertEquals(QueueOperation.getQueueItem, QueueOperation.fromString("getItem")); // legacy
        context.assertEquals(QueueOperation.getQueueItem, QueueOperation.fromString("getitem")); // legacy
        context.assertEquals(QueueOperation.getQueueItem, QueueOperation.fromString("getQueueItem"));
        context.assertEquals(QueueOperation.getQueueItem, QueueOperation.fromString("GETQueUEItem"));

        context.assertEquals(QueueOperation.replaceQueueItem, QueueOperation.fromString("replaceItem")); // legacy
        context.assertEquals(QueueOperation.replaceQueueItem, QueueOperation.fromString("replACeIteM")); // legacy
        context.assertEquals(QueueOperation.replaceQueueItem, QueueOperation.fromString("replaceQueueItem"));
        context.assertEquals(QueueOperation.replaceQueueItem, QueueOperation.fromString("REPLACEQUEUEITEM"));

        context.assertEquals(QueueOperation.getQueues, QueueOperation.fromString("getQueues"));
        context.assertEquals(QueueOperation.getQueuesCount, QueueOperation.fromString("getQueuesCount"));
        context.assertEquals(QueueOperation.getQueueItemsCount, QueueOperation.fromString("getQueueItemsCount"));
    }

    @Test
    public void testLegacyName(TestContext context){
        context.assertTrue(QueueOperation.getQueueItems.hasLegacyName());
        context.assertTrue(QueueOperation.addQueueItem.hasLegacyName());
        context.assertTrue(QueueOperation.deleteQueueItem.hasLegacyName());
        context.assertTrue(QueueOperation.getQueueItem.hasLegacyName());
        context.assertTrue(QueueOperation.replaceQueueItem.hasLegacyName());

        context.assertEquals("getListRange", QueueOperation.getQueueItems.getLegacyName());
        context.assertEquals("addItem", QueueOperation.addQueueItem.getLegacyName());
        context.assertEquals("deleteItem", QueueOperation.deleteQueueItem.getLegacyName());
        context.assertEquals("getItem", QueueOperation.getQueueItem.getLegacyName());
        context.assertEquals("replaceItem", QueueOperation.replaceQueueItem.getLegacyName());

        context.assertFalse(QueueOperation.enqueue.hasLegacyName());
        context.assertFalse(QueueOperation.check.hasLegacyName());
        context.assertFalse(QueueOperation.reset.hasLegacyName());
        context.assertFalse(QueueOperation.stop.hasLegacyName());
        context.assertFalse(QueueOperation.deleteAllQueueItems.hasLegacyName());
        context.assertFalse(QueueOperation.getAllLocks.hasLegacyName());
        context.assertFalse(QueueOperation.putLock.hasLegacyName());
        context.assertFalse(QueueOperation.getLock.hasLegacyName());
        context.assertFalse(QueueOperation.deleteLock.hasLegacyName());
        context.assertFalse(QueueOperation.getQueues.hasLegacyName());
        context.assertFalse(QueueOperation.getQueuesCount.hasLegacyName());
        context.assertFalse(QueueOperation.getQueueItemsCount.hasLegacyName());
    }
}
