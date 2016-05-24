package org.swisspush.redisques.lua;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class RedisCleanupLuaScriptTests extends AbstractLuaScriptTest {

    private final String key = "queue_cleanup_lastexec";

    @Test
    public void testCleanup() throws InterruptedException {
        assertThat(jedis.get(key), nullValue());
        assertThat(evalScriptCleanup(key, 2), equalTo(1L));
        String lastExecTS = jedis.get(key);

        assertThat(evalScriptCleanup(key, 30), equalTo(0L));
        assertThat(jedis.get(key), equalTo(lastExecTS));
        assertThat(evalScriptCleanup(key, 30), equalTo(0L));
        assertThat(jedis.get(key), equalTo(lastExecTS));
        assertThat(evalScriptCleanup(key, 30), equalTo(0L));
        assertThat(jedis.get(key), equalTo(lastExecTS));

        Thread.sleep(2500); //wait 2.5 seconds

        assertThat(jedis.get(key), nullValue());
        assertThat(evalScriptCleanup(key, 2), equalTo(1L));
        assertThat(jedis.get(key), notNullValue());
        assertThat(jedis.get(key), not(equalTo(lastExecTS)));
    }
}
