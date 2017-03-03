package org.swisspush.redisques.lua;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * Abstract class containing common methods for LuaScript tests
 */
@RunWith(VertxUnitRunner.class)
public abstract class AbstractLuaScriptTest {

    Jedis jedis = null;

    @Before
    public void connect() {
        jedis = new Jedis("localhost", 6379, 5000);
    }

    @After
    public void disconnect() {
        jedis.flushAll();
        jedis.close();
    }

    protected String readScript(String scriptFileName) {
        return readScript(scriptFileName, false);
    }

    protected String readScript(String scriptFileName, boolean stripLogNotice) {
        BufferedReader in = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(scriptFileName)));
        StringBuilder sb;
        try {
            sb = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                if (stripLogNotice && line.contains("redis.LOG_NOTICE,")) {
                    continue;
                }
                sb.append(line + "\n");
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                // Ignore
            }
        }
        return sb.toString();
    }

    protected Object evalScriptCheck(String lastCheckExecKey, int checkInterval) {
        String checkScript = readScript("redisques_check.lua");
        return jedis.eval(checkScript, new ArrayList() {
                    {
                        add(lastCheckExecKey);
                    }
                }, new ArrayList() {
                    {
                        add(String.valueOf(System.currentTimeMillis()));
                        add(String.valueOf(checkInterval));
                    }
                }
        );
    }
}
