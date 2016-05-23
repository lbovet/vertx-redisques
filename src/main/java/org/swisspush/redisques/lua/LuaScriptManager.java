package org.swisspush.redisques.lua;

import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.RedisClient;

import java.util.*;

/**
 * Manages the lua scripts.
 *
 * @author https://github.com/mcweba [Marc-Andre Weber]
 */
public class LuaScriptManager {

    private RedisClient redisClient;
    private Map<LuaScript,LuaScriptState> luaScripts = new HashMap<>();
    private Logger log = LoggerFactory.getLogger(LuaScriptManager.class);

    public LuaScriptManager(RedisClient redisClient){
        this.redisClient = redisClient;

        LuaScriptState luaGetScriptState = new LuaScriptState(LuaScript.CLEANUP, redisClient, false);
        luaGetScriptState.loadLuaScript(new RedisCommandDoNothing(), 0);
        luaScripts.put(LuaScript.CLEANUP, luaGetScriptState);
    }

    /**
     * If the loglevel is trace and the logoutput in luaScriptState is false, then reload the script with logoutput and execute the RedisCommand.
     * If the loglevel is not trace and the logoutput in luaScriptState is true, then reload the script without logoutput and execute the RedisCommand.
     * If the loglevel is matching the luaScriptState, just execute the RedisCommand.
     *
     * @param luaScript the type of lua script
     * @param redisCommand the redis command to execute
     * @param executionCounter current count of already passed executions
     */
    private void reloadScriptIfLoglevelChangedAndExecuteRedisCommand(LuaScript luaScript, RedisCommand redisCommand, int executionCounter) {
        boolean logoutput = log.isTraceEnabled();
        LuaScriptState luaScriptState = luaScripts.get(luaScript);
        // if the loglevel didn't change, execute the command and return
        if(logoutput == luaScriptState.getLogoutput()) {
            redisCommand.exec(executionCounter);
            return;
            // if the loglevel changed, set the new loglevel into the luaScriptState, recompose the script and provide the redisCommand as parameter to execute
        } else if(logoutput && ! luaScriptState.getLogoutput()) {
            luaScriptState.setLogoutput(true);
            luaScriptState.recomposeLuaScript();

        } else if(! logoutput && luaScriptState.getLogoutput()) {
            luaScriptState.setLogoutput(false);
            luaScriptState.recomposeLuaScript();
        }
        luaScriptState.loadLuaScript(redisCommand, executionCounter);
    }

    public void handleQueueCleanup(String lastCleanupExecKey, int cleanupInterval, Handler<Boolean> handler){
        List<String> keys = Collections.singletonList(lastCleanupExecKey);
        List<String> arguments = Arrays.asList(
                String.valueOf(System.currentTimeMillis()),
                String.valueOf(cleanupInterval)
        );
        reloadScriptIfLoglevelChangedAndExecuteRedisCommand(LuaScript.CLEANUP, new Cleanup(keys, arguments, redisClient, handler), 0);
    }

    private class Cleanup implements RedisCommand {

        private List<String> keys;
        private List<String> arguments;
        private Handler<Boolean> handler;
        private RedisClient redisClient;

        public Cleanup(List<String> keys, List<String> arguments, RedisClient redisClient, final Handler<Boolean> handler) {
            this.keys = keys;
            this.arguments = arguments;
            this.redisClient = redisClient;
            this.handler = handler;
        }

        @Override
        public void exec(int executionCounter) {
            redisClient.evalsha(luaScripts.get(LuaScript.CLEANUP).getSha(), keys, arguments, event -> {
                if(event.succeeded()){
                    Long value = event.result().getLong(0);
                    if (log.isTraceEnabled()) {
                        log.trace("Cleanup lua script got result: " + value);
                    }
                    handler.handle(value == 1L);
                } else {
                    String message = event.cause().getMessage();
                    if(message != null && message.startsWith("NOSCRIPT")) {
                        log.warn("cleanup script couldn't be found, reload it");
                        log.warn("amount the script got loaded: " + String.valueOf(executionCounter));
                        if(executionCounter > 10) {
                            log.error("amount the script got loaded is higher than 10, we abort");
                        } else {
                            luaScripts.get(LuaScript.CLEANUP).loadLuaScript(new Cleanup(keys, arguments, redisClient, handler), executionCounter);
                        }
                    } else {
                        log.error("Cleanup request failed with message: " + message);
                    }
                }
            });
        }
    }
}
