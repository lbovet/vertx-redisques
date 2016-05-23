package org.swisspush.redisques.lua;

public enum LuaScript {
    CLEANUP("redisques_cleanup.lua");

    private String file;

    LuaScript(String file) {
        this.file = file;
    }

    public String getFile() {
        return file;
    }
}
