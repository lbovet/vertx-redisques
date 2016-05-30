local path = KEYS[1]
local lastExecTS = tonumber(ARGV[1])
local interval = tonumber(ARGV[2])

local result = redis.call('set',path,lastExecTS,'NX','EX',interval)

if result then
    return 1
else
    return 0
end