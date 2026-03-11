-- Transfer script: Atomically transfer value between two keys
-- Usage: EVALSHA <sha1> 2 from_key to_key amount
-- Returns: 1 on success, 0 if insufficient balance

local from_key = KEYS[1]
local to_key = KEYS[2]
local amount = tonumber(ARGV[1])

local balance = tonumber(redis.call('GET', from_key) or 0)

if balance >= amount then
    redis.call('DECRBY', from_key, amount)
    redis.call('INCRBY', to_key, amount)
    return 1
else
    return 0
end
