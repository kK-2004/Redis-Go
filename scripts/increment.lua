-- Increment script: Atomically increment multiple keys by different amounts
-- Usage: EVALSHA <sha1> 2 a b
-- Returns: New values as array [new_a, new_b]

local a = KEYS[1]
local b = KEYS[2]

-- Increment a by 1
local new_a = redis.call('INCRBY', a, 1)

-- Increment b by 2
local new_b = redis.call('INCRBY', b, 2)

-- Return new values
return {new_a, new_b}
