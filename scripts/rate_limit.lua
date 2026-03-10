-- Rate limit script: Simple token bucket rate limiting
-- Usage: EVALSHA <sha1> 1 rate_limit_key limit window_seconds
-- Returns: 1 if allowed, 0 if rate limited

local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local current = tonumber(redis.call('GET', key) or 0)

if current < limit then
    redis.call('INCR', key)
    if current == 0 then
        redis.call('EXPIRE', key, window)
    end
    return 1
else
    return 0
end
