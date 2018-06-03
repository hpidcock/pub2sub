local channelID = KEYS[1]
local expireIn = ARGV[1]
local c = tonumber(redis.call('ttl', channelID))
if c ~= -2 then
    local n = tonumber(expireIn)
    if c >= 0 and n < c then
        return 1
    end
    redis.call('expire', channelID, expireIn)
    return 1
end
redis.call('xadd', channelID, '0-1', 'new_queue', '0')
redis.call('expire', channelID, expireIn)
return 1
