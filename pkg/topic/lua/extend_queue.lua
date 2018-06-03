local channelID = KEYS[1]
local expireIn = ARGV[1]
local c = tonumber(redis.call('ttl', channelID))
if c == -2 then
    return 0
end
local n = tonumber(expireIn)
if c ~= -1 and n < c then
    return 1
end
redis.call('expire', channelID, expireIn)
return 1
