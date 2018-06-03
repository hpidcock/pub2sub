local topicID = KEYS[1]
local expireAt = ARGV[1]
local channelID = ARGV[2]
local res = redis.call('zadd', topicID, 'NX', '0', channelID)
if res == 0 then
    return 0
end
local c = tonumber(redis.call('ttl', topicID))
if c < 0 then
    redis.call('expireat', topicID, expireAt)
end
return 1
