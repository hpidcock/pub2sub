local channelID = KEYS[1]
local exists = redis.call('exists', channelID)
if exists == 0 then
    return nil
end
return redis.call('xadd', channelID, '*', unpack(ARGV))
