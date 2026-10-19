-- Acknowledges and deletes stream entries atomically, so a crash never leaves an
-- acknowledged entry behind in the stream.

local streamKey = KEYS[1]
local group = ARGV[1]

local acked = 0
for i = 2, #ARGV do
  acked = acked + redis.call("XACK", streamKey, group, ARGV[i])
  redis.call("XDEL", streamKey, ARGV[i])
end

return acked
