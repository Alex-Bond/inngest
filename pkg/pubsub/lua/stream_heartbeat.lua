-- Resets the idle time of entries this consumer still owns so a slow live handler
-- is never reclaimed; RETRYCOUNT keeps the delivery count unchanged.

local streamKey = KEYS[1]
local group = ARGV[1]
local consumer = ARGV[2]

local refreshed = 0
for i = 3, #ARGV do
  local id = ARGV[i]
  local pending = redis.call("XPENDING", streamKey, group, id, id, 1, consumer)
  if pending and #pending > 0 then
    redis.call("XCLAIM", streamKey, group, consumer, 0, id, "RETRYCOUNT", pending[1][4], "JUSTID")
    refreshed = refreshed + 1
  end
end

return refreshed
