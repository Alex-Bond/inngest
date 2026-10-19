-- Moves a pending entry to the dead-letter stream and acks and deletes it, unless
-- a live consumer holds it.  Returns 1 moved, 0 not pending, -1 held by a live consumer.

local streamKey = KEYS[1]
local deadLetterKey = KEYS[2]

local group = ARGV[1]
local id = ARGV[2]
local consumer = ARGV[3]
local minIdleMs = tonumber(ARGV[4])
local reason = ARGV[5]

local pending = redis.call("XPENDING", streamKey, group, id, id, 1)
if not pending or #pending == 0 then
  return 0
end

local owner = pending[1][2]
local idle = tonumber(pending[1][3])
local deliveries = pending[1][4]
if owner ~= consumer and idle < minIdleMs then
  return -1
end

local entries = redis.call("XRANGE", streamKey, id, id)
if entries and #entries > 0 then
  local args = { "XADD", deadLetterKey, "*", "source_id", id, "reason", reason, "deliveries", tostring(deliveries) }
  local fields = entries[1][2]
  for i = 1, #fields do
    table.insert(args, fields[i])
  end
  redis.call(unpack(args))
end

redis.call("XACK", streamKey, group, id)
redis.call("XDEL", streamKey, id)

return 1
