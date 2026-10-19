--
-- This script runs bulk batch append ops in an atomic action.
-- It accepts multiple events and handles overflow atomically - if adding N events
-- exceeds MaxSize, the script splits into current + new batch.
--

local batchPointerKey = KEYS[1]      -- key to the batch pointer

local batchLimit = tonumber(ARGV[1]) -- max size configured for this batch
local batchSizeLimit = tonumber(ARGV[2])
local prefix = ARGV[3]               -- the prefix used for redis
local batchStatusAppending = ARGV[4]
local batchStatusStarted = ARGV[5]
local nowUnixSeconds = tonumber(ARGV[6])
local idempotenceSetTTL = tonumber(ARGV[7])
local newULID = ARGV[8]              -- ULID to update the pointer with if the batch is full or doesn't exist
local overflowULID = ARGV[9]         -- ULID to use for overflow batch if needed
local eventCount = tonumber(ARGV[10])

-- Events are passed as pairs: eventID1, event1, eventID2, event2, ...
-- Starting at ARGV[11]

-- helper functions
-- $include(helpers.lua)

local function get_or_create_batch_key(key)
  local val = redis.call("GET", key)

  -- if empty or doesn't exist
  if is_empty(val) then
    -- create new pointer by setting the ULID
    update_pointer(batchPointerKey, newULID)
    val = newULID
  end

  return val
end

-- start execution
local batchID = get_or_create_batch_key(batchPointerKey)

-- NOTE: these need to be identical to the ones in the queue key generator
--   * Batch
--   * BatchMetadata
local keyfmt = "%s:batches:%s"
local idemKeyFmt = "%s:batch_idem:%s"
local batchKey = string.format(keyfmt, prefix, batchID)
local batchMetadataKey = string.format("%s:metadata", batchKey)

-- set the batch status if it doesn't exist but don't overwrite
-- this is necessary for functions that never enabled batch before
if is_status_empty(batchMetadataKey) then
  set_batch_status(batchMetadataKey, batchStatusAppending)
end

-- Dedup: per-event keys (O(1)) storing each event's batch, which a duplicate reports for scheduling.
local eventsToAdd = {}
local idsToAdd = {}
local seenInCall = {}
local duplicateBatchIDs = {}
local seenDuplicateBatch = {}
local duplicateCount = 0
local committedBytes = 0
local argOffset = 11

local function add_duplicate_batch(id)
  if not seenDuplicateBatch[id] then
    seenDuplicateBatch[id] = true
    table.insert(duplicateBatchIDs, id)
  end
end

for i = 1, eventCount do
  local eventID = ARGV[argOffset + (i - 1) * 2]
  local eventData = ARGV[argOffset + (i - 1) * 2 + 1]

  local idemKey = string.format(idemKeyFmt, prefix, eventID)
  local existing = redis.call("GET", idemKey)
  if existing then
    duplicateCount = duplicateCount + 1
    -- "1" predates storing the batch ID; fall back to the current batch.
    if existing == "1" then
      add_duplicate_batch(batchID)
    else
      add_duplicate_batch(existing)
    end
  elseif seenInCall[eventID] then
    duplicateCount = duplicateCount + 1
  else
    seenInCall[eventID] = true
    table.insert(eventsToAdd, eventData)
    table.insert(idsToAdd, eventID)
    committedBytes = committedBytes + string.len(eventData)
  end
end

local function encode_result(result)
  if #duplicateBatchIDs > 0 then
    result.duplicateBatchIDs = duplicateBatchIDs
  end
  return cjson.encode(result)
end

-- If all events were duplicates, return early
if #eventsToAdd == 0 then
  return encode_result({
    status = "itemexists",
    batchID = batchID,
    batchPointerKey = batchPointerKey,
    committed = 0,
    duplicates = duplicateCount,
    committedBytes = 0
  })
end

-- Get current batch length
local currentLen = redis.call("LLEN", batchKey)
local capacity = batchLimit - currentLen

-- Determine how many events fit in current batch
local eventsForCurrentBatch = {}
local eventsForOverflow = {}

if #eventsToAdd <= capacity then
  -- All events fit in current batch
  eventsForCurrentBatch = eventsToAdd
else
  -- Split events between current batch and overflow
  for i = 1, capacity do
    table.insert(eventsForCurrentBatch, eventsToAdd[i])
  end
  for i = capacity + 1, #eventsToAdd do
    table.insert(eventsForOverflow, eventsToAdd[i])
  end
end

-- Add events to current batch
local finalLen = currentLen
if #eventsForCurrentBatch > 0 then
  finalLen = redis.call("RPUSH", batchKey, unpack(eventsForCurrentBatch))
end

for i = 1, #idsToAdd do
  local target = batchID
  if i > #eventsForCurrentBatch then
    target = overflowULID
  end
  redis.call("SET", string.format(idemKeyFmt, prefix, idsToAdd[i]), target, "EX", idempotenceSetTTL)
end

-- Check batch size limit
local batchMemorySize = redis.call("MEMORY", "USAGE", batchKey) or 0

-- Determine the result status
local status = "append"
local nextBatchID = nil
local overflowCount = 0

-- Check if this was the first item(s) in a new batch
if currentLen == 0 then
  status = "new"
end

-- Check if batch is full (count or size limit)
local batchFull = finalLen >= batchLimit or batchMemorySize >= batchSizeLimit

if batchFull then
  -- NOTE: We intentionally do NOT set status to "started" here.
  -- The batch pointer is updated to prevent new items from being added,
  -- and start.lua will set status to "started" when execution begins.
  -- Setting it here would cause start.lua to skip execution thinking
  -- the batch is already running.

  -- Check if we have overflow events
  if #eventsForOverflow > 0 then
    -- Create new batch for overflow
    update_pointer(batchPointerKey, overflowULID)
    nextBatchID = overflowULID

    -- Set up new batch
    local overflowBatchKey = string.format(keyfmt, prefix, overflowULID)
    local overflowMetadataKey = string.format("%s:metadata", overflowBatchKey)

    -- Add overflow events to new batch
    redis.call("RPUSH", overflowBatchKey, unpack(eventsForOverflow))
    set_batch_status(overflowMetadataKey, batchStatusAppending)

    overflowCount = #eventsForOverflow
    status = "overflow"
  else
    -- No overflow, just rotate the pointer for next batch
    update_pointer(batchPointerKey, overflowULID)

    if batchMemorySize >= batchSizeLimit then
      status = "maxsize"
    else
      status = "full"
    end
  end
end

return encode_result({
  status = status,
  batchID = batchID,
  batchPointerKey = batchPointerKey,
  committed = #eventsToAdd,
  duplicates = duplicateCount,
  nextBatchID = nextBatchID,
  overflowCount = overflowCount,
  committedBytes = committedBytes
})
