-- Applies one newly-persisted event to real-time state atomically.
-- One round trip per event; Redis runs the script without interleaving other
-- commands, so concurrent processors never observe a half-applied event.
--
-- KEYS[1] applied marker      ss:rt:applied:{event_id}
-- KEYS[2] second bucket       ss:rt:sec:{event_second}
-- KEYS[3] active users zset   ss:rt:active_users
-- KEYS[4] user state hash     ss:rt:user:{user_id}
-- KEYS[5] user rate counter   ss:rl:user:{user_id}:{now_second}
--
-- ARGV[1]  dedup ttl (s)
-- ARGV[2]  bucket EXPIREAT (unix s), 0 = do not write bucket (too old / too new)
-- ARGV[3]  event type
-- ARGV[4]  region
-- ARGV[5]  revenue in cents (purchases only, else 0)
-- ARGV[6]  event time (ms), already clamped to <= now
-- ARGV[7]  user id
-- ARGV[8]  active window (ms)
-- ARGV[9]  now (ms, processor wall clock)
-- ARGV[10] user state ttl (s)
-- ARGV[11] rate limit per window, 0 = disabled
-- ARGV[12] rate counter ttl (s)
-- ARGV[13] active users key ttl (s)
--
-- Returns {applied (0|1), rate_limited (0|1)}

if not redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1]) then
  return {0, 0}
end

local etype   = ARGV[3]
local ts      = tonumber(ARGV[6])
local now     = tonumber(ARGV[9])
local active  = tonumber(ARGV[8])
local uid     = ARGV[7]

-- Per-user fixed-window rate limit (shared across all processor instances).
local limited = 0
local limit = tonumber(ARGV[11])
if limit > 0 then
  local n = redis.call('INCR', KEYS[5])
  if n == 1 then
    redis.call('EXPIRE', KEYS[5], ARGV[12])
  end
  if n > limit then
    limited = 1
  end
end

-- Rolling metrics: event-time second bucket.
local expat = tonumber(ARGV[2])
if expat > 0 then
  local b = KEYS[2]
  redis.call('HINCRBY', b, 'events', 1)
  redis.call('HINCRBY', b, 'type:' .. etype, 1)
  redis.call('HINCRBY', b, 'region:' .. ARGV[4], 1)
  if etype == 'PURCHASE' then
    redis.call('HINCRBY', b, 'purchases', 1)
    redis.call('HINCRBY', b, 'revenue_cents', ARGV[5])
  elseif etype == 'PAYMENT_SUCCESS' then
    redis.call('HINCRBY', b, 'pay_ok', 1)
  elseif etype == 'PAYMENT_FAILURE' then
    redis.call('HINCRBY', b, 'pay_fail', 1)
  end
  if limited == 1 then
    redis.call('HINCRBY', b, 'rate_limited', 1)
  end
  redis.call('EXPIREAT', b, expat)
end

-- Per-user session state. Skipped for rate-limited users (hot-key protection),
-- except for a cheap flag so the throttling is visible.
local ukey = KEYS[4]
local recent = ts > (now - tonumber(ARGV[10]) * 1000)
local logged_out_at = tonumber(redis.call('HGET', ukey, 'logged_out_at_ms') or '0')

if recent then
  if limited == 1 then
    redis.call('HINCRBY', ukey, 'rate_limited', 1)
  else
    local last = tonumber(redis.call('HGET', ukey, 'last_seen_ms') or '0')
    if ts >= last then
      redis.call('HSET', ukey, 'last_seen_ms', ts, 'last_event_type', etype, 'region', ARGV[4])
    end
    if etype == 'LOGIN' and ts >= last then
      redis.call('HSET', ukey, 'logged_in', 1, 'session_start_ms', ts, 'session_events', 0)
    elseif etype == 'LOGOUT' and ts >= last then
      redis.call('HSET', ukey, 'logged_in', 0, 'logged_out_at_ms', ts)
      logged_out_at = ts
    end
    redis.call('HINCRBY', ukey, 'session_events', 1)
  end
  redis.call('EXPIRE', ukey, ARGV[10])
end

-- Active users: last-seen score, trimmed to the active window on every write.
local akey = KEYS[3]
if etype == 'LOGOUT' then
  local cur = redis.call('ZSCORE', akey, uid)
  if cur and tonumber(cur) <= ts then
    redis.call('ZREM', akey, uid)
  end
elseif ts > (now - active) and ts > logged_out_at then
  redis.call('ZADD', akey, 'GT', ts, uid)
end
redis.call('ZREMRANGEBYSCORE', akey, '-inf', '(' .. (now - active))
redis.call('EXPIRE', akey, ARGV[13])

return {1, limited}
