-- quota.lua
-- AI Token 配额管理插件：给每个 Consumer 分配总 Token 额度（月度总额），
-- 用完即封禁；配套管理接口（quota_admin_api.lua）支持查询 / 设置 / 增减 / 重置。
--
-- 设计（对应文档"AI-配额管理插件"，保留三阶段思路，修正实现缺陷）：
--   access      预估 token 后 Redis 单脚本原子完成
--               「回收孤儿预占 + 判额 + 预占 + 写预占台账」，
--               超限直接 429 不请求大模型；检查与预占必须同一脚本原子完成。
--   body_filter 只读旁路解析响应（SSE 流 / 非流 JSON），抓真实 usage 缓存到 ctx，
--               绝不改动响应字节（改写会丢 [DONE] 帧、与 ai_proxy 的改写打架）。
--   log         调度 ngx.timer 异步修正记账（log_by_lua 禁用 cosocket）：
--               撤回预占，写真实消耗；没抓到真实值按策略保留估算或全额回退。
--
-- 与 token_limit（滑动窗口）的本质区别：
--   配额是"永久累计"计数器，没有滑动窗口过期自愈的能力；access 预占一旦
--   孤儿化（worker 崩溃导致 log 修正没执行）会永久占用额度。因此额外引入
--   pending ZSET 预占台账 + 惰性回收：每次 access 先回收账龄超时的孤儿预占。
--
-- Redis 存储设计（修正文档后）：
--   quota:cfg:{consumer_key}     → 该用户【月度总额】（管理接口写入，无 TTL）
--   quota:used:{consumer_key}    → 本月已消耗 token，EXPIREAT 到下月 1 日 0 点
--                                  自动过期重置（每次脚本操作后刷新，幂等）
--   quota:pending:{consumer_key} → ZSET 预占台账：member=request_id:预占值，
--                                  score=预占时间；账龄超 PENDING_TTL 视为孤儿，
--                                  由后续 access 惰性回收（文档原 pending key 设计
--                                  是半成品：access 从不写、log 只 DEL，形同虚设）
--
-- 文档缺陷修正记录：
--   1) 文档 log 阶段直接连 Redis —— log_by_lua 禁用 cosocket，会报
--      "API disabled in the context of log_by_lua*"，必须走 ngx.timer；
--   2) 文档"校验剩余配额"与"预占"分两步，并发请求同时通过检查全部预占成功
--      → 超扣击穿，必须单脚本原子（INCRBY 先占、超限回滚）；
--   3) INCRBY 新建 used key 不带 TTL；EXPIRE 按剩余秒数算跨月边界会错
--      （1 月 31 日算 29 天），必须 EXPIREAT 固定到下月 1 日 0 点；
--   4) 文档 eval 返回判定 if not resp：超限返回 0 是 falsy 会被误判为错误，
--      超限分支不可达、配额形同虚设，必须 res == nil 才是错误；
--   5) 文档"请求失败全额回退"对恶意客户端等于免费打 LLM（断连白嫖），
--      做成 CHARGE_ON_MISSING_USAGE 开关（默认保留估算，防白嫖）；
--   6) 文档消费身份取 ngx.ctx.consumer_key，与限流插件的 gateway_token 命名
--      不一致，统一复用 ai_proxy 鉴权出的 ctx.gateway_token。
--
-- 挂载方式（见 nginx.conf）：
-- lua-nginx-module 每个阶段只允许一个 *_by_lua_block，
-- 必须与 ai_proxy / token_limit 在同一 block 内链式调用（配额在限流之后）：
--   access_by_lua_block {
--       require("ai_proxy.ai_proxy").access()
--       require("token_limit.token_limit").access()
--       require("quota.quota").access()
--   }
--   body_filter_by_lua_block {
--       require("ai_proxy.ai_proxy").body_filter()
--       require("token_limit.token_limit").body_filter()
--       require("quota.quota").body_filter()
--   }
--   log_by_lua_block {
--       require("token_limit.token_limit").log()
--       require("quota.quota").log()
--   }
-- 管理接口：quota_admin_api.lua，挂 content_by_lua location（/ai/quota_admin）
-- 自检接口：_M.test()，挂 content_by_lua location（/ai/quota_selftest）

local cjson = require "cjson"
local redis = require "resty.redis"

local _M = {}

-- ---------------------------------------------------------------------------
-- 可调配置
-- ---------------------------------------------------------------------------
local REDIS_HOST    = "127.0.0.1"
local REDIS_PORT    = 6379
local KEY_PREFIX    = "quota:"       -- cfg / used / pending 三套 key 的公共前缀
local REDIS_TIMEOUT = 200            -- Redis 超时（ms），网关路径上必须短
local POOL_SIZE     = 100            -- 每 worker 连接池上限
local POOL_IDLE     = 10000          -- 连接池空闲时间（ms）

-- Redis 不可用时的策略：true=放行（fail-open 保可用）；false=拒绝（fail-close 保成本）
local FAIL_OPEN = true

-- 没抓到真实 usage（上游报错 / 流中断 / 客户端断连）时的计费策略：
--   true  = 保留估算预占（成本优先，防"断连白嫖"，推荐）
--   false = 撤回预占不计费（文档原方案；对恶意客户端等于免费打 LLM）
local CHARGE_ON_MISSING_USAGE = true

-- 请求没带 max_tokens 时的输出预留（保守预占）
local DEFAULT_OUT_TOKENS = 512

-- 预占账龄上限（秒）：access 预占后超过该时长仍未结算视为孤儿预占
-- （worker 崩溃 / log 修正未执行），由后续请求惰性回收。需大于
-- 正常请求最大时长（本地 proxy_read_timeout 600s，取 900s 保险）
local PENDING_TTL = 900

-- ---------------------------------------------------------------------------
-- 工具函数
-- ---------------------------------------------------------------------------

local function err_json(status, message, err_type, code, extra)
    ngx.status = status
    ngx.header["Content-Type"] = "application/json"
    local e = { message = message, type = err_type or "invalid_request_error" }
    if code then e.code = code end
    local body = { error = e }
    if extra then
        for k, v in pairs(extra) do body[k] = v end
    end
    ngx.say(cjson.encode(body))
    return ngx.exit(status)
end

-- 读取完整请求体（兼容超过 client_body_buffer_size 的大 body 落盘文件）
local function read_request_body()
    ngx.req.read_body()
    local body = ngx.req.get_body_data()
    if body then return body end

    local file = ngx.req.get_body_file()
    if not file then return nil end
    local f = io.open(file, "rb")
    if not f then return nil end
    local data = f:read("*a")
    f:close()
    return data
end

-- 简易 token 估算（粗估，事后用真实 usage 修正；生产可替换 tiktoken-lua）
-- 英文约 4 字符 1 token；CJK 约 2 字 1 token
local function estimate_tokens(text)
    if type(text) ~= "string" or text == "" then return 0 end
    -- 连续 2 个高位字节近似一个 CJK 字（UTF-8 汉字实际 3 字节，粗估取 2）
    local _, cn = string.gsub(text, "[\128-\255][\128-\255]", "")
    local en_bytes = #text - 2 * cn
    return math.floor(cn * 0.5 + en_bytes / 4)
end

-- 预估整个请求消耗：prompt token + 输出预留（max_tokens 或默认值）
local function estimate_request(req)
    local sum = 0
    for _, msg in ipairs(req.messages or {}) do
        sum = sum + estimate_tokens(msg.content)
    end
    local max_tokens = tonumber(req.max_tokens)
    local reserve_out = (max_tokens and max_tokens > 0) and max_tokens or DEFAULT_OUT_TOKENS
    return sum + reserve_out
end

-- 下月 1 日 0 点（服务器本地时区）的 epoch 秒，用作 used key 的重置点。
-- 为什么不用 EXPIRE + 剩余秒数：跨月边界天数不定（28/29/30/31），
-- 动态算秒数会错；EXPIREAT 固定时间戳天然幂等，每次操作后刷新无副作用。
local function next_month_epoch()
    local t = ngx.localtime()   -- "2026-11-25 12:34:56"
    local y, m = t:match("^(%d+)-(%d+)-")
    y, m = tonumber(y), tonumber(m)
    if m == 12 then
        y, m = y + 1, 1
    else
        m = m + 1
    end
    return os.time({ year = y, month = m, day = 1, hour = 0, min = 0, sec = 0 })
end

local function redis_conn()
    local r = redis:new()
    r:set_timeout(REDIS_TIMEOUT)
    local ok, err = r:connect(REDIS_HOST, REDIS_PORT)
    if not ok then
        ngx.log(ngx.ERR, "quota: redis connect fail: ", err)
        return nil
    end
    return r
end

-- ---------------------------------------------------------------------------
-- Redis 脚本（长字符串避免 Lua 转义地狱）
-- ---------------------------------------------------------------------------

-- 预检 + 预占 + 孤儿回收原子脚本：
-- 文档原版是"先查剩余配额（GET），够再扣"两步走，并发请求会同时通过检查
-- 全部预占成功 → 超扣击穿；这里 INCRBY 先占、超限回滚，单脚本原子。
local QUOTA_RESERVE_SCRIPT = [==[
local cfg_key     = KEYS[1]
local used_key    = KEYS[2]
local pending_key = KEYS[3]
local pre         = tonumber(ARGV[1])
local now         = tonumber(ARGV[2])
local expire_at   = tonumber(ARGV[3])
local pending_ttl = tonumber(ARGV[4])
local member      = ARGV[5]   -- request_id:预占值

-- 1. 惰性回收账龄超时的孤儿预占（worker 崩溃导致 log 修正没执行留下的）。
--    滑动窗口限流有"窗口过期自愈"，配额永久累计没有，必须主动回收。
local stale = redis.call('ZRANGEBYSCORE', pending_key, 0, now - pending_ttl)
for i = 1, #stale do
    local pre_old = string.match(stale[i], ':(%d+)$')
    if pre_old then
        redis.call('ZREM', pending_key, stale[i])
        local used = tonumber(redis.call('GET', used_key) or 0)
        if used >= tonumber(pre_old) then
            redis.call('INCRBY', used_key, 0 - tonumber(pre_old))
        else
            redis.call('SET', used_key, 0)   -- clamp 防负（reset 后残留台账）
        end
    end
end
if redis.call('ZCARD', pending_key) == 0 then
    redis.call('DEL', pending_key)
end

-- 2. 未配置额度的 consumer 不限额（放行且不预占，used 不新建）
local cfg = tonumber(redis.call('GET', cfg_key) or 0)
if not cfg or cfg <= 0 then
    return -1
end

-- 3. 原子判额 + 预占：INCRBY 先占，超了回滚（保留 TTL）
local used = tonumber(redis.call('INCRBY', used_key, pre))
redis.call('EXPIREAT', used_key, expire_at)
if used > cfg then
    redis.call('INCRBY', used_key, 0 - pre)
    return 0
end

-- 4. 写预占台账（供 log 修正撤回 + 孤儿惰性回收）
redis.call('ZADD', pending_key, now, member)
return 1
]==]

-- log 阶段修正记账原子脚本：
-- 撤回预占 + 写真实消耗。ZREM 返回 1 说明预占还在；返回 0 说明已被
-- 惰性回收（超长请求超过 PENDING_TTL），此时不再撤回避免重复扣减。
local QUOTA_CORRECT_SCRIPT = [==[
local used_key    = KEYS[1]
local pending_key = KEYS[2]
local member      = ARGV[1]
local pre         = tonumber(ARGV[2])
local real        = tonumber(ARGV[3])
local expire_at   = tonumber(ARGV[4])
local policy      = ARGV[5]   -- keep=保留估算 / free=不计费

-- 撤回预占（若台账里还在）
local removed = redis.call('ZREM', pending_key, member)
if redis.call('ZCARD', pending_key) == 0 then
    redis.call('DEL', pending_key)
end
if removed == 1 then
    local used = tonumber(redis.call('GET', used_key) or 0)
    if used >= pre then
        redis.call('INCRBY', used_key, 0 - pre)
    else
        redis.call('SET', used_key, 0)
    end
end

-- 写入最终记账值
local final = 0
if real > 0 then
    final = real
elseif policy == 'keep' then
    final = pre
end
if final > 0 then
    redis.call('INCRBY', used_key, final)
end
redis.call('EXPIREAT', used_key, expire_at)
return final
]==]

-- ---------------------------------------------------------------------------
-- 阶段一：access —— 预估 + 原子预占（含孤儿预占惰性回收）
-- ---------------------------------------------------------------------------
function _M.access()
    local ctx = ngx.ctx

    -- 1. consumer 身份：优先复用 ai_proxy 鉴权出的 gateway_token；
    --    也支持 header / IP 维度兜底（与 token_limit 保持一致）
    local consumer = ctx.gateway_token or ngx.var.http_x_consumer_id or ngx.var.remote_addr
    if not consumer or consumer == "" then
        return err_json(ngx.HTTP_UNAUTHORIZED, "no consumer identity for quota")
    end

    -- 2. 拿 OpenAI 格式请求体（ai_proxy 已解码缓存，避免重复读 body；
    --    独立使用时自己读）
    local req = ctx.openai_req
    if type(req) ~= "table" then
        local raw = read_request_body()
        if not raw or raw == "" then
            return err_json(ngx.HTTP_BAD_REQUEST, "empty body")
        end
        local ok, obj = pcall(cjson.decode, raw)
        if not ok or type(obj) ~= "table" then
            return err_json(ngx.HTTP_BAD_REQUEST, "json decode fail")
        end
        req = obj
    end

    -- 3. 流式请求注入 stream_options.include_usage：
    --    OpenAI 规范默认流式不带 usage 字段，不注入永远抓不到真实消耗，
    --    只能走估算兜底（文档完全没提这一步；token_limit 注入过则跳过）
    if req.stream == true and type(req.stream_options) ~= "table" then
        req.stream_options = { include_usage = true }
        ngx.req.set_body_data(cjson.encode(req))
    end

    -- 4. 预估 + 原子预占
    local pre_total = estimate_request(req)
    local r = redis_conn()
    if not r then
        -- fail-open：Redis 挂了按策略放行（不做预占，log 阶段同样跳过）
        ngx.log(ngx.ERR, "quota: redis 不可用，FAIL_OPEN=", tostring(FAIL_OPEN))
        if not FAIL_OPEN then
            return err_json(ngx.HTTP_INTERNAL_SERVER_ERROR,
                "quota backend unavailable", "server_error")
        end
        return
    end

    local now = ngx.time()
    local rid = ngx.var.request_id
    local member = rid .. ":" .. pre_total
    -- 注意：eval 返回 nil 才是错误；返回 0 是"超限"、-1 是"未配置额度"
    -- （0 在 Lua 里是 falsy，文档原版用 if not resp 判断错误会导致
    --   超限分支永远不可达、配额形同虚设）
    local res, err = r:eval(QUOTA_RESERVE_SCRIPT, 3,
        KEY_PREFIX .. "cfg:" .. consumer,
        KEY_PREFIX .. "used:" .. consumer,
        KEY_PREFIX .. "pending:" .. consumer,
        pre_total, now, next_month_epoch(), PENDING_TTL, member)
    if res == nil then
        ngx.log(ngx.ERR, "quota: reserve eval fail: ", err)
        r:close()
        return -- fail-open
    end
    r:set_keepalive(POOL_SIZE, POOL_IDLE)

    if res == 0 then
        return err_json(ngx.HTTP_TOO_MANY_REQUESTS, "monthly token quota exceeded",
            "insufficient_quota", "monthly_quota_exceeded",
            { pre_estimate = pre_total })
    end

    if res == -1 then
        -- 未配置月度额度：不限额放行，body_filter / log 阶段跳过
        ngx.log(ngx.INFO, "quota: consumer=[", consumer, "] 未配置月度额度，跳过限额")
        return
    end

    -- 预占成功，记录上下文给 body_filter / log 阶段用
    ctx.q_reserved  = true
    ctx.q_consumer  = consumer
    ctx.q_pre_total = pre_total
    ctx.q_rid       = rid
    ctx.q_member    = member
    ctx.q_stream    = req.stream == true

    ngx.log(ngx.INFO, "quota: consumer=[", consumer, "] 预占 ", pre_total, " token")
end

-- ---------------------------------------------------------------------------
-- 阶段二：body_filter —— 只读旁路抓真实 usage
-- ---------------------------------------------------------------------------

-- 从上游 JSON 对象里抓 usage。
-- 坑：cjson 把 JSON null 解码为 cjson.null（userdata，truthy），
-- "usage": null 时 if obj.usage 为真，log 阶段访问 .total_tokens 会抛
-- "attempt to index field (a userdata value)"，必须 type 检查
local function capture_usage(usage, ctx)
    if type(usage) ~= "table" then return end
    local total = usage.total_tokens
    if type(total) == "number" and total >= 0 then
        ctx.q_real_total = total
    end
end

-- 解析一个完整 SSE 事件（只读旁路，不修改任何输出字节）
local function scan_sse_event(ev, ctx)
    -- 兼容 "data: xxx" / "data:xxx"，多行 data 事件拼接
    local parts = {}
    for line in ev:gmatch("[^\r\n]+") do
        local d = line:match("^data:%s?(.*)$")
        if not d then return end -- 含 event:/id:/注释行，非纯 data 事件，跳过
        parts[#parts + 1] = d
    end
    local data = table.concat(parts, "\n")
    if data == "" or data == "[DONE]" then return end
    local ok, obj = pcall(cjson.decode, data)
    if not ok or type(obj) ~= "table" then return end
    capture_usage(obj.usage, ctx)
end

function _M.body_filter()
    local ctx = ngx.ctx
    if not ctx.q_reserved then return end

    local chunk, eof = ngx.arg[1], ngx.arg[2]

    if ctx.q_stream then
        -- 流式：旁路扫描，缓冲只留未闭合的半帧，绝不回写 ngx.arg[1]
        -- （回写会丢 [DONE] 帧、与 ai_proxy 的响应改写打架）
        local buf = (ctx.q_sse_buf or "") .. (chunk or "")
        -- \r\n 行尾归一为 \n（JSON 内不会出现裸 \r），兼容 CRLF 上游
        buf = buf:gsub("\r", "")
        ctx.q_sse_buf = ""
        while true do
            local pos = buf:find("\n\n", 1, true)
            if not pos then break end
            local ev = buf:sub(1, pos - 1)
            buf = buf:sub(pos + 2)
            scan_sse_event(ev, ctx)
        end
        if eof and buf ~= "" then
            scan_sse_event(buf, ctx) -- 残留半帧兜底
            buf = ""
        end
        ctx.q_sse_buf = buf
    else
        -- 非流式：累积完整 JSON，eof 时一次性解析
        ctx.q_json_buf = (ctx.q_json_buf or "") .. (chunk or "")
        if eof then
            local ok, obj = pcall(cjson.decode, ctx.q_json_buf)
            if ok and type(obj) == "table" then
                capture_usage(obj.usage, ctx)
            end
            ctx.q_json_buf = nil
        end
    end
end

-- ---------------------------------------------------------------------------
-- 阶段三：log —— 调度 ngx.timer 异步修正记账
-- ---------------------------------------------------------------------------
-- 坑：log_by_lua 上下文禁用 cosocket API（ngx.socket.tcp 会报
-- "API disabled in the context of log_by_lua*"），文档原方案在 log 阶段
-- 直接连 Redis 跑不通；标准解法是 log 阶段读 ctx 快照后 ngx.timer.at
-- 调度一个异步任务，timer 上下文允许 cosocket，修正最终一致即可。

-- 在 timer 上下文中执行 Redis 修正记账（cosocket 可用）
local function correct_usage(s)
    local r = redis_conn()
    if not r then
        -- 修正失败时预占台账保留，账龄超 PENDING_TTL 后由后续请求惰性回收
        ngx.log(ngx.ERR, "quota: 修正记账时 redis 不可用，预占台账等待惰性回收")
        return
    end

    local policy = CHARGE_ON_MISSING_USAGE and "keep" or "free"
    -- 返回 0 是合法结果（free 策略且 real=0：撤回预占后不写新值），nil 才是错误
    local final_tokens, err = r:eval(QUOTA_CORRECT_SCRIPT, 2,
        KEY_PREFIX .. "used:" .. s.consumer,
        KEY_PREFIX .. "pending:" .. s.consumer,
        s.member, s.pre_total, s.real_total, next_month_epoch(), policy)
    if final_tokens == nil then
        ngx.log(ngx.ERR, "quota: correct eval fail: ", err)
        r:close()
        return
    end
    r:set_keepalive(POOL_SIZE, POOL_IDLE)

    ngx.log(ngx.INFO, "quota: consumer=[", s.consumer, "] 预占 ",
        s.pre_total, " -> 实际 ", s.real_total, "，最终记账 ", final_tokens)
end

function _M.log()
    local ctx = ngx.ctx
    if not ctx.q_reserved then return end

    -- 快照 ctx 数据：timer 回调运行在独立协程，拿不到原请求的 ngx.ctx
    local snapshot = {
        consumer   = ctx.q_consumer,
        member     = ctx.q_member,
        pre_total  = ctx.q_pre_total or 0,
        real_total = tonumber(ctx.q_real_total) or 0,
    }

    local ok, err = ngx.timer.at(0, function(premature)
        if premature then return end
        correct_usage(snapshot)
    end)
    if not ok then
        ngx.log(ngx.ERR, "quota: ngx.timer.at fail: ", err)
    end
end

-- ---------------------------------------------------------------------------
-- 自检接口：模仿 test 插件，验证插件环境是否就绪
-- ---------------------------------------------------------------------------
function _M.test()
    ngx.log(ngx.INFO, "=== quota 插件自检 ===")
    ngx.header["Content-Type"] = "application/json"

    local status = { code = 0, msg = "quota plugin ok", config = {
        redis = REDIS_HOST .. ":" .. REDIS_PORT,
        fail_open = FAIL_OPEN,
        charge_on_missing_usage = CHARGE_ON_MISSING_USAGE,
        pending_ttl = PENDING_TTL,
        reset_point = "下月 1 日 0 点（服务器本地时区，EXPIREAT）",
    }, checks = {} }
    local checks = status.checks

    -- 1. resty.redis 模块可加载
    local ok_redis_mod = pcall(require, "resty.redis")
    checks[#checks + 1] = { item = "resty.redis 模块", ok = ok_redis_mod }

    -- 2. Redis 连通性
    local r = redis_conn()
    if r then
        local ok_ping, err = r:ping()
        if ok_ping then
            checks[#checks + 1] = { item = "Redis " .. REDIS_HOST .. ":" .. REDIS_PORT,
                ok = true }
        else
            status.code = 1
            checks[#checks + 1] = { item = "Redis ping", ok = false,
                hint = tostring(err) }
        end
        r:set_keepalive(POOL_SIZE, POOL_IDLE)
    else
        status.code = 1
        checks[#checks + 1] = { item = "Redis 连接", ok = false,
            hint = "请先启动 redis-server（默认 127.0.0.1:6379）" }
    end

    -- 3. 挂载顺序提示
    checks[#checks + 1] = { item = "挂载顺序", ok = true,
        hint = "access/body_filter 必须挂在 ai_proxy、token_limit 之后（复用 gateway_token 与 OpenAI 标准响应）" }

    ngx.say(cjson.encode(status))
end

return _M
