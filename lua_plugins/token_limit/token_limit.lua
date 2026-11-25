-- token_limit.lua
-- AI Token 限流插件：按时间滑动窗口限制每个 Consumer 的 token 消耗总量，
-- 防止突发调用打爆大模型成本预算。
--
-- 设计（对应文档"AI-token限流插件"，保留三阶段思路，修正实现缺陷）：
--   access      预估 token（prompt + max_tokens 保守预留），Redis 单脚本原子完成
--               「清理过期 + 汇总已用 + 判额 + 预占」，超限直接 429 不请求大模型；
--               检查与预占必须同一脚本原子完成：分开两步时并发请求会同时通过
--               检查全部预占成功，导致超扣击穿。
--   body_filter 只读旁路解析响应（SSE 流 / 非流 JSON），抓真实 usage 缓存到 ctx，
--               绝不改动响应字节（改写会丢 [DONE] 帧、与 ai_proxy 的改写打架）；
--   log         单脚本原子修正记账：删除预占条目，有真实 usage 记真实值，
--               没抓到按策略保留估算值（防"断连白嫖"）。
--               注意：log_by_lua 上下文禁用 cosocket，修正动作通过 ngx.timer
--               异步执行（timer 上下文允许 cosocket，修正最终一致即可）。
--
-- 为什么用 Redis 不用 shared_dict（文档分析）：
--   单机 shared_dict 可行，但多实例集群必须 Redis；
--   worker 重启 / 网关滚动升级 shared_dict 数据直接丢。
-- 滑动窗口数据结构：ZSET，score=请求时间戳，member=request_id:token数。
--
-- 挂载方式（见 nginx.conf）：
-- lua-nginx-module 每个阶段只允许一个 *_by_lua_block，
-- 必须与 ai_proxy 在同一 block 内链式调用（限流在 ai_proxy 之后）：
--   access_by_lua_block {
--       require("ai_proxy.ai_proxy").access()
--       require("token_limit.token_limit").access()
--   }
--   body_filter_by_lua_block {
--       require("ai_proxy.ai_proxy").body_filter()
--       require("token_limit.token_limit").body_filter()
--   }
--   log_by_lua_block { require("token_limit.token_limit").log() }
-- 自检接口：_M.test()，可挂任意 content_by_lua location

local cjson = require "cjson"
local redis = require "resty.redis"

local _M = {}

-- ---------------------------------------------------------------------------
-- 可调配置
-- ---------------------------------------------------------------------------
local WINDOW_SEC    = 60            -- 滑动窗口长度（秒）
local QUOTA         = 20000         -- 窗口内每个 consumer 的 token 总配额
local REDIS_HOST    = "127.0.0.1"
local REDIS_PORT    = 6379
local KEY_PREFIX    = "token_limit:sliding:"
local REDIS_TIMEOUT = 200           -- Redis 超时（ms），网关路径上必须短
local POOL_SIZE     = 100           -- 每 worker 连接池上限
local POOL_IDLE     = 10000         -- 连接池空闲时间（ms）

-- Redis 不可用时的策略：true=放行（fail-open 保可用）；false=拒绝（fail-close 保成本）
local FAIL_OPEN = true

-- 没抓到真实 usage（上游报错 / 流中断 / 客户端断连）时的计费策略：
--   true  = 保留估算预占（成本优先，防"断连白嫖"，推荐）
--   false = 删除预占不计费（文档原方案；对恶意客户端等于免费打 LLM）
local CHARGE_ON_MISSING_USAGE = true

-- 请求没带 max_tokens 时的输出预留（保守预占）
local DEFAULT_OUT_TOKENS = 512

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
-- 返回：预占总量, prompt 估算值
local function estimate_request(req)
    local sum = 0
    for _, msg in ipairs(req.messages or {}) do
        sum = sum + estimate_tokens(msg.content)
    end
    local max_tokens = tonumber(req.max_tokens)
    local reserve_out = (max_tokens and max_tokens > 0) and max_tokens or DEFAULT_OUT_TOKENS
    return sum + reserve_out, sum
end

local function redis_conn()
    local r = redis:new()
    r:set_timeout(REDIS_TIMEOUT)
    local ok, err = r:connect(REDIS_HOST, REDIS_PORT)
    if not ok then
        ngx.log(ngx.ERR, "token_limit: redis connect fail: ", err)
        return nil
    end
    return r
end

-- ---------------------------------------------------------------------------
-- Redis 脚本（长字符串避免 Lua 转义地狱）
-- ---------------------------------------------------------------------------

-- 预检 + 预占原子脚本：
-- 文档原版是先 eval 检查（ZCARD 数条数）再单独 zadd 预占，两个问题：
--   1) ZCARD 统计的是"请求条数"，和 token 配额维度不一致（文档自查逻辑
--      写的是"把 token 加起来"，但代码用的是 ZCARD）；
--   2) 检查与写入不原子，并发请求同时通过检查后全部预占成功 -> 超扣击穿。
local PRE_RESERVE_SCRIPT = [==[
local key          = KEYS[1]
local window_start = tonumber(ARGV[1])
local pre          = tonumber(ARGV[2])
local quota        = tonumber(ARGV[3])
local member       = ARGV[4]   -- request_id:token数
local now          = tonumber(ARGV[5])
local expire       = tonumber(ARGV[6])

-- 1. 清理窗口外旧记录
redis.call('ZREMRANGEBYSCORE', key, 0, window_start)

-- 2. 汇总窗口内已用 token（token 数编码在 member 尾部）
local members = redis.call('ZRANGE', key, 0, -1)
local used = 0
for i = 1, #members do
    local t = string.match(members[i], ':(%d+)$')
    if t then used = used + tonumber(t) end
end

-- 3. 原子判定 + 预占
if used + pre > quota then
    return 0
end
redis.call('ZADD', key, now, member)
redis.call('EXPIRE', key, expire)
return 1
]==]

-- log 阶段修正记账原子脚本：
-- 文档原版 ZSCAN 只扫一次（COUNT 100）不带游标循环，匹配多时会漏删预占条目
local CORRECT_SCRIPT = [==[
local key          = KEYS[1]
local window_start = tonumber(ARGV[1])
local rid          = ARGV[2]
local real         = tonumber(ARGV[3])
local now          = tonumber(ARGV[4])
local expire       = tonumber(ARGV[5])
local policy       = ARGV[6]   -- keep=保留估算 / free=不计费
local estimate     = tonumber(ARGV[7])

redis.call('ZREMRANGEBYSCORE', key, 0, window_start)

-- 删除 access 阶段预占的条目（member 前缀 rid:），ZSCAN 需游标循环
local cursor = '0'
repeat
    local res = redis.call('ZSCAN', key, cursor, 'MATCH', rid .. ':*', 'COUNT', 100)
    cursor = res[1]
    local arr = res[2]
    for i = 1, #arr, 2 do
        redis.call('ZREM', key, arr[i])
    end
until cursor == '0'

-- 写入最终记账值
local final_tokens = 0
if real > 0 then
    final_tokens = real
elseif policy == 'keep' then
    final_tokens = estimate
end
if final_tokens > 0 then
    redis.call('ZADD', key, now, rid .. ':' .. final_tokens)
end
redis.call('EXPIRE', key, expire)
return final_tokens
]==]

-- ---------------------------------------------------------------------------
-- 阶段一：access —— 预估 + 原子预占
-- ---------------------------------------------------------------------------
function _M.access()
    local ctx = ngx.ctx

    -- 1. consumer 身份：优先复用 ai_proxy 鉴权出的 gateway_token；
    --    也支持 header / IP 维度兜底（对应文档标题 Consumer/IP/Header）
    local consumer = ctx.gateway_token or ngx.var.http_x_consumer_id or ngx.var.remote_addr
    if not consumer or consumer == "" then
        return err_json(ngx.HTTP_UNAUTHORIZED, "no consumer identity for token limit")
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
    --    只能走估算兜底（文档完全没提这一步）
    if req.stream == true and type(req.stream_options) ~= "table" then
        req.stream_options = { include_usage = true }
        local ok_set, err = ngx.req.set_body_data(cjson.encode(req))
        if not ok_set then
            ngx.log(ngx.WARN, "token_limit: set_body_data fail: ", tostring(err))
        else
            ngx.log(ngx.INFO, "token_limit: 已注入 stream_options.include_usage")
        end
    end

    -- 4. 预估 + 原子预占
    local pre_total, prompt_tokens = estimate_request(req)
    local r = redis_conn()
    if not r then
        -- fail-open：Redis 挂了按策略放行（不做预占，log 阶段同样跳过）
        ngx.log(ngx.ERR, "token_limit: redis 不可用，FAIL_OPEN=", tostring(FAIL_OPEN))
        if not FAIL_OPEN then
            return err_json(ngx.HTTP_INTERNAL_SERVER_ERROR,
                "token limit backend unavailable", "server_error")
        end
        return
    end

    local now = ngx.time()
    local rid = ngx.var.request_id
    local redis_key = KEY_PREFIX .. consumer
    -- 注意：eval 返回 nil 才是错误，返回 0 是"超限"（0 在 Lua 里是 falsy，
    -- 文档原版用 if not resp 判断错误，导致超限分支永远不可达、限流失效）
    local res, err = r:eval(PRE_RESERVE_SCRIPT, 1, redis_key,
        now - WINDOW_SEC, pre_total, QUOTA, rid .. ":" .. pre_total, now, WINDOW_SEC * 2)
    if res == nil then
        ngx.log(ngx.ERR, "token_limit: pre-reserve eval fail: ", err)
        r:close()
        return -- fail-open
    end
    r:set_keepalive(POOL_SIZE, POOL_IDLE)

    if res == 0 then
        return err_json(ngx.HTTP_TOO_MANY_REQUESTS, "token quota exceeded",
            "insufficient_quota", "token_quota_exceeded",
            { quota = QUOTA, pre_estimate = pre_total, window_sec = WINDOW_SEC })
    end

    -- 预占成功，记录上下文给 body_filter / log 阶段用
    ctx.tl_reserved     = true
    ctx.tl_consumer_key = consumer
    ctx.tl_pre_total    = pre_total
    ctx.tl_prompt_token = prompt_tokens
    ctx.tl_stream       = req.stream == true
    ctx.tl_rid          = rid

    ngx.log(ngx.INFO, "token_limit: consumer=[", consumer, "] 预占 ", pre_total,
        " token（prompt 估 ", prompt_tokens, "）")
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
        ctx.tl_real_total = total
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
    if not ctx.tl_reserved then return end

    local chunk, eof = ngx.arg[1], ngx.arg[2]

    if ctx.tl_stream then
        -- 流式：旁路扫描，缓冲只留未闭合的半帧，绝不回写 ngx.arg[1]
        local buf = (ctx.tl_sse_buf or "") .. (chunk or "")
        -- \r\n 行尾归一为 \n（JSON 内不会出现裸 \r），兼容 CRLF 上游
        buf = buf:gsub("\r", "")
        ctx.tl_sse_buf = ""
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
        ctx.tl_sse_buf = buf
    else
        -- 非流式：累积完整 JSON，eof 时一次性解析
        ctx.tl_json_buf = (ctx.tl_json_buf or "") .. (chunk or "")
        if eof then
            local ok, obj = pcall(cjson.decode, ctx.tl_json_buf)
            if ok and type(obj) == "table" then
                capture_usage(obj.usage, ctx)
            end
            ctx.tl_json_buf = nil
        end
    end
end

-- ---------------------------------------------------------------------------
-- 阶段三：log —— 调度 ngx.timer 异步修正记账
-- （log_by_lua 禁用 cosocket，不能直接连 Redis，见文件内 log() 注释）
-- ---------------------------------------------------------------------------
-- 坑：log_by_lua 上下文禁用 cosocket API（ngx.socket.tcp 会报
-- "API disabled in the context of log_by_lua*"），文档原方案在 log 阶段
-- 直接连 Redis 跑不通；标准解法是 log 阶段读 ctx 快照后 ngx.timer.at
-- 调度一个异步任务，timer 上下文允许 cosocket，修正最终一致即可。

-- 在 timer 上下文中执行 Redis 修正记账（cosocket 可用）
local function correct_usage(s)
    local r = redis_conn()
    if not r then
        ngx.log(ngx.ERR, "token_limit: 修正记账时 redis 不可用，预占记录保留")
        return
    end

    local policy = CHARGE_ON_MISSING_USAGE and "keep" or "free"
    -- 返回 0 是合法结果（free 策略删预占后没写新记录），nil 才是错误
    local final_tokens, err = r:eval(CORRECT_SCRIPT, 1, KEY_PREFIX .. s.consumer,
        ngx.time() - WINDOW_SEC, s.rid, s.real_total, ngx.time(),
        WINDOW_SEC * 2, policy, s.pre_total)
    if final_tokens == nil then
        ngx.log(ngx.ERR, "token_limit: correct eval fail: ", err)
        r:close()
        return
    end
    r:set_keepalive(POOL_SIZE, POOL_IDLE)

    ngx.log(ngx.INFO, "token_limit: consumer=[", s.consumer, "] 预占 ",
        s.pre_total, " -> 实际 ", s.real_total, "，最终记账 ", final_tokens)
end

function _M.log()
    local ctx = ngx.ctx
    if not ctx.tl_reserved then return end

    -- 快照 ctx 数据：timer 回调运行在独立协程，拿不到原请求的 ngx.ctx
    local snapshot = {
        consumer   = ctx.tl_consumer_key,
        rid        = ctx.tl_rid,
        pre_total  = ctx.tl_pre_total or 0,
        real_total = tonumber(ctx.tl_real_total) or 0,
    }

    local ok, err = ngx.timer.at(0, function(premature)
        if premature then return end
        correct_usage(snapshot)
    end)
    if not ok then
        ngx.log(ngx.ERR, "token_limit: ngx.timer.at fail: ", err)
    end
end

-- ---------------------------------------------------------------------------
-- 自检接口：模仿 test 插件，验证插件环境是否就绪
-- ---------------------------------------------------------------------------
function _M.test()
    ngx.log(ngx.INFO, "=== token_limit 插件自检 ===")
    ngx.header["Content-Type"] = "application/json"

    local status = { code = 0, msg = "token_limit plugin ok", config = {
        window_sec = WINDOW_SEC,
        quota = QUOTA,
        redis = REDIS_HOST .. ":" .. REDIS_PORT,
        fail_open = FAIL_OPEN,
        charge_on_missing_usage = CHARGE_ON_MISSING_USAGE,
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
        hint = "access/body_filter 必须挂在 ai_proxy 之后（复用 gateway_token 与 OpenAI 标准响应）" }

    ngx.say(cjson.encode(status))
end

return _M
