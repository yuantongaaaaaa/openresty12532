-- ai_history.lua
-- AI-历史对话插件：以请求头 sessionId 为会话标识，在 Redis 中维护多轮对话历史。
--   access       读取历史并拼入 prompt（system 之后、本轮消息之前）
--   body_filter  旁路只读解析模型回答（SSE 流式 / 非流式 JSON 均支持），
--                流结束后通过 ngx.timer 异步原子追加写回 Redis
--
-- Redis 数据结构：List
--   key:   llm:session:{sessionId}
--   value: 每条消息一个元素，JSON 编码 {"role":"user","content":"..."}
--   写入用 EVAL 脚本 RPUSH + LTRIM + EXPIRE 一步完成（原子）
--
-- 文档原方案的关键修正（详见交付说明）：
--   1) body_filter_by_lua 与 log_by_lua 一样禁用 cosocket：文档在
--      body_filter 里同步 red:connect 必报 "API disabled in the context
--      of body_filter_by_lua*"，历史永远写不进去。改为 [DONE]/eof 时
--      ngx.timer.at(0) 异步写（timer 上下文允许 cosocket），传值快照
--   2) GET + 合并 + SET 非原子：同 sessionId 并发请求后写覆盖先写，丢对话。
--      改为 List + EVAL 原子脚本 RPUSH/LTRIM/EXPIRE，并发追加不丢
--   3) cjson.null 是 userdata（truthy）：delta.content / messages 元素
--      必须 type 守卫，否则字符串拼接 / 字段索引直接 500
--   4) 截断必须保持偶数条：文档逐条 table.remove 奇数截断会以 assistant
--      开头，user/assistant 配对错位；这里 LTRIM 保留偶数条 + 读取侧
--      剔除首条 assistant 自愈
--   5) 文档只支持流式：非流式（stream:false）响应永远写不进历史，但
--      access 照样拼历史，会话"只读不写"。修正：按 Content-Type 分流，
--      非流式 JSON 在 eof 解析 choices[1].message.content 同样写历史
--   6) SSE 拆帧兼容 CRLF 行尾 / data:xxx 无空格 / 多行 data；[DONE]
--      容忍尾部空白
--   7) 大请求体落盘临时文件时 get_body_data 返回 nil：需读 get_body_file
--   8) sessionId 无校验：超长/特殊字符直接进 Redis key；限制 64 字节 +
--      白名单 [A-Za-z0-9_-]，非法 fail-open 跳过
--   9) 历史拼在 system 之前语义错误：修正为 system 最前、历史居中、
--      本轮其余消息在后
--   10) 无历史时不重编码回写 body：避免字节流变化影响下游插件/上游
--
-- 挂载方式（见 nginx.conf，每阶段只有一个 *_by_lua_block，必须链式调用）：
--   access_by_lua_block {
--       require("prompt_template.prompt_template").access()
--       require("mask.mask").access()
--       require("ai_history.ai_history").access()   -- mask 之后 ai_proxy 之前
--       require("observability.observability").access()
--       require("ai_proxy.ai_proxy").access()
--       ...
--   }
--   body_filter_by_lua_block {
--       ...
--       require("observability.observability").body_filter()
--       require("ai_history.ai_history").body_filter()   -- 链尾旁路只读
--   }
-- 开关（server 级 set，见 nginx.conf）：
--   set $history_enable     "on";
--   set $history_redis_host "127.0.0.1";
--   set $history_redis_port "6379";
--   set $history_redis_pass "";
--   set $history_max_msg    "20";
--   set $history_redis_ttl  "604800";
-- 自检接口：_M.test()，挂 content_by_lua location（/ai/history_selftest）

local cjson = require "cjson"

local _M = {}
local PLUGIN = "[ai-history]"

-- 旁路缓冲上限：异常上游（无 \n\n 的假 SSE / 超大 JSON）时丢弃旁路缓冲，
-- 只影响历史记录，绝不阻塞或篡改响应字节（fail-open）
local SSE_BUF_MAX  = 256 * 1024
local JSON_BUF_MAX = 4 * 1024 * 1024

-- sessionId 长度上限（防恶意超长 header 撑爆 Redis key）
local SESSION_ID_MAX = 64

-- Redis List 追加写脚本：RPUSH 本轮问答 + LTRIM 保留尾部偶数条 + EXPIRE，
-- 单脚本原子执行，并发请求追加互不覆盖
local SAVE_SCRIPT = [[
local key = KEYS[1]
redis.call('RPUSH', key, ARGV[1], ARGV[2])
local max = tonumber(ARGV[3])
if max > 0 then
    redis.call('LTRIM', key, -max, -1)
end
local ttl = tonumber(ARGV[4])
if ttl > 0 then
    redis.call('EXPIRE', key, ttl)
end
return redis.call('LLEN', key)
]]

-- ---------------------------------------------------------------------------
-- 内部工具函数
-- ---------------------------------------------------------------------------

-- 读取完整请求体（兼容超过 client_body_buffer_size 落盘临时文件的大 body）
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

-- 新建 Redis 连接（access 同步阶段 / timer 回调均可用）
local function redis_connect(host, port, pass)
    local ok_red, redis = pcall(require, "resty.redis")
    if not ok_red then
        ngx.log(ngx.ERR, PLUGIN, " resty.redis 加载失败: ", tostring(redis))
        return nil
    end
    local red = redis:new()
    red:set_timeout(1000)
    local ok, err = red:connect(host, port)
    if not ok then
        ngx.log(ngx.ERR, PLUGIN, " Redis 连接失败 ", host, ":", port, " ", tostring(err))
        return nil
    end
    if pass and pass ~= "" then
        local ok_auth, err_auth = red:auth(pass)
        if not ok_auth then
            ngx.log(ngx.ERR, PLUGIN, " Redis auth 失败: ", tostring(err_auth))
            red:close()
            return nil
        end
    end
    return red
end

-- ---------------------------------------------------------------------------
-- 阶段一：access —— 读历史拼接进 prompt
-- ---------------------------------------------------------------------------
function _M.access()
    if ngx.var.history_enable ~= "on" then return end

    -- 从请求头取会话 ID；缺失 = 无状态请求，不读不写
    local session_id = ngx.var.http_sessionid
    if not session_id or session_id == "" then return end

    -- 校验：白名单 + 长度上限，非法 fail-open 跳过（不进 Redis key）
    if #session_id > SESSION_ID_MAX or not session_id:match("^[%w%-_]+$") then
        ngx.log(ngx.WARN, PLUGIN, " sessionId 非法，跳过历史: ", session_id)
        return
    end

    -- 读取原始请求 body（此时在 mask 之后，body 已是脱敏后的 OpenAI 格式）
    local raw_body = read_request_body()
    if not raw_body or raw_body == "" then return end
    local ok_req, req = pcall(cjson.decode, raw_body)
    if not ok_req or type(req) ~= "table" then return end
    local messages = req.messages
    if type(messages) ~= "table" then return end

    -- 提取本轮用户提问（拼接历史之前；倒序找最后一条 user 字符串消息，
    -- type 守卫挡住 cjson.null / 多模态数组）
    local user_query
    for i = #messages, 1, -1 do
        local m = messages[i]
        if type(m) == "table" and m.role == "user" and type(m.content) == "string" then
            user_query = m.content
            break
        end
    end

    -- 读配置变量（access 阶段一次性读好，timer 回调拿不到 ngx.var）
    local cfg = {
        host = ngx.var.history_redis_host or "127.0.0.1",
        port = tonumber(ngx.var.history_redis_port or "6379") or 6379,
        pass = ngx.var.history_redis_pass or "",
        max_msg = math.floor(tonumber(ngx.var.history_max_msg or "20") or 20),
        ttl = tonumber(ngx.var.history_redis_ttl or "604800") or 604800,
    }
    if cfg.max_msg < 2 then cfg.max_msg = 2 end
    cfg.max_msg = cfg.max_msg - cfg.max_msg % 2   -- 偶数条：保证 user/assistant 配对

    local ctx = {
        session_id = session_id,
        redis_key = "llm:session:" .. session_id,
        user_query = user_query,
        reply = "",
        cfg = cfg,
        stream_done = false,      -- SSE 是否收到 [DONE]
        history_saved = false,    -- 防重复写入
    }
    ngx.ctx.history_ctx = ctx

    -- 读历史（Redis 不可用 fail-open：不带历史直通 LLM，不拦主链路）
    local red = redis_connect(cfg.host, cfg.port, cfg.pass)
    if not red then return end
    local hist_raw, h_err = red:lrange(ctx.redis_key, 0, -1)
    red:set_keepalive(10000, 100)
    if type(hist_raw) ~= "table" or #hist_raw == 0 then
        return   -- 无历史：body 保持原样（不重编码，避免字节流变化）
    end

    -- 逐条解码历史（坏数据剔除，fail-open）
    local history = {}
    for _, item in ipairs(hist_raw) do
        local ok_h, msg = pcall(cjson.decode, item)
        if ok_h and type(msg) == "table" and type(msg.role) == "string" then
            table.insert(history, msg)
        end
    end

    -- 截断到尾部 max_msg 条（防旧数据超限）
    if #history > cfg.max_msg then
        local drop = #history - cfg.max_msg
        for _ = 1, drop do table.remove(history, 1) end
    end
    -- 自愈配对：首条是 assistant（历史奇数截断遗留）则剔除
    while #history > 0 and history[1].role ~= "user" do
        table.remove(history, 1)
    end
    if #history == 0 then return end

    -- 重组 messages：system 最前 → 历史居中 → 本轮其余消息（含 user）在后
    local system_msgs, other_msgs = {}, {}
    for _, m in ipairs(messages) do
        if type(m) == "table" and m.role == "system" then
            table.insert(system_msgs, m)
        else
            table.insert(other_msgs, m)
        end
    end
    local new_messages = {}
    for _, m in ipairs(system_msgs) do table.insert(new_messages, m) end
    for _, h in ipairs(history) do table.insert(new_messages, h) end
    for _, m in ipairs(other_msgs) do table.insert(new_messages, m) end
    req.messages = new_messages

    -- 写回 body（set_body_data 自动更新 Content-Length；lua-nginx-module
    -- 成功时不返回值、失败时抛 Lua 异常，用 pcall 包住做 fail-open）
    local ok_set, err_set = pcall(ngx.req.set_body_data, cjson.encode(req))
    if not ok_set then
        ngx.log(ngx.ERR, PLUGIN, " set_body_data 失败: ", tostring(err_set))
        return
    end

    ngx.log(ngx.INFO, PLUGIN, " 历史加载成功 sessionId=", session_id,
        " history=", #history, " total_msgs=", #new_messages)
end

-- ---------------------------------------------------------------------------
-- 阶段二：body_filter —— 旁路只读解析回答，流结束后异步写历史
-- ---------------------------------------------------------------------------
-- body_filter_by_lua 上下文禁用 cosocket（与 log 阶段相同），不能同步连
-- Redis；[DONE]/eof 时 ngx.timer.at(0) 调度异步任务，timer 上下文允许
-- cosocket，传值快照完成写入。旁路只读：绝不改写 ngx.arg[1]。

local function maybe_save(ctx)
    if ctx.history_saved then return end
    if not ctx.user_query or ctx.reply == "" then
        ngx.log(ngx.INFO, PLUGIN, " 本轮无有效问答，不写历史 sessionId=",
            ctx.session_id)
        return
    end
    ctx.history_saved = true

    -- 快照 ctx：timer 回调运行在独立协程，拿不到原请求的 ngx.ctx
    local s = {
        redis_key = ctx.redis_key,
        session_id = ctx.session_id,
        user_query = ctx.user_query,
        reply = ctx.reply,
        host = ctx.cfg.host,
        port = ctx.cfg.port,
        pass = ctx.cfg.pass,
        max_msg = ctx.cfg.max_msg,
        ttl = ctx.cfg.ttl,
    }
    local ok, err = ngx.timer.at(0, function(premature)
        if premature then return end
        local red = redis_connect(s.host, s.port, s.pass)
        if not red then
            ngx.log(ngx.ERR, PLUGIN, " 写历史失败：Redis 不可用（本轮对话丢失，fail-open）")
            return
        end
        local len, e_err = red:eval(SAVE_SCRIPT, 1, s.redis_key,
            cjson.encode({ role = "user", content = s.user_query }),
            cjson.encode({ role = "assistant", content = s.reply }),
            s.max_msg, s.ttl)
        red:set_keepalive(10000, 100)
        if not len then
            ngx.log(ngx.ERR, PLUGIN, " 写历史 EVAL 失败: ", tostring(e_err))
        else
            ngx.log(ngx.INFO, PLUGIN, " 会话已写入 sessionId=", s.session_id,
                " key=", s.redis_key, " len=", len)
        end
    end)
    if not ok then
        ctx.history_saved = false
        ngx.log(ngx.ERR, PLUGIN, " 创建写历史 timer 失败: ", tostring(err))
    end
end

-- 解析一条完整 SSE 事件（ev 不含尾部 \n\n）：累积 delta.content，
-- 检测 [DONE] 触发写历史
local function parse_sse_event(ctx, ev)
    -- 提取全部 data 行（兼容多行 data / data:xxx 无空格 / event: 行跳过）
    local parts = {}
    for line in ev:gmatch("[^\r\n]+") do
        local d = line:match("^data:%s?(.*)$")
        if not d then return end
        parts[#parts + 1] = d
    end
    local data = table.concat(parts, "\n")
    if data == "" then return end

    if data:match("^%[DONE%]%s*$") then
        ctx.stream_done = true
        maybe_save(ctx)
        return
    end

    local ok, obj = pcall(cjson.decode, data)
    if not ok or type(obj) ~= "table" then return end
    local choices = obj.choices
    if type(choices) ~= "table" then return end
    local c1 = choices[1]
    if type(c1) ~= "table" then return end  -- usage chunk（choices 为空）
    local delta = c1.delta
    if type(delta) ~= "table" then return end
    if type(delta.content) == "string" then
        ctx.reply = ctx.reply .. delta.content
    end
end

local function sse_accum(ctx, bc, chunk, eof)
    local buf = bc.buf .. chunk
    if buf ~= "" then
        buf = buf:gsub("\r", "")   -- CRLF 行尾归一（JSON 字符串内 \r 是转义字面量，安全）
        while true do
            local pos = buf:find("\n\n", 1, true)
            if not pos then break end
            local ev = buf:sub(1, pos - 1)
            buf = buf:sub(pos + 2)
            parse_sse_event(ctx, ev)
        end
    end
    if #buf > SSE_BUF_MAX then buf = "" end   -- 假 SSE 超限：丢旁路缓冲（fail-open）
    bc.buf = eof and "" or buf
end

local function json_accum(ctx, bc, chunk, eof)
    if not eof then
        bc.buf = bc.buf .. chunk
        if #bc.buf > JSON_BUF_MAX then bc.buf = "" end
        return
    end
    local full = bc.buf .. chunk
    bc.buf = ""
    if full == "" then return end

    local ok, obj = pcall(cjson.decode, full)
    if not ok or type(obj) ~= "table" then return end
    local choices = obj.choices
    if type(choices) ~= "table" then return end
    local c1 = choices[1]
    if type(c1) ~= "table" then return end
    local msg = c1.message
    if type(msg) == "table" and type(msg.content) == "string" and msg.content ~= "" then
        ctx.reply = ctx.reply .. msg.content
        maybe_save(ctx)
    end
end

function _M.body_filter()
    if ngx.var.history_enable ~= "on" then return end
    local ctx = ngx.ctx.history_ctx
    if not ctx or ctx.history_saved then return end

    local bc = ngx.ctx.history_bf_ctx
    if not bc then
        -- 首包：按 Content-Type 分流（非流式 JSON 绝不能当作 SSE 拆帧）
        local ct = string.lower(ngx.header["Content-Type"] or "")
        local mode = "passthrough"
        if ct:find("text/event-stream", 1, true) then
            mode = "sse"
        elseif ct:find("application/json", 1, true) then
            mode = "json"
        end
        bc = { mode = mode, buf = "" }
        ngx.ctx.history_bf_ctx = bc
    end

    local chunk = ngx.arg[1] or ""
    local eof = ngx.arg[2]

    if bc.mode == "sse" then
        sse_accum(ctx, bc, chunk, eof)
    elseif bc.mode == "json" then
        json_accum(ctx, bc, chunk, eof)
    end
    -- passthrough（错误页等）：不解析
    -- 三种模式都不改写 ngx.arg[1]：旁路只读，挂链尾分析 mask 重组后的
    -- 标准 OpenAI 帧，与 mask 的响应改写互不干扰
end

-- ---------------------------------------------------------------------------
-- 自检接口：模仿 test 插件，验证开关变量 / resty.redis / Redis 连通性
-- ---------------------------------------------------------------------------
function _M.test()
    ngx.log(ngx.INFO, "=== ai_history 插件自检 ===")
    ngx.header["Content-Type"] = "application/json"

    local status = { code = 0, msg = "ai_history plugin ok", config = {
        history_enable = ngx.var.history_enable,
        history_redis_host = ngx.var.history_redis_host,
        history_redis_port = ngx.var.history_redis_port,
        history_redis_pass = ngx.var.history_redis_pass,
        history_max_msg = ngx.var.history_max_msg,
        history_redis_ttl = ngx.var.history_redis_ttl,
    }, checks = {} }
    local checks = status.checks

    -- 1. 开关变量是否声明
    if ngx.var.history_enable ~= nil then
        checks[#checks + 1] = { item = "nginx var $history_enable", ok = true,
            detail = ngx.var.history_enable }
    else
        status.code = 1
        checks[#checks + 1] = { item = "nginx var $history_enable", ok = false,
            hint = 'server 级添加 set $history_enable "on";' }
    end

    -- 2. 其余配置变量是否声明
    for _, name in ipairs({ "history_redis_host", "history_redis_port",
        "history_redis_pass", "history_max_msg", "history_redis_ttl" }) do
        if ngx.var[name] ~= nil then
            checks[#checks + 1] = { item = "nginx var $" .. name, ok = true,
                detail = ngx.var[name] }
        else
            status.code = 1
            checks[#checks + 1] = { item = "nginx var $" .. name, ok = false,
                hint = "server 级添加 set $" .. name .. ' "...";' }
        end
    end

    -- 3. resty.redis 可加载
    local ok_red = pcall(require, "resty.redis")
    checks[#checks + 1] = { item = "resty.redis 模块", ok = ok_red }
    if not ok_red then status.code = 1 end

    -- 4. Redis 连通性（content 阶段允许 cosocket，直接探测）
    local host = ngx.var.history_redis_host or "127.0.0.1"
    local port = tonumber(ngx.var.history_redis_port or "6379") or 6379
    local pass = ngx.var.history_redis_pass or ""
    local ok_conn, detail = pcall(function()
        local red = redis_connect(host, port, pass)
        if not red then return nil, "connect fail" end
        local pong = red:ping()
        red:set_keepalive(10000, 100)
        return pong
    end)
    if ok_conn and detail then
        checks[#checks + 1] = { item = "Redis 连通性", ok = true,
            detail = host .. ":" .. port .. " PING=" .. tostring(detail) }
    else
        status.code = 1
        checks[#checks + 1] = { item = "Redis 连通性", ok = false,
            detail = host .. ":" .. port .. " " .. tostring(detail) }
    end

    -- 5. 会话 key 结构与示例
    local demo_key = "llm:session:__selftest__"
    checks[#checks + 1] = { item = "Redis key 结构", ok = true,
        detail = "List: " .. demo_key .. " -> [{\"role\":\"user\",...},{\"role\":\"assistant\",...}]" }

    -- 6. 挂载顺序说明
    checks[#checks + 1] = { item = "挂载顺序", ok = true,
        hint = "access 挂在 mask 之后 ai_proxy 之前（读脱敏后的 OpenAI 请求体、"
            .. "改写 body 必须在 ai_proxy 协议转换之前）；"
            .. "body_filter 挂链尾旁路只读（分析 mask 重组后的标准帧，"
            .. "timer 异步写 Redis）" }

    ngx.say(cjson.encode(status))
end

return _M
