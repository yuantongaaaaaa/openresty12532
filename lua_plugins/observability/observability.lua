-- observability.lua
-- AI-可观测插件：采集 LLM 指标（TTFT 首 token 时延、token 消耗、错误率、流指标）
--
-- 三阶段架构（挂载于 /v1/chat/completions 现有插件链）：
--   access      初始化观测 ctx + best-effort 估算 prompt token
--   body_filter 旁路只读解析（按 Content-Type 分流 SSE / 非流式 JSON），
--               绝不改写 ngx.arg[1]，不干扰 mask / ai_proxy 的响应转换
--   log         聚合写入 shared dict + ngx.timer 节流异步推送 PushGateway，
--               错误率 / 流中断率由 Prometheus 侧 PromQL 计算
--
-- 文档原方案的关键修正（详见项目文档）：
-- 1. log 阶段禁用 cosocket：文档在 log_by_lua 里同步调 resty.http 必报
--    "API disabled in the context of log_by_lua*"，改为 ngx.timer.at(0) 异步推送
-- 2. PushGateway 是覆盖式存储：文档每请求推 counter 值 1，同 label 序列互相
--    覆盖，Prometheus 抓到的 llm_request_total 恒为 1，rate() 永远为 0，
--    错误率算不出来。改为 shared dict 累计 counter + 周期推送快照
--    （rate() 依赖单调递增样本算增量）
-- 3. gauge 同样被覆盖：TTFT 等每请求推一次只剩最后一个值。改为窗口内
--    sum/cnt 聚合，推送平均值
-- 4. 非流式响应不能当 SSE 拆帧：否则所有非流式请求 got_done=false 被标记
--    stream_break=true，错误率虚高。必须按 Content-Type 分流
-- 5. cjson.null 是 userdata（truthy）：error/content/choices/usage 全字段
--    type 守卫，否则 #delta_text 等操作直接崩溃
-- 6. token 估算必须 UTF-8 感知：Lua # 是字节数，纯中文 3 字节/字，
--    floor(3/4)=0 会估出 0 token
-- 7. SSE data 提取兼容多行 data: / data:xxx 无空格 / CRLF 行尾

local cjson = require "cjson"

local _M = {}

local PLUGIN = "[ai-obs]"
local SHM = "ai_obs_dict"

-- 旁路缓冲上限：异常上游（无 \n\n 的假 SSE / 超大 JSON）时丢弃旁路缓冲，
-- 只影响指标统计，绝不阻塞或篡改响应字节（fail-open）
local SSE_BUF_MAX  = 256 * 1024
local JSON_BUF_MAX = 4 * 1024 * 1024

-- 窗口聚合的 gauge 指标定义（key -> Prometheus 指标名 / HELP 文案）
local GAUGES = {
    { key = "ttft",             name = "llm_ttft_seconds",              help = "LLM首token时延TTFT（窗口平均，秒）" },
    { key = "stream_dur",       name = "llm_stream_duration_seconds",   help = "流持续时长，首token到[DONE]（窗口平均，秒）" },
    { key = "gen_speed",        name = "llm_gen_speed_token_per_sec",   help = "模型生成速度（窗口平均，token/s）" },
    { key = "prompt_tokens",    name = "llm_prompt_tokens",             help = "输入prompt token数（窗口平均）" },
    { key = "completion_tokens",name = "llm_completion_tokens",         help = "输出completion token数（窗口平均）" },
    { key = "total_tokens",     name = "llm_total_tokens",              help = "总token消耗（窗口平均）" },
    { key = "sse_events",       name = "llm_sse_event_count",           help = "单请求SSE事件总数（窗口平均）" },
}

-- ---------------------------------------------------------------------------
-- 内部工具函数
-- ---------------------------------------------------------------------------

-- UTF-8 感知 token 估算（demo 精度，生产可替换 tiktoken 等价实现）：
-- 多字节字符（含中文）每字符 ≈ 1 token；ASCII 每 4 字符 ≈ 1 token
local function estimate_tokens(s)
    if type(s) ~= "string" or s == "" then return 0 end
    local tokens, ascii = 0, 0
    local i, n = 1, #s
    while i <= n do
        local b = s:byte(i)
        if b < 0x80 then
            ascii = ascii + 1
            i = i + 1
        else
            tokens = tokens + 1
            if b >= 0xF0 then i = i + 4
            elseif b >= 0xE0 then i = i + 3
            elseif b >= 0xC0 then i = i + 2
            else i = i + 1 end
        end
    end
    return tokens + math.floor(ascii / 4)
end

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

-- 请求侧估算 prompt token（messages[].content / 多模态 parts / tool_calls）
-- 返回 number 或 nil（body 读不到 / 非 OpenAI 结构）
local function estimate_prompt_tokens(body_str)
    local ok, req = pcall(cjson.decode, body_str)
    if not ok or type(req) ~= "table" then return nil end
    local messages = req.messages
    if type(messages) ~= "table" then return nil end

    local total = 0
    for _, m in ipairs(messages) do
        if type(m) == "table" then
            local c = m.content
            if type(c) == "string" then
                total = total + estimate_tokens(c)
            elseif type(c) == "table" then
                -- 多模态 content：[{type="text",text=...}, ...]
                for _, part in ipairs(c) do
                    if type(part) == "table" and type(part.text) == "string" then
                        total = total + estimate_tokens(part.text)
                    end
                end
            end
            local tcs = m.tool_calls
            if type(tcs) == "table" then
                for _, tc in ipairs(tcs) do
                    -- function 是 Lua 保留字，必须下标访问
                    if type(tc) == "table" then
                        local fn = tc["function"]
                        if type(fn) == "table" then
                            if type(fn.name) == "string" then
                                total = total + estimate_tokens(fn.name)
                            end
                            if type(fn.arguments) == "string" then
                                total = total + estimate_tokens(fn.arguments)
                            end
                        end
                    end
                end
            end
        end
    end
    return total
end

-- ---------------------------------------------------------------------------
-- 阶段一：access —— 初始化观测 ctx + 估算 prompt token
-- ---------------------------------------------------------------------------
function _M.access()
    if ngx.var.observe_enable ~= "on" then return end

    local ctx = {
        start_time = ngx.now(),        -- 请求进入网关时间（高精度秒）
        req_path   = ngx.var.uri,
        req_id     = ngx.var.request_id or "",
        resp_mode  = nil,              -- 响应模式：sse/json/passthrough（body_filter 首包判定）

        -- 流指标
        ttft             = nil,        -- 首 token 时延
        first_token_time = nil,
        stream_end_time  = nil,        -- 收到 [DONE] 的时刻
        got_first_token  = false,
        got_done         = false,
        sse_event_count  = 0,

        -- token 指标
        prompt_tokens     = 0,         -- usage 优先；无 usage 用请求/响应侧估算
        completion_tokens = 0,
        total_tokens      = 0,
        has_usage         = false,     -- 响应是否带 usage 帧
        usage_p           = nil,       -- usage 原始字段（可能只回 total，log 阶段归一）
        usage_c           = nil,
        usage_t           = nil,

        -- 错误指标
        is_biz_error = false,          -- SSE/JSON 内 error 字段（HTTP 200 但业务失败）
        err_msg      = nil,
    }

    -- best-effort 估算 prompt token（响应带 usage 后会被精确值覆盖）
    local body = read_request_body()
    if body and body ~= "" then
        local est = estimate_prompt_tokens(body)
        if est then
            ctx.prompt_tokens = est
            ctx.total_tokens = est
        end
    end

    ngx.ctx.observe_ctx = ctx
end

-- ---------------------------------------------------------------------------
-- 阶段二：body_filter —— 旁路只读分析（绝不改写 ngx.arg[1]）
-- ---------------------------------------------------------------------------

-- 分析一条完整 SSE 事件（ev 不含尾部 \n\n）
local function analyze_sse_event(ctx, ev)
    -- 提取全部 data 行（兼容多行合并、data:/data:xxx、行尾 \r 已归一）；
    -- 含 event:/id:/注释行的事件不做分析
    local parts = {}
    for line in ev:gmatch("[^\r\n]+") do
        local d = line:match("^data:%s?(.*)$")
        if not d then return end
        parts[#parts + 1] = d
    end
    local data = table.concat(parts, "\n")
    if data == "" then return end

    if data == "[DONE]" then
        ctx.got_done = true
        ctx.stream_end_time = ngx.now()
        return
    end

    local ok, obj = pcall(cjson.decode, data)
    if not ok or type(obj) ~= "table" then return end

    -- 业务错误：SSE 内带 error 字段（type 守卫挡住 cjson.null 误判）
    if type(obj.error) == "table" then
        ctx.is_biz_error = true
        ctx.err_msg = cjson.encode(obj.error)
        return
    end

    -- usage 帧优先（最精准）
    local usage = obj.usage
    if type(usage) == "table" then
        ctx.has_usage = true
        if type(usage.prompt_tokens) == "number" then ctx.usage_p = usage.prompt_tokens end
        if type(usage.completion_tokens) == "number" then ctx.usage_c = usage.completion_tokens end
        if type(usage.total_tokens) == "number" then ctx.usage_t = usage.total_tokens end
    end

    -- choices[1].delta 文本（content + tool_calls arguments 都算"吐字"）
    local choices = obj.choices
    if type(choices) ~= "table" then return end
    local choice1 = choices[1]
    if type(choice1) ~= "table" then return end
    local delta = choice1.delta
    if type(delta) ~= "table" then return end

    local text = ""
    if type(delta.content) == "string" then text = delta.content end
    local tcs = delta.tool_calls
    if type(tcs) == "table" and type(tcs[1]) == "table" then
        local fn = tcs[1]["function"]
        if type(fn) == "table" and type(fn.arguments) == "string" then
            text = text .. fn.arguments
        end
    end
    if text == "" then return end

    -- 首 token：TTFT
    if not ctx.got_first_token then
        ctx.got_first_token = true
        ctx.first_token_time = ngx.now()
        ctx.ttft = ctx.first_token_time - ctx.start_time
    end

    -- 无 usage 时兜底估算（usage 帧出现后停止估算，由精确值覆盖）
    if not ctx.has_usage then
        ctx.completion_tokens = ctx.completion_tokens + estimate_tokens(text)
        ctx.total_tokens = ctx.prompt_tokens + ctx.completion_tokens
    end
end

local function analyze_sse_chunk(ctx, bc, chunk, eof)
    local buf = bc.buf .. (chunk or "")
    if buf ~= "" then
        buf = buf:gsub("\r", "")   -- CRLF 归一（JSON 字符串内 \r 都是转义字面量，安全）
        while true do
            local pos = buf:find("\n\n", 1, true)
            if not pos then break end
            local ev = buf:sub(1, pos - 1)
            buf = buf:sub(pos + 2)
            ctx.sse_event_count = ctx.sse_event_count + 1
            analyze_sse_event(ctx, ev)
        end
    end
    if #buf > SSE_BUF_MAX then buf = "" end   -- 假 SSE 超限：丢旁路缓冲（fail-open）
    bc.buf = eof and "" or buf
end

local function analyze_json_chunk(ctx, bc, chunk, eof)
    if not eof then
        bc.buf = bc.buf .. (chunk or "")
        if #bc.buf > JSON_BUF_MAX then bc.buf = "" end
        return
    end
    local full = bc.buf .. (chunk or "")
    bc.buf = ""
    if full == "" then return end

    local ok, obj = pcall(cjson.decode, full)
    if not ok or type(obj) ~= "table" then return end

    if type(obj.error) == "table" then
        ctx.is_biz_error = true
        ctx.err_msg = cjson.encode(obj.error)
        return
    end

    local usage = obj.usage
    if type(usage) == "table" then
        ctx.has_usage = true
        if type(usage.prompt_tokens) == "number" then ctx.usage_p = usage.prompt_tokens end
        if type(usage.completion_tokens) == "number" then ctx.usage_c = usage.completion_tokens end
        if type(usage.total_tokens) == "number" then ctx.usage_t = usage.total_tokens end
    end

    -- 无 usage：从 choices[0].message.content 估算 completion
    if not ctx.has_usage then
        local choices = obj.choices
        if type(choices) == "table" and type(choices[1]) == "table" then
            local msg = choices[1].message
            if type(msg) == "table" and type(msg.content) == "string" and msg.content ~= "" then
                ctx.completion_tokens = estimate_tokens(msg.content)
                ctx.total_tokens = ctx.prompt_tokens + ctx.completion_tokens
            end
        end
    end
end

function _M.body_filter()
    if ngx.var.observe_enable ~= "on" then return end
    local ctx = ngx.ctx.observe_ctx
    if not ctx then return end

    local bc = ngx.ctx.observe_bf_ctx
    if not bc then
        -- 首包：按 Content-Type 分流（非流式 JSON 绝不能被当作 SSE 拆帧）
        local ct = string.lower(ngx.header["Content-Type"] or "")
        local mode = "passthrough"
        if ct:find("text/event-stream", 1, true) then
            mode = "sse"
        elseif ct:find("application/json", 1, true) then
            mode = "json"
        end
        ctx.resp_mode = mode
        bc = { mode = mode, buf = "" }
        ngx.ctx.observe_bf_ctx = bc
    end

    local chunk = ngx.arg[1] or ""
    local eof = ngx.arg[2]

    if bc.mode == "sse" then
        analyze_sse_chunk(ctx, bc, chunk, eof)
    elseif bc.mode == "json" then
        analyze_json_chunk(ctx, bc, chunk, eof)
    end
    -- passthrough（错误页等）：不分析
    -- 三种模式都不改写 ngx.arg[1]：本插件是旁路只读，与 mask 的响应改写
    -- 互不干扰（挂链尾分析 mask 重组后的标准 OpenAI 帧）
end

-- ---------------------------------------------------------------------------
-- 阶段三：log —— 聚合 + 节流异步推送 PushGateway
-- ---------------------------------------------------------------------------

local function record_metrics(ctx, resp_status, stream_duration, token_per_sec, is_stream_break)
    local dict = ngx.shared[SHM]
    if not dict then
        ngx.log(ngx.ERR, PLUGIN, " shared dict missing: ", SHM)
        return
    end

    local path = ctx.req_path or ""
    local status = tostring(resp_status or 0)
    local bizerr = tostring(ctx.is_biz_error)
    local sb = is_stream_break == nil and "" or tostring(is_stream_break)

    -- counter：每条请求打点一次，shared dict 累计（永不清理，
    -- 推送单调递增快照，Prometheus rate() 才能算增量/错误率）
    local ckey = table.concat({ "cnt", path, status, bizerr, sb }, "|")
    dict:incr(ckey, 1, 0)

    -- gauge：窗口内 sum/cnt 聚合，推送平均值（推送成功后减去已消费部分）
    local function add(metric, value)
        if value == nil then return end
        dict:incr("s|" .. metric .. "|" .. path, value, 0)
        dict:incr("n|" .. metric .. "|" .. path, 1, 0)
    end
    add("ttft", ctx.ttft)
    add("stream_dur", stream_duration)
    add("gen_speed", token_per_sec)
    add("prompt_tokens", ctx.prompt_tokens)
    add("completion_tokens", ctx.completion_tokens)
    add("total_tokens", ctx.total_tokens)
    add("sse_events", ctx.sse_event_count)
end

-- 渲染 Prometheus 文本格式；返回 buf 与已消费的 gauge key 快照（用于精确扣减）
local function render_metrics(dict)
    local out = {}
    local consumed = {}
    local keys = dict:get_keys(0) or {}

    -- counter
    local counter_lines = {}
    for _, k in ipairs(keys) do
        local path, status, bizerr, sb = k:match("^cnt|([^|]*)|([^|]*)|([^|]*)|([^|]*)$")
        if path then
            local v = tonumber(dict:get(k)) or 0
            counter_lines[#counter_lines + 1] = string.format(
                'llm_request_total{path="%s",status="%s",biz_error="%s",stream_break="%s"} %d',
                path, status, bizerr, sb, v)
        end
    end
    if #counter_lines > 0 then
        out[#out + 1] = "# HELP llm_request_total LLM请求总计数（status/biz_error/stream_break 标签，错误率由 PromQL rate 计算）"
        out[#out + 1] = "# TYPE llm_request_total counter"
        for _, line in ipairs(counter_lines) do out[#out + 1] = line end
    end

    -- gauge：窗口平均（按 path 维度，从 sum/cnt key 收集出现过的 path）
    local paths = {}
    for _, k in ipairs(keys) do
        local p = k:match("^s|ttft|(.+)$")
        if p then paths[p] = true end
    end
    for _, k in ipairs(keys) do
        local p = k:match("^cnt|([^|]*)")
        if p then paths[p] = true end
    end
    for path in pairs(paths) do
        for _, g in ipairs(GAUGES) do
            local sk, nk = "s|" .. g.key .. "|" .. path, "n|" .. g.key .. "|" .. path
            local sum, cnt = tonumber(dict:get(sk)), tonumber(dict:get(nk))
            if sum and cnt and cnt > 0 then
                local avg = sum / cnt
                local fmt = (g.key == "ttft" or g.key == "stream_dur" or g.key == "gen_speed")
                    and "%.4f" or "%.2f"
                out[#out + 1] = "# HELP " .. g.name .. " " .. g.help
                out[#out + 1] = "# TYPE " .. g.name .. " gauge"
                out[#out + 1] = string.format(g.name .. '{path="%s"} ' .. fmt, path, avg)
                consumed[#consumed + 1] = { sk, nk, sum, cnt }
            end
        end
    end

    return table.concat(out, "\n"), consumed
end

-- timer 回调：节流推送（log 阶段禁用 cosocket，必须异步）
local function flush_to_pushgateway(premature, pushgateway, interval)
    if premature then return end
    local dict = ngx.shared[SHM]
    if not dict or pushgateway == "" then return end

    -- 节流：interval 秒内只推一次（高并发下多个请求共用一次推送）
    local now = ngx.now()
    local last = tonumber(dict:get("__last_flush") or 0)
    if now - last < interval then return end
    dict:set("__last_flush", now)

    local buf, consumed = render_metrics(dict)
    if buf == "" then return end

    local ok_http, http = pcall(require, "resty.http")
    if not ok_http then
        ngx.log(ngx.ERR, PLUGIN, " resty.http not available: ", tostring(http))
        return
    end
    local httpc = http.new()
    httpc:set_timeouts(2000, 2000, 2000)
    local res, err = httpc:request_uri(pushgateway, {
        method = "POST",
        body = buf,
        headers = { ["Content-Type"] = "text/plain; charset=utf-8" },
    })
    if not res or (res.status ~= 200 and res.status ~= 202) then
        ngx.log(ngx.ERR, PLUGIN, " pushgateway send failed: ",
            tostring(err or (res and res.status)))
        return  -- 推送失败：gauge 窗口不清，下次一并推
    end

    -- 推送成功：精确扣减已推送的 gauge 窗口（推送期间新入账的增量保留），
    -- counter 保持累计不清
    for _, item in ipairs(consumed) do
        local cur = tonumber(dict:get(item[1])) or 0
        dict:set(item[1], math.max(0, cur - item[3]))
        local cur_n = tonumber(dict:get(item[2])) or 0
        dict:set(item[2], math.max(0, cur_n - item[4]))
    end
    ngx.log(ngx.INFO, PLUGIN, " metrics pushed to pushgateway, bytes=", #buf)
end

function _M.log()
    if ngx.var.observe_enable ~= "on" then return end
    local ctx = ngx.ctx.observe_ctx
    if not ctx then return end

    -- usage 归一：优先回写精确值；只回 total 的上游用 total 反推缺失项
    -- （先补 completion 再补 prompt，保证 total = prompt + completion 恒成立）
    if ctx.usage_p then ctx.prompt_tokens = ctx.usage_p end
    if ctx.usage_c then ctx.completion_tokens = ctx.usage_c end
    if ctx.usage_t then
        ctx.total_tokens = ctx.usage_t
        if not ctx.usage_c then
            ctx.completion_tokens = math.max(0, ctx.usage_t - ctx.prompt_tokens)
        end
        if not ctx.usage_p then
            ctx.prompt_tokens = math.max(0, ctx.usage_t - ctx.completion_tokens)
        end
    end

    local resp_status = ngx.status          -- log 阶段最终状态码（比 body_filter 里记更可靠）
    local total_req_cost = ngx.now() - ctx.start_time

    -- 流指标：仅流式响应参与；非流式 stream_break 不适用（保持 nil -> 打点空标签）
    -- 中断判定只看是否收到 [DONE]（纯错误流无 content 但收 [DONE] 不算中断；
    -- got_first_token 只决定流时长/生成速度能否计算）
    local stream_duration, token_per_sec, is_stream_break = nil, nil, nil
    if ctx.resp_mode == "sse" then
        if ctx.got_done and ctx.stream_end_time then
            is_stream_break = false
            if ctx.got_first_token then
                stream_duration = ctx.stream_end_time - ctx.first_token_time
                if stream_duration > 0 and ctx.completion_tokens > 0 then
                    token_per_sec = ctx.completion_tokens / stream_duration
                end
            end
        else
            is_stream_break = true
        end
    end

    -- 结构化日志（error.log 全量明细）
    ngx.log(ngx.INFO, PLUGIN, " ", cjson.encode({
        req_id = ctx.req_id,
        path = ctx.req_path,
        mode = ctx.resp_mode,
        status = resp_status,
        ttft = ctx.ttft,
        total_req_cost = total_req_cost,
        stream_duration = stream_duration,
        token_per_sec = token_per_sec,
        prompt_tokens = ctx.prompt_tokens,
        completion_tokens = ctx.completion_tokens,
        total_tokens = ctx.total_tokens,
        sse_event_count = ctx.sse_event_count,
        got_done = ctx.got_done,
        is_stream_break = is_stream_break,
        is_biz_error = ctx.is_biz_error,
        err_msg = ctx.err_msg,
        time = ngx.localtime(),
    }))

    -- 聚合写 shared dict（log 阶段同步操作，无 cosocket 限制）
    record_metrics(ctx, resp_status, stream_duration, token_per_sec, is_stream_break)

    -- 异步节流推送（闭包传值：timer 上下文不读 ngx.ctx / ngx.var）
    local pushgateway = ngx.var.observe_pushgateway or ""
    if pushgateway == "" then return end
    local interval = tonumber(ngx.var.observe_flush_interval) or 5
    local ok, err = ngx.timer.at(0, flush_to_pushgateway, pushgateway, interval)
    if not ok then
        ngx.log(ngx.ERR, PLUGIN, " timer.at failed: ", tostring(err))
    end
end

-- ---------------------------------------------------------------------------
-- 自检接口：模仿 test 插件，验证开关变量 / shared dict / PushGateway 连通性
-- ---------------------------------------------------------------------------
function _M.test()
    ngx.log(ngx.INFO, "=== observability 插件自检 ===")
    ngx.header["Content-Type"] = "application/json"

    local status = { code = 0, msg = "observability plugin ok", config = {
        observe_enable = ngx.var.observe_enable,
        observe_pushgateway = ngx.var.observe_pushgateway,
        observe_flush_interval = ngx.var.observe_flush_interval,
    }, checks = {} }
    local checks = status.checks

    -- 1. 开关变量是否声明
    if ngx.var.observe_enable ~= nil then
        checks[#checks + 1] = { item = "nginx var $observe_enable", ok = true,
            detail = ngx.var.observe_enable }
    else
        status.code = 1
        checks[#checks + 1] = { item = "nginx var $observe_enable", ok = false,
            hint = 'server 级添加 set $observe_enable "on";' }
    end

    -- 2. shared dict 是否定义
    if ngx.shared[SHM] then
        checks[#checks + 1] = { item = "shared_dict " .. SHM, ok = true }
    else
        status.code = 1
        checks[#checks + 1] = { item = "shared_dict " .. SHM, ok = false,
            hint = "http{} 添加 lua_shared_dict " .. SHM .. " 1m;" }
    end

    -- 3. resty.http 可加载（timer 推送依赖）
    local ok_http = pcall(require, "resty.http")
    checks[#checks + 1] = { item = "resty.http 模块", ok = ok_http }
    if not ok_http then status.code = 1 end

    -- 4. PushGateway 连通性（content 阶段允许 cosocket，直接探测）
    local gw = ngx.var.observe_pushgateway or ""
    if gw ~= "" then
        local ok_gw, healthy = pcall(function()
            local http = require "resty.http"
            local httpc = http.new()
            httpc:set_timeouts(1500, 1500, 1500)
            local base = gw:match("^(.*)/metrics") or gw
            local res, err = httpc:request_uri(base .. "/-/healthy", { method = "GET" })
            if res and res.status == 200 then return true end
            -- 老版本 PushGateway 无 /-/healthy：探测根路径
            local res2 = httpc:request_uri(base .. "/", { method = "GET" })
            if res2 and (res2.status == 200 or res2.status == 404) then return true end
            return nil, err or (res and res.status)
        end)
        checks[#checks + 1] = { item = "PushGateway 连通性", ok = ok_gw and healthy or false,
            detail = tostring(gw) }
        if not (ok_gw and healthy) then status.code = 1 end
    else
        checks[#checks + 1] = { item = "PushGateway 连通性", ok = true,
            detail = "未配置 $observe_pushgateway，跳过推送（仅打结构化日志）" }
    end

    -- 5. 当前 shared dict 累计统计
    local dict = ngx.shared[SHM]
    local cnt_series, cnt_total = 0, 0
    if dict then
        local keys = dict:get_keys(0) or {}
        for _, k in ipairs(keys) do
            if k:sub(1, 4) == "cnt|" then
                cnt_series = cnt_series + 1
                cnt_total = cnt_total + (tonumber(dict:get(k)) or 0)
            end
        end
    end
    checks[#checks + 1] = { item = "已累计请求打点", ok = true,
        detail = "series=" .. cnt_series .. " total=" .. cnt_total }

    -- 6. 挂载顺序说明
    checks[#checks + 1] = { item = "挂载顺序", ok = true,
        hint = "access 挂在 mask 之后 ai_proxy 之前（覆盖限流/配额拦截等全部分支）；"
            .. "body_filter 挂链尾旁路只读（分析 mask 重组后的标准帧）；log 挂链尾" }

    ngx.say(cjson.encode(status))
end

return _M
