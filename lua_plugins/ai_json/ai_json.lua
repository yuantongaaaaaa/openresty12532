-- ai_json.lua
-- AI-JSON 插件：强制 LLM 输出标准 JSON（解决下游解析不稳定）
--
-- 设计（对应文档"AI-JSON插件"，修复实现缺陷）：
--   1) 文档 body_filter 阶段用 resty.http 同步重打上游 + 用结果覆盖响应：
--      技术上不成立。cosocket（resty.http 底层）在 body_filter/header_filter/
--      log 阶段被禁用，直接报 'API disabled in the context of
--      body_filter_by_lua*'；body_filter 阶段连 ngx.location.capture 内部
--      子请求都不可用（官方限定 rewrite/access/content 阶段）。响应流已在
--      下发途中，能做的只有改写尚未输出的响应体，不可能"同步拿到重试结果
--      再换血"。
--      修正：校验失败 -> 响应体整体替换为标准 OpenAI 错误 JSON（ai_proxy
--      的 header_filter 已清 Content-Length 自动转 chunked；状态行已发送
--      无法改，保持 200 + error 字段，业务方按 error.type 幂等重试）；
--      可选"异步补偿重试"：ngx.timer.at(0) 后台（timer 阶段 cosocket 可用）
--      重打本网关一次，结果存 shared dict，/ai/json_admin 查询。
--   2) 文档 ctx.stream = req.stream or false：stream 为 JSON null 时 cjson
--      解出 userdata（truthy），"null or false" 得到 truthy，非流式被误判
--      为流式、静默跳过校验。修正：type(req.stream)=="boolean" and 取值。
--   3) 文档校验只 pcall(cjson.decode, content) 不查类型：模型输出裸数字/
--      裸字符串/布尔也会 decode "成功"（"123"、"true" 都被当成合法 JSON）。
--      修正（文档自认的局限 3）：decode 结果必须是 table（object/array），
--      且带 X-Json-Schema 头时用内置轻量校验器做 type/properties/required/
--      items 语义校验（不引第三方 lua-json-schema）。
--   4) 文档重试只带 Content-Type，丢 Authorization 等鉴权头，真实厂商 API
--      直接 401。本方案重试重打【本网关】统一入口、透传原 Authorization
--      快照（access 阶段抓取存入 ctx），全链路鉴权/限流/配额/协议转换复用；
--      重试请求带 X-Ai-Json-Retry 头：access 跳过重复注入、body_filter
--      失败后不再递归重试（防文档自认的 token 雪球）。
--   5) 文档 messages[last_idx].role 无 type 守卫（数组里混入 JSON null 项
--      直接崩 500）；且只取最后一条消息、最后一条不是 user 时静默不注入
--      （多轮对话结尾通常是 assistant）。修正：逐条 type 守卫 + 倒序找
--      最后一条字符串 content 的 user；找不到则 messages 末尾追加一条
--      独立 user 消息，保证约束必达。
--   6) 文档 get_body_data 空即跳过：超过 client_body_buffer_size 的大 body
--      落盘临时文件时返回 nil，schema 注入就丢了。修正：get_body_file
--      兜底读全量（与 mask/prompt_template 一致）。
--   7) 文档"非流式必须 proxy_buffering on"不准确：proxy_buffering 只影响
--      nginx 的发送缓冲策略，不影响 body_filter 拿数据。本项目该 location
--      同时服务 SSE 流式必须 proxy_buffering off（缓冲会破坏流式实时性），
--      完整响应的缓冲由插件在 body_filter 内自行完成（同 mask.lua 的
--      json 模式，透明缓冲到 eof）。
--   8) 文档独立 location /v1/chat/completions + proxy_pass http://llm-upstream
--      与本项目统一入口冲突（同名 location 重复定义，服务起不来）；本项目
--      按惯例链式接入 /v1/chat/completions 的 access / body_filter 链。
--   9) 文档重试复用"注入前"的原始 body（ctx.raw_req_body 在注入之前快照），
--      重试时约束丢失、再失败概率不变；本实现重试复用【注入后】body。
--
-- 局限说明（诚实告知）：
--   - 流式 SSE 不做校验（逐 token 输出时完整 JSON 尚未成形，等全量拼完再
--     校验会毁掉流式实时性；文档也自认这层结论，完整性交业务后端收尾）。
--   - 非流式校验失败时响应流已成型，网关无法同步换血，自动重试结果通过
--     异步补偿 + /ai/json_admin 查询交付；或业务方按 error.type 自行重试。
--   - 内置 schema 校验器支持 type/properties/required/items 语法子集，
--     覆盖业务最常用的对象结构；oneOf/enum/正则等高级关键字不支持。
--
-- 挂载方式（见 nginx.conf，每阶段只有一个 *_by_lua_block，必须链式调用）：
--   access_by_lua_block {
--       ...
--       require("ai_history.ai_history").access()
--       require("ai_json.ai_json").access()      -- history 后、ai_proxy 前
--       require("observability.observability").access()
--       require("ai_proxy.ai_proxy").access()
--       ...
--   }
--   body_filter_by_lua_block {
--       ...
--       require("ai_history.ai_history").body_filter()
--       require("ai_json.ai_json").body_filter() -- 链尾兜底校验
--   }
-- 配置（server 级 set，见 nginx.conf）：
--   set $json_plugin_enable "on";        -- 总开关；off: 插件完全不生效
--   set $json_async_retry   "off";       -- 校验失败异步补偿重试（耗额外 token）
--   set $json_max_retry     "1";         -- 异步重试上限（重试请求带标记头，不递归）
--   set $json_retry_gateway "http://127.0.0.1:8080";  -- 重试入口（本网关）
-- 自检接口：_M.test()（/ai/json_selftest）
-- 重试结果查询：_M.admin()（/ai/json_admin?record_id=xxx）

local cjson = require "cjson"

local _M = {}
local PLUGIN = "[ai-json]"

local RETRY_PATH          = "/v1/chat/completions"  -- 重试请求的网关统一入口
local MAX_JSON_BUF        = 4 * 1024 * 1024         -- 非流式响应缓冲上限（超限直通保护）
local RETRY_RECORD_TTL    = 300                     -- 重试结果在 shared dict 保留秒数
local CONTENT_PREVIEW_LEN = 1024                    -- 错误响应附带的原文截断长度
local RETRY_BODY_MAX      = 8192                    -- 重试结果体存 dict 的截断长度

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

-- table 形状判定：cjson decode 的 JSON array 是 [1..#v] 连续整数键的纯
-- array-part table；JSON object 是纯 map。空表 []/{} 无法区分，返回 "empty"
--（type 匹配时对 object/array 双向放行，见 validate_schema）。
local function tbl_shape(v)
    if type(v) ~= "table" then return nil end
    local n = #v
    local has_key = false
    for k in pairs(v) do
        has_key = true
        if type(k) ~= "number" or k < 1 or k > n or k % 1 ~= 0 then
            return "object"
        end
    end
    if not has_key then return "empty" end
    local count = 0
    for _ in pairs(v) do count = count + 1 end
    if count == n then return "array" end
    return "object"
end

-- json_type：把 Lua 值归一为 JSON 类型名（cjson.null 是 userdata，特殊处理）
local function json_type(v)
    local t = type(v)
    if t == "userdata" then return "null" end
    if t == "table" then return tbl_shape(v) end
    return t
end

-- 轻量 JSON Schema 校验器：支持 type（含 type 数组、integer）/ properties /
-- required / items 递归。返回 ok, err_detail（路径 + 原因）。
-- 不支持 oneOf/enum/正则等高级关键字（见文件头局限说明）。
local function validate_schema(v, schema, path)
    path = path or "$"
    local t = json_type(v)

    -- type 校验（支持 {"string","null"} 形式的 type 数组）
    local expect = schema.type
    if type(expect) == "string" then
        expect = { expect }
    end
    if type(expect) == "table" then
        local hit = false
        for _, e in ipairs(expect) do
            if type(e) == "string" then
                if t == e
                    or (e == "integer" and t == "number" and v % 1 == 0)
                    -- cjson 无法区分空 {} 与空 []，空表对两者双向放行
                    or (t == "empty" and (e == "object" or e == "array")) then
                    hit = true
                    break
                end
            end
        end
        if not hit then
            local want = {}
            for _, e in ipairs(expect) do
                if type(e) == "string" then want[#want + 1] = e end
            end
            return false, path .. " expect type "
                .. table.concat(want, "/") .. " but got " .. t
        end
    end

    if t == "object" or t == "empty" then
        local props = schema.properties
        if type(props) == "table" then
            for k, sub in pairs(props) do
                if type(sub) == "table" and type(k) == "string" then
                    local sv = v[k]
                    local st = json_type(sv)
                    -- 缺省字段 / null 值不做属性校验（required 另行把关）；
                    -- null 是否合法交给 schema 的 type 声明（未声明 type
                    -- 的宽松模式下 null 放行）
                    if st ~= "nil" and st ~= "null" then
                        local ok, err = validate_schema(sv, sub,
                            path .. "." .. tostring(k))
                        if not ok then return false, err end
                    end
                end
            end
        end
        local required = schema.required
        if type(required) == "table" then
            for _, k in ipairs(required) do
                if type(k) == "string" then
                    local rv = v[k]
                    local rt = json_type(rv)
                    if rt == "nil" or rt == "null" then
                        return false, path .. " missing required field: "
                            .. tostring(k)
                    end
                end
            end
        end
    elseif t == "array" then
        local items = schema.items
        if type(items) == "table" then
            for i, iv in ipairs(v) do
                local ok, err = validate_schema(iv, items,
                    path .. "[" .. i .. "]")
                if not ok then return false, err end
            end
        end
    end
    return true
end

-- JSON 合法性校验：
--   1) content 必须是字符串
--   2) 语法：cjson.decode 成功 且 结果是 table（object/array）——
--      裸数字/裸字符串/布尔 decode 虽"成功"但不是合法 JSON 树（文档缺陷 3）
--   3) 语义：带 X-Json-Schema 头时按 schema 校验 required/type/properties
-- 返回 ok, err_detail
local function check_json_content(content, schema)
    if type(content) ~= "string" then
        return false, "content is not string (type=" .. type(content) .. ")"
    end
    local ok, v = pcall(cjson.decode, content)
    if not ok then
        return false, "json decode fail: " .. tostring(v)
    end
    if type(v) ~= "table" then
        return false, "decoded value is not object/array (got "
            .. json_type(v) .. ")"
    end
    if type(schema) == "table" then
        local s_ok, s_err = validate_schema(v, schema, "$")
        if not s_ok then return false, "schema validate fail: " .. s_err end
    end
    return true
end

-- 注入 schema 约束提示词：倒序找最后一条字符串 content 的 user 消息追加
-- （cjson.null 项与多模态数组 content 必须 type 守卫跳过）；找不到可拼接
-- 的 user 时在 messages 末尾追加一条独立 user 消息，保证约束必达模型。
local function inject_schema_prompt(req, schema)
    local messages = req.messages
    if type(messages) ~= "table" then
        ngx.log(ngx.WARN, PLUGIN, " messages missing, skip inject")
        return false
    end
    local prompt = "你必须严格只输出JSON，不要任何解释、markdown、```json标记。"
        .. "输出JSON需要完全符合下面的JSON Schema："
        .. cjson.encode(schema)
        .. "禁止额外文字，只返回合法JSON。"
    for i = #messages, 1, -1 do
        local item = messages[i]
        if type(item) == "table" and item.role == "user"
            and type(item.content) == "string" then
            item.content = item.content .. "\n\n" .. prompt
            return true
        end
    end
    -- 无可拼接 user（全多模态 / 无 user）：追加独立 user 消息
    messages[#messages + 1] = { role = "user", content = prompt }
    return true
end

-- 生成重试记录 id：优先 nginx 内建 $request_id（每个请求全局唯一）
local function get_record_id()
    local rid = ngx.var.request_id
    if rid and rid ~= "" then
        return "rid:" .. rid
    end
    return "rid:" .. string.format("%x", ngx.now() * 1000)
        .. ":" .. string.format("%x", math.random(0, 0x7fffffff))
end

-- 保存异步重试结果到 shared dict（TTL 过期自动回收）
local function save_retry_record(record_id, result)
    local dict = ngx.shared.ai_json_retry_dict
    if not dict then
        ngx.log(ngx.ERR, PLUGIN, " shared dict ai_json_retry_dict missing")
        return
    end
    local ok, err = dict:set(record_id, cjson.encode(result), RETRY_RECORD_TTL)
    if not ok then
        ngx.log(ngx.ERR, PLUGIN, " save retry record fail: ", tostring(err))
    end
end

-- 异步补偿重试 worker（timer 阶段 cosocket 可用）：
-- 重打本网关统一入口，带原 Authorization 快照 + X-Ai-Json-Retry 标记头。
-- 网关侧识别标记：access 跳过重复注入；body_filter 失败后不再递归重试。
local function retry_worker(premature, params)
    if premature then return end

    local result = {
        time = ngx.http_time(ngx.time()),
        status = 0,
        note = nil,
    }

    local ok_http, httpc = pcall(function()
        local http = require "resty.http"
        return http.new()
    end)
    if not ok_http then
        result.note = "resty.http unavailable: " .. tostring(httpc)
        save_retry_record(params.record_id, result)
        return
    end

    local res, err = httpc:request_uri(params.gateway .. RETRY_PATH, {
        method = "POST",
        body = params.body,
        headers = {
            ["Content-Type"] = "application/json",
            ["Authorization"] = params.auth or "",
            ["X-Ai-Json-Retry"] = "1",
        },
        ssl_verify = false,
        timeout = 60000,
    })
    httpc:close()

    if not res then
        result.note = "retry request fail: " .. tostring(err)
        save_retry_record(params.record_id, result)
        return
    end

    result.status = res.status
    local body = res.body or ""
    result.body = body:sub(1, RETRY_BODY_MAX)

    if res.status == 200 then
        -- 重试响应自身可能仍是校验失败错误体（LLM 第二次又输出非法 JSON），
        -- 用 error.type 区分"重试成功"与"仍然失败"
        local ok_j, robj = pcall(cjson.decode, body)
        if ok_j and type(robj) == "table" and type(robj.error) == "table"
            and robj.error.type == "ai_json_invalid" then
            result.note = "retry response still failed json validation"
        else
            result.note = "retry ok"
        end
    end
    save_retry_record(params.record_id, result)
end

-- ---------------------------------------------------------------------------
-- 阶段一：access —— 注入 schema 约束提示词（正向引导，从源头降低非法概率）
-- ---------------------------------------------------------------------------
function _M.access()
    if ngx.var.json_plugin_enable ~= "on" then return end

    local ctx = ngx.ctx
    local jc = {
        stream = false,
        raw_body = nil,
        injected_body = nil,          -- 注入后 body（异步重试复用，文档重试用注入前的是缺陷）
        schema = nil,                 -- X-Json-Schema 头解析出的 schema
        retry_marked = (ngx.var.http_x_ai_json_retry == "1"),
        -- 异步补偿重试配置（body_filter 失败分支用）
        async_enabled = (ngx.var.json_async_retry == "on"),
        max_retry = tonumber(ngx.var.json_max_retry or "0") or 0,
        retry_gateway = ngx.var.json_retry_gateway,
        auth = ngx.var.http_authorization,   -- timer 阶段拿不到请求头，此处快照
    }
    ctx.ai_json_ctx = jc

    local raw = read_request_body()
    if not raw or raw == "" then
        ngx.log(ngx.WARN, PLUGIN, " empty body, skip")
        return
    end
    jc.raw_body = raw

    local req
    local ok, err = pcall(function() req = cjson.decode(raw) end)
    if not ok or type(req) ~= "table" then
        -- fail-open：请求体非法交上游处理，插件不拦截
        ngx.log(ngx.ERR, PLUGIN, " request json decode fail: ", tostring(err))
        return
    end

    -- 坑：req.stream 可能是 cjson.null（userdata，truthy），
    --     "req.stream or false" 会得到 truthy 的 null，非流式被误判为流式
    jc.stream = type(req.stream) == "boolean" and req.stream or false

    -- schema 来源：请求头 X-Json-Schema（业务方携带，JSON 字符串）
    local schema_raw = ngx.var.http_x_json_schema
    if schema_raw and schema_raw ~= "" then
        local s_ok, schema = pcall(cjson.decode, schema_raw)
        if s_ok and type(schema) == "table" then
            jc.schema = schema
        else
            ngx.log(ngx.WARN, PLUGIN, " X-Json-Schema decode fail, "
                .. "fallback to syntax-only validation")
        end
    end

    if jc.retry_marked then
        -- 异步补偿重试请求：body 已注入过约束，跳过注入只做校验准备
        jc.injected_body = raw
    elseif jc.schema and inject_schema_prompt(req, jc.schema) then
        local new_body = cjson.encode(req)
        jc.injected_body = new_body
        if new_body ~= raw then
            -- set_body_data 自动更新 Content-Length（0.10.26 成功返回 nil，
            -- 失败抛异常；调用前已 read_body，此处无需 pcall 判返回值）
            ngx.req.set_body_data(new_body)
        end
        ngx.log(ngx.INFO, PLUGIN, " schema prompt injected, stream=",
            tostring(jc.stream), ", schema_len=", #cjson.encode(jc.schema))
    else
        -- 未注入（无 schema 头 / messages 缺失）：重试仍复用原 body
        jc.injected_body = raw
    end
end

-- ---------------------------------------------------------------------------
-- 阶段二：body_filter —— 兜底校验（非流式整段缓冲到 eof；流式透传）
-- ---------------------------------------------------------------------------
function _M.body_filter()
    local ctx = ngx.ctx
    local jc = ctx.ai_json_ctx
    if not jc then return end  -- access 未初始化本插件，直通

    local bf = ctx.ai_json_bf_ctx
    if not bf then
        -- 首包：读开关 + 按 Content-Type 分流
        if ngx.var.json_plugin_enable ~= "on" then
            ctx.ai_json_bf_ctx = { mode = "off" }
            return
        end
        local ct = string.lower(ngx.header["Content-Type"] or "")
        local mode = "passthrough"
        if ct:find("application/json", 1, true) then
            mode = "json"
        elseif ct:find("text/event-stream", 1, true) then
            mode = "sse"
        end
        bf = { mode = mode, buf = "" }
        ctx.ai_json_bf_ctx = bf
        if mode ~= "json" then
            return  -- 流式 / 非 JSON 响应（错误页等）直通
        end
    elseif bf.mode ~= "json" then
        return
    end

    -- 流式请求整体跳过校验（与 access 的 stream 标记双保险）
    if jc.stream then return end

    local chunk = ngx.arg[1] or ""
    local eof = ngx.arg[2]

    if not eof then
        -- 透明缓冲：整段攒到 eof 再校验，期间不输出
        bf.buf = bf.buf .. chunk
        if #bf.buf > MAX_JSON_BUF then
            -- 超限保护：已缓冲内容直通，后续转 passthrough（fail-open）
            ngx.arg[1] = bf.buf
            bf.buf = ""
            bf.mode = "passthrough"
        else
            ngx.arg[1] = ""
        end
        return
    end

    local resp_str = bf.buf .. chunk
    bf.buf = ""

    -- 解析 OpenAI 非流式结构：choices[0].message.content
    local resp = nil
    local ok_decode = pcall(function() resp = cjson.decode(resp_str) end)
    local content = nil
    if ok_decode and type(resp) == "table"
        and type(resp.choices) == "table"
        and type(resp.choices[1]) == "table"
        and type(resp.choices[1].message) == "table" then
        local msg = resp.choices[1].message
        if type(msg.content) == "string" then content = msg.content end
    end

    -- ===== 校验 =====
    local fail_reason = nil
    if not ok_decode then
        fail_reason = "upstream response is not valid json"
    elseif content == nil then
        fail_reason = "no string content in llm response"
    else
        local c_ok, c_err = check_json_content(content, jc.schema)
        if not c_ok then fail_reason = c_err end
    end

    if not fail_reason then
        -- 校验通过：完整响应原样输出
        ngx.arg[1] = resp_str
        ngx.log(ngx.INFO, PLUGIN, " json validate success")
        return
    end

    -- ===== 校验失败：替换为结构化错误响应 =====
    -- 状态行已随 header 发出（header_filter 阶段先于 body_filter），
    -- 保持 HTTP 200 + OpenAI 风格 error 字段，业务方按 error.type 识别。
    -- ai_proxy 的 header_filter 已清 Content-Length，替换体自动 chunked。
    ngx.log(ngx.WARN, PLUGIN, " json validate fail: ", fail_reason)

    local err_body = {
        error = {
            message = "LLM output failed JSON validation",
            type = "ai_json_invalid",
            code = "json_validate_failed",
            detail = fail_reason,
        },
    }
    if type(content) == "string" then
        err_body.error.original_content = content:sub(1, CONTENT_PREVIEW_LEN)
    end

    -- 异步补偿重试：同步重试在 body_filter 阶段做不到（cosocket 与
    -- location.capture 均被禁用），走 ngx.timer 通道重打本网关；
    -- 重试请求（X-Ai-Json-Retry=1）再失败时不递归，防 token 雪球
    if jc.async_enabled and not jc.retry_marked
        and jc.max_retry >= 1 and jc.injected_body
        and type(jc.retry_gateway) == "string"
        and jc.retry_gateway:match("^https?://") then

        local record_id = get_record_id()
        local ok_timer, terr = ngx.timer.at(0, retry_worker, {
            record_id = record_id,
            gateway = jc.retry_gateway,
            body = jc.injected_body,
            auth = jc.auth,
        })
        if ok_timer then
            err_body.error.record_id = record_id
            err_body.error.hint = "async compensate retry scheduled, query "
                .. "/ai/json_admin?record_id=" .. record_id
            ngx.log(ngx.INFO, PLUGIN, " async retry scheduled, record_id=",
                record_id)
        else
            ngx.log(ngx.ERR, PLUGIN, " async retry timer create fail: ",
                tostring(terr))
        end
    end

    ngx.arg[1] = cjson.encode(err_body)
end

-- ---------------------------------------------------------------------------
-- 管理接口：查询异步重试结果
--   GET /ai/json_admin?record_id=xxx  （缺参数时列出存量 record_id）
-- ---------------------------------------------------------------------------
function _M.admin()
    ngx.header["Content-Type"] = "application/json"
    local dict = ngx.shared.ai_json_retry_dict
    if not dict then
        ngx.say(cjson.encode({ code = 1,
            msg = "shared dict missing: ai_json_retry_dict" }))
        return
    end
    local record_id = ngx.var.arg_record_id
    if not record_id or record_id == "" then
        ngx.say(cjson.encode({
            code = 1, msg = "usage: GET /ai/json_admin?record_id=xxx",
            record_ids = dict:get_keys(0),
        }))
        return
    end
    local raw = dict:get(record_id)
    if not raw then
        ngx.say(cjson.encode({ code = 1,
            msg = "record not found or expired", record_id = record_id }))
        return
    end
    local ok_r, record = pcall(cjson.decode, raw)
    if not ok_r then
        ngx.say(cjson.encode({ code = 1, msg = "record corrupted" }))
        return
    end
    ngx.say(cjson.encode({ code = 0, msg = "ok", data = record }))
end

-- ---------------------------------------------------------------------------
-- 自检接口：模仿 test 插件，验证 nginx 变量 / shared dict / 校验器行为
-- ---------------------------------------------------------------------------
function _M.test()
    ngx.log(ngx.INFO, "=== ai_json 插件自检 ===")
    ngx.header["Content-Type"] = "application/json"

    local status = { code = 0, msg = "ai_json plugin ok",
        config = {
            json_plugin_enable = ngx.var.json_plugin_enable,
            json_async_retry = ngx.var.json_async_retry,
            json_max_retry = ngx.var.json_max_retry,
            json_retry_gateway = ngx.var.json_retry_gateway,
        }, checks = {} }
    local checks = status.checks

    -- 1. 开关/配置变量是否声明（未 set 的变量读出来是 nil）
    for _, name in ipairs({ "json_plugin_enable", "json_async_retry",
                            "json_max_retry", "json_retry_gateway" }) do
        if ngx.var[name] ~= nil then
            checks[#checks + 1] = { item = "nginx var $" .. name, ok = true,
                detail = tostring(ngx.var[name]) }
        else
            status.code = 1
            checks[#checks + 1] = { item = "nginx var $" .. name, ok = false,
                hint = 'server 级添加 set $' .. name .. ' "...";' }
        end
    end

    -- 2. 重试结果 shared dict
    if ngx.shared.ai_json_retry_dict then
        checks[#checks + 1] = { item = "shared_dict ai_json_retry_dict",
            ok = true }
    else
        status.code = 1
        checks[#checks + 1] = { item = "shared_dict ai_json_retry_dict",
            ok = false,
            hint = "http{} 添加 lua_shared_dict ai_json_retry_dict 1m;" }
    end

    -- 3. 语法校验样例（无 schema：仅要求 decode 成 object/array）
    local syntax_cases = {
        { name = "合法对象", input = '{"a":1}', want = true },
        { name = "合法数组", input = '[1,2,3]', want = true },
        { name = "非法JSON语法", input = '{"a":1', want = false },
        { name = "裸数字（非JSON树）", input = "123", want = false },
        { name = "裸字符串（非JSON树）", input = '"hello"', want = false },
        { name = "普通文本", input = "这不是JSON", want = false },
    }
    for _, c in ipairs(syntax_cases) do
        local ok, _ = check_json_content(c.input, nil)
        local pass = ok == c.want
        if not pass then status.code = 1 end
        checks[#checks + 1] = { item = "语法校验 " .. c.name, ok = pass,
            detail = "want=" .. tostring(c.want) .. " got=" .. tostring(ok) }
    end

    -- 4. schema 语义校验样例（文档局限 3 的补强点）
    local user_schema = {
        type = "object",
        properties = {
            name = { type = "string" },
            age = { type = "integer" },
            tags = { type = "array", items = { type = "string" } },
        },
        required = { "name" },
    }
    local schema_cases = {
        { name = "schema全满足", input = '{"name":"张三","age":30,"tags":["a","b"]}', want = true },
        { name = "缺required字段", input = '{"age":30}', want = false },
        { name = "类型不匹配", input = '{"name":"张三","age":"30"}', want = false },
        { name = "数组元素类型错", input = '{"name":"张三","tags":["a",1]}', want = false },
        { name = "嵌套子对象", input = '{"name":"张三","tags":[]}', want = true },
    }
    for _, c in ipairs(schema_cases) do
        local ok, _ = check_json_content(c.input, user_schema)
        local pass = ok == c.want
        if not pass then status.code = 1 end
        checks[#checks + 1] = { item = "schema校验 " .. c.name, ok = pass,
            detail = "want=" .. tostring(c.want) .. " got=" .. tostring(ok) }
    end

    -- 5. 挂载顺序提示
    checks[#checks + 1] = { item = "挂载顺序", ok = true,
        hint = "access 挂在 ai_history 之后、observability/ai_proxy 之前"
            .. "（基于脱敏+拼历史后的 messages 注入约束，token 估算计入注入"
            .. "部分）；body_filter 挂链尾兜底校验（替换响应不影响上游旁路"
            .. "累积到的原始 LLM 内容）" }

    ngx.say(cjson.encode(status))
end

return _M