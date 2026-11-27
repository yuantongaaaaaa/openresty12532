-- vec_cache.lua
-- AI 向量数据库语义缓存插件：对相同/相似语义的 prompt 做缓存（非字符串匹配），
-- 命中直接返回缓存答案，避免重复调用大模型、降低成本。
--
-- 链路：客户端 -> ai_proxy（鉴权+路由）-> token_limit（限流）-> 本插件
--       -> quota（配额）-> LLM 上游
--   命中缓存：本插件在 access 阶段直接构造 OpenAI 标准响应并结束请求，
--             不再经过 quota / 上游 LLM（缓存命中不消耗月度 token 额度；
--             但已过限流预占，缓存响应带 usage 供 token_limit 修正记账，防刷）。
--   未命中  ：放行继续 proxy_pass，body_filter 只读旁路累积完整 answer，
--             log 阶段 ngx.timer 异步写入 Milvus。
--
-- 设计（对应文档"AI-向量数据库缓存插件"，修复实现缺陷）：
--   1) 文档用 content_by_lua 转发上游 —— 与 proxy_pass 互斥（content_by_lua
--      接管 content 阶段后 proxy_pass 永远不执行），且 resty.http 是缓冲式
--      调用，未命中路径的 SSE 流式被破坏；更致命的是与现有 ai_proxy
--      （变量化 proxy_pass + 协议转换）架构完全冲突。
--      修正：命中缓存在 access 阶段直接 ngx.exit(200) 输出，未命中走
--      正常 proxy_pass，与 ai_proxy / token_limit / quota 完全共存。
--   2) 文档 body_filter 无条件按 SSE 拆帧：非流式 JSON 响应不含 "\n\n"
--      会被全部吞进缓冲、客户端收不到任何字节。必须按 Content-Type 区分。
--   3) 文档 body_filter eof 时同步调 Milvus insert：阻塞最后一个响应字节
--      的 flush；改为 log 阶段 ngx.timer.at 异步写（log_by_lua 禁用
--      cosocket，但 timer 回调可用），best-effort 不影响响应。
--   4) 文档 Milvus REST 路径缺 /entities 段，search 缺 annsField 指定向量
--      字段；缓存主键用 request_id 会导致同一 prompt 无限重复入库，
--      改用 md5(prompt|consumer_key) 作主键，insert 冲突即忽略（已有缓存）。
--   5) 文档 body_filter 里 [DONE] 判断有前导空格 bug（永远匹配不上），
--      且改写 ngx.arg[1] 会与 ai_proxy 的响应转换打架；本插件 body_filter
--      全程只读旁路，绝不改写响应字节。
--   6) embedding / Milvus 任一不可用一律 fail-open 直通 LLM（缓存是加速
--      优化，不能成为主链路的单点故障）。
--   7) 维度由配置 VEC_DIM 控制（TEI+BGE：bge-small-zh=512 / bge-base-zh=768
--      / bge-large-zh=1024），自检接口会校验 TEI 返回维度与建表一致。
--   8) 缓存命中的 SSE 逐块输出按 UTF-8 字符边界切分（文档逐字符输出
--      sleep 8ms，1000 字要 8 秒，且切开多字节字符会让 JSON 帧非法）。
--
-- 挂载方式（见 nginx.conf，每阶段只有一个 *_by_lua_block，必须链式调用）：
--   access_by_lua_block {
--       require("ai_proxy.ai_proxy").access()
--       require("token_limit.token_limit").access()
--       require("vec_cache.vec_cache").access()      -- 在 quota 之前：命中即返回
--       require("quota.quota").access()
--   }
--   body_filter_by_lua_block {
--       require("ai_proxy.ai_proxy").body_filter()
--       require("token_limit.token_limit").body_filter()
--       require("quota.quota").body_filter()
--       require("vec_cache.vec_cache").body_filter() -- 链尾只读旁路
--   }
--   log_by_lua_block {
--       require("token_limit.token_limit").log()
--       require("quota.quota").log()
--       require("vec_cache.vec_cache").log()
--   }
-- 管理接口：vec_cache_admin.lua，挂 content_by_lua location（/ai/vec_cache_admin）
-- 自检接口：_M.test()，挂 content_by_lua location（/ai/vec_cache_selftest）

local cjson = require "cjson"
local http  = require "resty.http"

local _M = {}

-- ---------------------------------------------------------------------------
-- 可调配置
-- ---------------------------------------------------------------------------
local EMBED_URL         = "http://127.0.0.1:8081/v1/embeddings"  -- TEI OpenAI 兼容接口
local MILVUS_BASE       = "http://127.0.0.1:9091"                -- Milvus REST v2 端口
local COLLECTION        = "ai_chat_cache"
local VEC_DIM           = 768     -- 与 TEI 部署的 BGE 模型一致（bge-base-zh=768）
local SIMILAR_THRESHOLD = 0.85    -- COSINE 相似度 >= 0.85 判定命中（越高越相似）
local SEARCH_LIMIT      = 3       -- 取 top-N 再逐条比较，防单条噪声误命中
local EMBED_TIMEOUT     = 5000    -- embedding 超时（ms）：TEI CPU 推理慢，放宽
local MILVUS_TIMEOUT    = 2000    -- Milvus 超时（ms）
local MAX_PROMPT_LEN    = 4096    -- 超过该长度的 prompt 不查缓存（embedding 成本/噪声）
local MAX_CACHE_ANSWER  = 16 * 1024  -- 超过该长度的答案不写缓存（控制存储）
local SSE_CHUNK_BYTES   = 128     -- 缓存命中 SSE 每片字节数（UTF-8 安全切分）
local SSE_CHUNK_DELAY   = 0.02    -- 每片间隔（秒），模拟流式节奏
local ENABLED           = true    -- 总开关

-- ---------------------------------------------------------------------------
-- 工具函数
-- ---------------------------------------------------------------------------

local function err_json(status, message, err_type, code)
    ngx.status = status
    ngx.header["Content-Type"] = "application/json"
    local e = { message = message, type = err_type or "invalid_request_error" }
    if code then e.code = code end
    ngx.say(cjson.encode({ error = e }))
    return ngx.exit(status)
end

-- 读取完整请求体（兼容超过 client_buffer 落盘文件的大 body）
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

-- 通用 HTTP POST JSON（短超时，网关路径上必须能快速失败）
-- 注意：Milvus REST v2 的业务错误也是 HTTP 200，错误码在 body 的 code 字段里
local function http_post_json(url, body_table, timeout)
    local httpc = http.new()
    httpc:set_timeout(timeout)
    local res, err = httpc:request_uri(url, {
        method  = "POST",
        headers = { ["Content-Type"] = "application/json" },
        body    = cjson.encode(body_table),
    })
    if not res then
        return nil, "http fail: " .. tostring(err)
    end
    local body = res.body or ""
    local ok, obj = pcall(cjson.decode, body)
    if not ok then
        return nil, "bad json body, status=" .. tostring(res.status)
    end
    if res.status ~= 200 then
        return nil, "status=" .. res.status .. " body=" .. body
    end
    -- Milvus 风格业务码：code 字段存在且非 200 即为业务错误
    if type(obj.code) == "number" and obj.code ~= 200 then
        return nil, "code=" .. obj.code .. " msg=" .. tostring(obj.message)
    end
    return obj
end

-- 提取最后一轮 user prompt。
-- 兼容纯文本 content 与多模态 content（数组 [{type="text",text=...}, ...]）
local function extract_last_user_prompt(req)
    local messages = req.messages
    if type(messages) ~= "table" then return "" end
    for i = #messages, 1, -1 do
        local m = messages[i]
        if type(m) == "table" and m.role == "user" then
            local c = m.content
            if type(c) == "string" then return c end
            if type(c) == "table" then
                local texts = {}
                for _, part in ipairs(c) do
                    if type(part) == "table" and type(part.text) == "string" then
                        texts[#texts + 1] = part.text
                    end
                end
                if #texts > 0 then return table.concat(texts, "\n") end
            end
            return ""
        end
    end
    return ""
end

-- 调用 TEI embedding 服务，返回向量或 nil,err
local function do_embedding(text)
    local resp, err = http_post_json(EMBED_URL, { input = text }, EMBED_TIMEOUT)
    if not resp then return nil, err end
    -- OpenAI 兼容格式：{ data: [ { embedding: [...] } ] }
    local data = resp.data
    if type(data) ~= "table" or type(data[1]) ~= "table"
        or type(data[1].embedding) ~= "table" then
        return nil, "unexpected embedding response"
    end
    local vec = data[1].embedding
    if #vec ~= VEC_DIM then
        return nil, "embedding dim mismatch: got " .. #vec .. " expect " .. VEC_DIM
    end
    return vec
end

-- Milvus 向量检索：按 consumer_key 过滤，返回 top-N
-- 返回结果表或 nil,err；结果表为空表表示"无相似记录"
local function milvus_search(vec, consumer_key)
    -- 注意 filter 表达式：值统一用双引号包裹（Milvus 表达式语法），内部引号转义
    local filter = 'consumer_key == "' .. consumer_key:gsub('"', '\\"') .. '"'
    local resp, err = http_post_json(MILVUS_BASE .. "/v2/vectordb/entities/search", {
        collectionName = COLLECTION,
        data           = { vec },
        annsField      = "vec",          -- 必须指定向量字段（Milvus 2.4 多向量字段要求）
        limit          = SEARCH_LIMIT,
        outputFields   = { "prompt", "answer" },
        filter         = filter,
        searchParams   = { metricType = "COSINE", params = {} },
    }, MILVUS_TIMEOUT)
    if not resp then return nil, err end
    if type(resp.data) ~= "table" then return nil, "unexpected search response" end
    return resp.data
end

-- Milvus 写入缓存（主键 = md5(prompt|consumer_key)，冲突即已有缓存）
-- 返回 true(新写入) / "duplicate"(已有缓存，并发场景) / nil,err
local function milvus_insert(doc_id, vec, prompt, answer, consumer_key)
    local resp, err = http_post_json(MILVUS_BASE .. "/v2/vectordb/entities/insert", {
        collectionName = COLLECTION,
        data = {
            {
                id           = doc_id,
                vec          = vec,
                prompt       = prompt,
                answer       = answer,
                consumer_key = consumer_key,
            },
        },
    }, MILVUS_TIMEOUT)
    if resp then return true end
    -- 主键冲突（同 prompt 已有缓存，如并发未命中场景）是预期内结果，不算失败
    if err and err:find("duplicate", 1, true) then
        return "duplicate"
    end
    return nil, err
end

-- UTF-8 安全分块迭代器：返回逐片切分 answer 的迭代函数，
-- 切点绝不落在多字节字符的续字节（0x80-0xBF）上
local function utf8_chunks(s, max_bytes)
    local n = #s
    local i = 1
    return function()
        if i > n then return nil end
        local j = math.min(i + max_bytes - 1, n)
        while j < n and s:byte(j + 1) >= 0x80 and s:byte(j + 1) < 0xC0 do
            j = j - 1
        end
        local part = s:sub(i, j)
        i = j + 1
        return part
    end
end

-- ---------------------------------------------------------------------------
-- 缓存命中时的响应构造（OpenAI 标准格式）
-- ---------------------------------------------------------------------------

local function cache_usage(prompt, answer)
    -- 估算 token（与 quota 一致的粗估）：CJK 2 字 1 token，英文 4 字符 1 token
    local function est(t)
        if type(t) ~= "string" or t == "" then return 0 end
        local _, cn = string.gsub(t, "[\128-\255][\128-\255]", "")
        return math.floor(cn * 0.5 + (#t - 2 * cn) / 4)
    end
    local pt, ct = est(prompt), est(answer)
    return { prompt_tokens = pt, completion_tokens = ct, total_tokens = pt + ct }
end

-- 构造单个 SSE chunk JSON
local function chunk_json(id, created, content, finish_reason)
    local choices
    if content ~= nil then
        choices = { { index = 0, delta = { content = content }, finish_reason = nil } }
    else
        choices = { { index = 0, delta = {}, finish_reason = finish_reason } }
    end
    return {
        id = id, object = "chat.completion.chunk", created = created,
        model = "milvus-cache", choices = choices,
    }
end

-- 流式命中：SSE 分块输出缓存答案
local function emit_cache_sse(id, answer, usage)
    ngx.header["Content-Type"] = "text/event-stream; charset=utf-8"
    ngx.header["Cache-Control"] = "no-cache"
    ngx.header["Connection"] = "keep-alive"
    ngx.header["X-Vec-Cache"] = "hit"

    local created = ngx.time()
    -- 首帧带 role（OpenAI 流式约定）
    ngx.print("data: " .. cjson.encode({
        id = id, object = "chat.completion.chunk", created = created,
        model = "milvus-cache",
        choices = { { index = 0, delta = { role = "assistant", content = "" },
                      finish_reason = nil } },
    }), "\n\n")

    for part in utf8_chunks(answer, SSE_CHUNK_BYTES) do
        local ok, err = ngx.print("data: " .. cjson.encode(
            chunk_json(id, created, part, nil)), "\n\n")
        if not ok then return end -- 客户端断连，停止输出
        ngx.flush(true)
        ngx.sleep(SSE_CHUNK_DELAY)
    end

    -- 结束帧 + usage 帧（usage 供 token_limit 修正记账，防缓存命中刷量）
    ngx.print("data: " .. cjson.encode(chunk_json(id, created, nil, "stop")), "\n\n")
    ngx.print("data: " .. cjson.encode({
        id = id, object = "chat.completion.chunk", created = created,
        model = "milvus-cache", choices = {},
        usage = usage,
    }), "\n\n")
    ngx.print("data: [DONE]\n\n")
    ngx.flush(true)
end

-- 非流式命中：一次性 JSON 输出
local function emit_cache_json(id, answer, usage)
    ngx.header["Content-Type"] = "application/json"
    ngx.header["X-Vec-Cache"] = "hit"
    ngx.print(cjson.encode({
        id = id, object = "chat.completion", created = ngx.time(),
        model = "milvus-cache",
        choices = { { index = 0, finish_reason = "stop",
                      message = { role = "assistant", content = answer } } },
        usage = usage,
    }))
end

-- ---------------------------------------------------------------------------
-- 阶段一：access —— embedding + Milvus 检索；命中直接返回，未命中放行
-- ---------------------------------------------------------------------------
function _M.access()
    local ctx = ngx.ctx
    if not ENABLED then return end

    -- 1. 复用 ai_proxy 已解码的 OpenAI 请求体（独立使用时自己读）
    local req = ctx.openai_req
    if type(req) ~= "table" then
        local raw = read_request_body()
        if not raw or raw == "" then return end
        local ok, obj = pcall(cjson.decode, raw)
        if not ok or type(obj) ~= "table" then return end
        req = obj
    end

    -- 2. 提取最后一轮 user prompt（多模态 content 兼容）
    local prompt = extract_last_user_prompt(req)
    if prompt == "" then return end
    if #prompt > MAX_PROMPT_LEN then
        ngx.log(ngx.INFO, "vec_cache: prompt 超长 ", #prompt, " 字节，跳过缓存")
        return
    end

    -- 3. consumer 身份：复用 ai_proxy 鉴权出的 gateway_token
    local consumer = ctx.gateway_token or ngx.var.http_x_consumer_id or "default"
    local stream = req.stream == true
    local id = "chatcmpl-vc-" .. string.sub(ngx.md5(prompt .. "|" .. consumer), 1, 12)

    -- 4. 调用 embedding 服务拿向量（失败 fail-open 直通 LLM，不拦主链路）
    local vec, emb_err = do_embedding(prompt)
    if not vec then
        ngx.log(ngx.WARN, "vec_cache: embedding 失败，fail-open 直通: ", emb_err)
        return
    end

    -- 5. Milvus 相似检索（失败同样 fail-open）
    local hits, s_err = milvus_search(vec, consumer)
    if not hits then
        ngx.log(ngx.WARN, "vec_cache: milvus search 失败，fail-open 直通: ", s_err)
        return
    end

    -- 6. 逐条比对 top-N，命中则直接构造响应返回
    for _, item in ipairs(hits) do
        -- 坑：cjson 把 JSON null 解码为 userdata（truthy），
        -- entity/distance 为 null 时直接访问会抛错，必须 type 检查
        local score = type(item) == "table" and tonumber(item.distance) or nil
        local entity = type(item) == "table" and item.entity or nil
        if not score or type(entity) ~= "table" then
            ngx.log(ngx.WARN, "vec_cache: 异常检索结果条目，跳过")
        elseif score >= SIMILAR_THRESHOLD then
            local answer = entity.answer
            if type(answer) == "string" and answer ~= "" then
                ctx.vc_cache_hit = true
                ngx.log(ngx.INFO, "vec_cache: 命中缓存 score=", score,
                    " id=", tostring(item.id), " consumer=[", consumer, "]")
                -- 置空 provider：让 ai_proxy 的 body_filter 跳过响应转换
                -- （缓存响应已是 OpenAI 标准格式，二次转换会损坏响应）
                ctx.provider = nil

                local usage = cache_usage(prompt, answer)
                if stream then
                    emit_cache_sse(id, answer, usage)
                else
                    emit_cache_json(id, answer, usage)
                end
                -- 结束请求：不再走 quota / proxy_pass
                return ngx.exit(ngx.OK)
            end
        end
    end

    -- 7. 未命中：记录上下文，放行继续（quota -> proxy_pass -> LLM），
    --    body_filter 累积 answer、log 阶段异步写缓存
    ctx.vc_cache_hit = false
    ctx.vc_query_vec = vec
    ctx.vc_prompt    = prompt
    ctx.vc_consumer  = consumer
    ctx.vc_stream    = stream
    ngx.log(ngx.INFO, "vec_cache: 未命中，直通 LLM（prompt 前 40 字: ",
        prompt:sub(1, 40):gsub("[\r\n]", " "), "）")
end

-- ---------------------------------------------------------------------------
-- 阶段二：body_filter —— 只读旁路累积完整 answer（绝不改写响应字节）
-- ---------------------------------------------------------------------------

-- 解析一个完整 SSE 事件：累积 delta.content，检测 [DONE] / finish_reason
local function scan_sse_event(ev, ctx)
    local parts = {}
    for line in ev:gmatch("[^\r\n]+") do
        local d = line:match("^data:%s?(.*)$")
        if not d then return end -- 含 event:/id:/注释行，跳过
        parts[#parts + 1] = d
    end
    local data = table.concat(parts, "\n")
    if data == "" then return end
    if data == "[DONE]" then
        ctx.vc_completed = true
        return
    end
    local ok, obj = pcall(cjson.decode, data)
    if not ok or type(obj) ~= "table" then return end
    local choices = obj.choices
    if type(choices) ~= "table" or type(choices[1]) ~= "table" then return end
    local c1 = choices[1]
    -- usage chunk（choices 为空数组）无 content，忽略
    if c1.finish_reason == "stop" then
        ctx.vc_completed = true
    end
    local delta = c1.delta
    if type(delta) == "table" and type(delta.content) == "string" then
        ctx.vc_full_answer = (ctx.vc_full_answer or "") .. delta.content
    end
end

function _M.body_filter()
    local ctx = ngx.ctx
    -- 缓存命中（自己构造的响应无需累积）/ 无向量（embedding 失败直通，
    -- 无法写缓存）时直接跳过
    if ctx.vc_cache_hit or not ctx.vc_query_vec then return end

    local chunk, eof = ngx.arg[1], ngx.arg[2]
    -- 长度保护：超限直接放弃本次缓存（防恶意大响应撑爆 ctx）
    if #(ctx.vc_full_answer or "") > MAX_CACHE_ANSWER then return end

    -- 流式判定：优先看请求参数，兜底看响应 Content-Type
    -- （ai_proxy 会把所有流式厂商响应都转成 text/event-stream）
    local ct = ngx.header["Content-Type"] or ""
    if ctx.vc_stream or ct:find("text/event-stream", 1, true) then
        local buf = (ctx.vc_sse_buf or "") .. (chunk or "")
        buf = buf:gsub("\r", "")   -- CRLF 行尾归一，兼容不同上游
        ctx.vc_sse_buf = ""
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
        ctx.vc_sse_buf = buf
    else
        -- 非流式：累积完整 JSON，eof 时一次性解析
        ctx.vc_json_buf = (ctx.vc_json_buf or "") .. (chunk or "")
        if eof then
            local ok, obj = pcall(cjson.decode, ctx.vc_json_buf)
            if ok and type(obj) == "table" and type(obj.choices) == "table"
                and type(obj.choices[1]) == "table" then
                local c1 = obj.choices[1]
                if c1.finish_reason == "stop" then
                    ctx.vc_completed = true
                end
                local msg = c1.message
                if type(msg) == "table" and type(msg.content) == "string" then
                    ctx.vc_full_answer = msg.content
                end
            end
            ctx.vc_json_buf = nil
        end
    end
end

-- ---------------------------------------------------------------------------
-- 阶段三：log —— 调度 ngx.timer 异步写入 Milvus 缓存
-- ---------------------------------------------------------------------------
-- log_by_lua 上下文禁用 cosocket，标准解法：log 阶段读 ctx 快照，
-- ngx.timer.at(0) 调度异步任务，在 timer 回调里完成 HTTP 写库（best-effort）

local function write_cache(s)
    -- 主键 = md5(prompt|consumer_key)：同一 prompt 只存一条，insert 冲突忽略
    local doc_id = "vc-" .. ngx.md5(s.prompt .. "|" .. s.consumer)
    local res, err = milvus_insert(doc_id, s.vec, s.prompt, s.answer, s.consumer)
    if res == true then
        ngx.log(ngx.INFO, "vec_cache: 缓存已写入 id=", doc_id,
            " prompt_len=", #s.prompt, " answer_len=", #s.answer)
    elseif res == "duplicate" then
        ngx.log(ngx.INFO, "vec_cache: 缓存已存在（并发写入冲突忽略） id=", doc_id)
    else
        ngx.log(ngx.WARN, "vec_cache: 缓存写入失败（best-effort 忽略）: ", tostring(err))
    end
end

function _M.log()
    local ctx = ngx.ctx
    if ctx.vc_cache_hit or not ctx.vc_query_vec then return end

    local answer = ctx.vc_full_answer or ""
    -- 只有上游正常完成（finish_reason=stop / [DONE]）且答案长度合理才写缓存
    if not ctx.vc_completed or answer == "" or #answer > MAX_CACHE_ANSWER then
        if not ctx.vc_completed then
            ngx.log(ngx.INFO, "vec_cache: 上游未正常完成，不写缓存")
        end
        return
    end

    -- 快照 ctx：timer 回调运行在独立协程，拿不到原请求的 ngx.ctx
    local snapshot = {
        vec      = ctx.vc_query_vec,
        prompt   = ctx.vc_prompt,
        answer   = answer,
        consumer = ctx.vc_consumer,
    }

    local ok, err = ngx.timer.at(0, function(premature)
        if premature then return end
        write_cache(snapshot)
    end)
    if not ok then
        ngx.log(ngx.ERR, "vec_cache: ngx.timer.at fail: ", err)
    end
end

-- ---------------------------------------------------------------------------
-- 自检接口：模仿 test 插件，验证 TEI / Milvus / 配置是否就绪
-- ---------------------------------------------------------------------------
function _M.test()
    ngx.log(ngx.INFO, "=== vec_cache 插件自检 ===")
    ngx.header["Content-Type"] = "application/json"

    local status = { code = 0, msg = "vec_cache plugin ok", config = {
        embed_url = EMBED_URL,
        milvus = MILVUS_BASE,
        collection = COLLECTION,
        vec_dim = VEC_DIM,
        threshold = SIMILAR_THRESHOLD,
        enabled = ENABLED,
    }, checks = {} }
    local checks = status.checks

    -- 1. resty.http 可加载
    local ok_http = pcall(require, "resty.http")
    checks[#checks + 1] = { item = "resty.http 模块", ok = ok_http }

    -- 2. TEI embedding 服务连通 + 维度校验
    local vec, emb_err = do_embedding("向量缓存自检测试")
    if vec then
        checks[#checks + 1] = { item = "TEI " .. EMBED_URL, ok = true,
            detail = "维度 " .. #vec .. " 与配置一致" }
    else
        status.code = 1
        checks[#checks + 1] = { item = "TEI embedding 服务", ok = false,
            hint = "启动 TEI（默认 127.0.0.1:8081），或检查 VEC_DIM 配置。err: " .. tostring(emb_err) }
    end

    -- 3. Milvus 连通 + collection 存在
    local desc, m_err = http_post_json(MILVUS_BASE .. "/v2/vectordb/collections/describe",
        { collectionName = COLLECTION }, MILVUS_TIMEOUT)
    if desc then
        checks[#checks + 1] = { item = "Milvus " .. MILVUS_BASE, ok = true,
            detail = "collection [" .. COLLECTION .. "] 已存在" }
    else
        status.code = 1
        checks[#checks + 1] = { item = "Milvus collection", ok = false,
            hint = "Milvus 未连接或 collection 不存在，用 POST /ai/vec_cache_admin "
                .. "action=create_collection 建表。err: " .. tostring(m_err) }
    end

    -- 4. 挂载顺序提示
    checks[#checks + 1] = { item = "挂载顺序", ok = true,
        hint = "access 挂在 token_limit 之后、quota 之前（命中缓存不消耗月度额度）；"
            .. "body_filter 挂在链尾只读旁路" }

    ngx.say(cjson.encode(status))
end

return _M
