-- vec_cache_admin.lua
-- AI 向量缓存插件管理接口：建表 / 删表 / 清空 / 统计 / 调试检索
--
-- Milvus 前置准备（对应文档"先执行一次"的建表步骤），全部通过 REST v2 完成，
-- 无需 python SDK。挂在 content_by_lua location（/ai/vec_cache_admin）：
--   POST /ai/vec_cache_admin   body: {"action":"create_collection"}
--
-- 可用 action：
--   create_collection : 创建 ai_chat_cache 集合（幂等，已存在则跳过）+ 建 COSINE 索引
--   drop_collection    : 删除集合（缓存全部清空）
--   clear              : drop + create，测试时快速清空缓存
--   stats              : 查看缓存记录数与最近 N 条（prompt 摘要）
--   search             : 调试用，给定 prompt 走 embedding+检索，返回 top-N 相似度
--
-- 安全提醒：生产环境必须给该接口加 IP 白名单 + 管理鉴权，不能裸奔！

local cjson = require "cjson"
local http  = require "resty.http"

local _M = {}

local MILVUS_BASE = "http://127.0.0.1:9091"
local EMBED_URL   = "http://127.0.0.1:8081/v1/embeddings"
local COLLECTION  = "ai_chat_cache"
local VEC_DIM     = 768
local TIMEOUT     = 2000

local function reply(code, msg, data)
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson.encode({ code = code, msg = msg, data = data }))
end

local function read_body()
    ngx.req.read_body()
    local body = ngx.req.get_body_data()
    if not body then return nil end
    local ok, obj = pcall(cjson.decode, body)
    if not ok or type(obj) ~= "table" then return nil end
    return obj
end

-- 通用 HTTP POST JSON，返回 { code, data, err }
-- 注意：Milvus REST v2 业务错误也是 HTTP 200，错误码在 body 的 code 字段里
local function http_post_json(url, body_table)
    local httpc = http.new()
    httpc:set_timeout(TIMEOUT)
    local res, err = httpc:request_uri(url, {
        method  = "POST",
        headers = { ["Content-Type"] = "application/json" },
        body    = cjson.encode(body_table),
    })
    if not res then return nil, tostring(err) end
    local ok, obj = pcall(cjson.decode, res.body or "")
    if not ok then return nil, "bad json body, status=" .. res.status end
    if res.status ~= 200 then
        return nil, "status=" .. res.status .. " body=" .. (res.body or "")
    end
    -- Milvus 风格业务码：code 字段存在且非 200 即为业务错误
    if type(obj.code) == "number" and obj.code ~= 200 then
        return nil, "code=" .. obj.code .. " msg=" .. tostring(obj.message)
    end
    return obj
end

-- 建集合 + 建索引（幂等：已存在则跳过）
-- 返回 ok,err（不直接 reply，供 clear 组合调用）
local function create_collection()
    local schema = {
        collectionName = COLLECTION,
        dbName = "default",
        schema = {
            autoID = false,
            fields = {
                { fieldName = "id", dataType = "VarChar", isPrimary = true,
                  maxLength = 128 },
                { fieldName = "vec", dataType = "FloatVector", dim = VEC_DIM },
                { fieldName = "prompt", dataType = "VarChar", maxLength = 65535 },
                { fieldName = "answer", dataType = "VarChar", maxLength = 1048576 },
                { fieldName = "consumer_key", dataType = "VarChar", maxLength = 128 },
            },
        },
    }
    local resp, err = http_post_json(MILVUS_BASE .. "/v2/vectordb/collections/create", schema)
    if not resp then
        -- "already exist" 视为幂等成功
        if err and err:find("exist", 1, true) then
            ngx.log(ngx.INFO, "vec_cache admin: collection 已存在，跳过建表")
        else
            return nil, "create collection fail: " .. tostring(err)
        end
    end

    local idx_resp, idx_err = http_post_json(MILVUS_BASE .. "/v2/vectordb/indexes/create", {
        collectionName = COLLECTION,
        indexParams = {
            { fieldName = "vec", indexName = "vec_cos_idx",
              indexType = "AUTOINDEX", metricType = "COSINE", params = {} },
        },
    })
    if not idx_resp then
        return nil, "create index fail: " .. tostring(idx_err)
    end
    return true
end

-- 删集合，返回 ok,err（不直接 reply，供 clear 组合调用）
local function drop_collection()
    local _, err = http_post_json(MILVUS_BASE .. "/v2/vectordb/collections/drop",
        { collectionName = COLLECTION })
    if err then
        return nil, "drop fail: " .. tostring(err)
    end
    return true
end

local function stats(limit)
    limit = tonumber(limit) or 10
    local resp, err = http_post_json(MILVUS_BASE .. "/v2/vectordb/entities/query", {
        collectionName = COLLECTION,
        filter = "",
        outputFields = { "id", "prompt", "consumer_key" },
        limit = limit,
        offset = 0,
    })
    if not resp then
        return reply(1, "query fail: " .. tostring(err))
    end
    local rows = resp.data or {}
    local items = {}
    for _, r in ipairs(rows) do
        items[#items + 1] = {
            id = r.id,
            consumer_key = r.consumer_key,
            prompt = type(r.prompt) == "string" and r.prompt:sub(1, 60) or "",
        }
    end
    return reply(0, "ok", { total_shown = #items, rows = items })
end

-- 调试检索：embedding + Milvus search，观察 top-N 相似度分布
local function debug_search(prompt, consumer_key)
    if not prompt or prompt == "" then
        return reply(1, "prompt required")
    end
    local emb_resp, emb_err = http_post_json(EMBED_URL, { input = prompt })
    if not emb_resp or type(emb_resp.data) ~= "table"
        or type(emb_resp.data[1]) ~= "table"
        or type(emb_resp.data[1].embedding) ~= "table" then
        return reply(1, "embedding fail: " .. tostring(emb_err))
    end
    local vec = emb_resp.data[1].embedding

    local consumer = consumer_key or "default"
    local filter = 'consumer_key == "' .. consumer:gsub('"', '\\"') .. '"'
    local resp, err = http_post_json(MILVUS_BASE .. "/v2/vectordb/entities/search", {
        collectionName = COLLECTION,
        data = { vec },
        annsField = "vec",
        limit = 5,
        outputFields = { "prompt" },
        filter = filter,
        searchParams = { metricType = "COSINE", params = {} },
    })
    if not resp then
        return reply(1, "search fail: " .. tostring(err))
    end
    local items = {}
    for _, r in ipairs(resp.data or {}) do
        items[#items + 1] = {
            id = r.id,
            distance = r.distance,
            prompt = type(r.entity) == "table" and type(r.entity.prompt) == "string"
                and r.entity.prompt:sub(1, 60) or "",
        }
    end
    return reply(0, "ok", { threshold_hint = "cosine >= 0.85 判定命中", top = items })
end

function _M.run()
    if ngx.req.get_method() ~= "POST" then
        return reply(1, "only POST allowed")
    end

    local req = read_body()
    if not req then
        return reply(1, "json decode fail")
    end

    local action = req.action
    if action == "create_collection" then
        local ok_c, c_err = create_collection()
        if not ok_c then return reply(1, c_err) end
        return reply(0, "collection [" .. COLLECTION .. "] ready (dim=" .. VEC_DIM .. ", COSINE)")
    elseif action == "drop_collection" then
        local ok_d, d_err = drop_collection()
        if not ok_d then return reply(1, d_err) end
        return reply(0, "collection [" .. COLLECTION .. "] dropped")
    elseif action == "clear" then
        local ok_d, d_err = drop_collection()
        if not ok_d then return reply(1, "clear: " .. tostring(d_err)) end
        local ok_c, c_err = create_collection()
        if not ok_c then return reply(1, "clear: " .. tostring(c_err)) end
        return reply(0, "cache cleared, collection recreated")
    elseif action == "stats" then
        return stats(req.limit)
    elseif action == "search" then
        return debug_search(req.prompt, req.consumer_key)
    else
        return reply(1, "unknown action: " .. tostring(action))
    end
end

return _M
