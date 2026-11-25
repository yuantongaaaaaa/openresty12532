-- quota_admin_api.lua
-- AI 配额管理接口（对应文档"额度管理接口 quota_admin_api.lua"，修正其缺陷）：
--
--   GET  /ai/quota_admin?consumer_key=xxx
--        查询：total_quota / used / remain
--   POST /ai/quota_admin
--        body: {"consumer_key":"xxx","action":"set_quota|add|reset","value":123}
--        set_quota : 设置月度总额（value >= 0）
--        add       : 增减额度（正数加 / 负数减，总额 clamp 到 0）
--        reset     : 重置当月已消耗（月初定时调用或人工重置，连同预占台账一起清）
--
-- 文档缺陷修正：
--   1) r:connect 错误未检查，失败后继续用连接会抛 500 —— 必须检查并明确报错；
--   2) POST body 未判空，cjson.decode(nil) 会抛 500 —— 必须校验；
--   3) POST 的 consumer 只从 URL 参数取，body 传 consumer_key 直接 400 ——
--      改为 body 优先、URL 参数回退；
--   4) 返回值没设 Content-Type，默认 octet-stream —— 统一 application/json；
--   5) GET 查询 cfg/used 非原子，高并发下 remain 可能偏差一条请求 ——
--      管理查询不要求强一致，可接受（注释说明）；
--   6) 鉴权：文档只留了注释，这里提供可选 ADMIN_TOKEN 简单兜底，
--      生产务必再加内网 IP 白名单 + HTTPS。
--
-- 挂载（见 nginx.conf）：
--   location /ai/quota_admin {
--       content_by_lua_block { require("quota.quota_admin_api").run() }
--   }

local cjson = require "cjson"
local redis = require "resty.redis"

local _M = {}

-- ---------------------------------------------------------------------------
-- 可调配置
-- ---------------------------------------------------------------------------
local REDIS_HOST    = "127.0.0.1"
local REDIS_PORT    = 6379
local KEY_PREFIX    = "quota:"
local REDIS_TIMEOUT = 200
local POOL_SIZE     = 100
local POOL_IDLE     = 10000

-- 管理接口鉴权：nil = 不校验（仅限本机联调）；
-- 设置为非空字符串后，请求必须带 header X-Admin-Token: <该值>。
-- 生产务必再加内网 IP 白名单 + HTTPS，不能裸奔！
local ADMIN_TOKEN = nil

local function reply(code, msg, data)
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson.encode({ code = code, msg = msg, data = data }))
end

local function redis_conn()
    local r = redis:new()
    r:set_timeout(REDIS_TIMEOUT)
    local ok, err = r:connect(REDIS_HOST, REDIS_PORT)
    if not ok then
        ngx.log(ngx.ERR, "quota admin: redis connect fail: ", err)
        return nil
    end
    return r
end

function _M.run()
    -- 可选管理鉴权
    if ADMIN_TOKEN and ngx.var.http_x_admin_token ~= ADMIN_TOKEN then
        ngx.status = ngx.HTTP_UNAUTHORIZED
        return reply(1, "invalid admin token")
    end

    local r = redis_conn()
    if not r then
        ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
        return reply(1, "redis connect fail")
    end

    local method = ngx.req.get_method()

    -- POST 时 consumer 优先取 body 里的 consumer_key，回退 URL 参数；
    -- GET 只取 URL 参数（文档原版 POST 也只取 URL 参数，导致 body 传参失败）
    local consumer = ngx.var.arg_consumer_key
    local body
    if method == "POST" then
        ngx.req.read_body()
        local raw = ngx.req.get_body_data()
        if not raw or raw == "" then
            r:set_keepalive(POOL_SIZE, POOL_IDLE)
            ngx.status = ngx.HTTP_BAD_REQUEST
            return reply(1, "empty body")
        end
        local ok, obj = pcall(cjson.decode, raw)
        if not ok or type(obj) ~= "table" then
            r:set_keepalive(POOL_SIZE, POOL_IDLE)
            ngx.status = ngx.HTTP_BAD_REQUEST
            return reply(1, "json decode fail")
        end
        body = obj
        if type(body.consumer_key) == "string" and body.consumer_key ~= "" then
            consumer = body.consumer_key
        end
    end

    if not consumer or consumer == "" then
        r:set_keepalive(POOL_SIZE, POOL_IDLE)
        ngx.status = ngx.HTTP_BAD_REQUEST
        return reply(1, "missing consumer_key")
    end

    local cfg_key  = KEY_PREFIX .. "cfg:" .. consumer
    local used_key = KEY_PREFIX .. "used:" .. consumer

    if method == "GET" then
        -- 查询额度。cfg/used 两个 GET 非原子：并发请求下 remain 可能偏差
        -- 一条正在途的预占，管理查询场景可接受
        local total, terr = r:get(cfg_key)
        if total == nil and terr then
            r:set_keepalive(POOL_SIZE, POOL_IDLE)
            return reply(1, "get cfg fail: " .. terr)
        end
        local used, uerr = r:get(used_key)
        if used == nil and uerr then
            r:set_keepalive(POOL_SIZE, POOL_IDLE)
            return reply(1, "get used fail: " .. uerr)
        end
        r:set_keepalive(POOL_SIZE, POOL_IDLE)

        local total_n = tonumber(total) or 0
        local used_n = tonumber(used) or 0
        return reply(0, "ok", {
            consumer_key = consumer,
            total_quota = total_n,
            used = used_n,
            remain = math.max(0, total_n - used_n),
        })
    end

    if method ~= "POST" then
        r:set_keepalive(POOL_SIZE, POOL_IDLE)
        -- 注意：本 OpenResty 版本 ngx.HTTP_METHOD_NOT_ALLOWED 常量为 nil，
        -- 赋值会抛 "cannot convert 'nil' to 'int'"，直接用数字 405
        ngx.status = 405
        return reply(1, "only GET/POST allowed")
    end

    local action = body.action
    local val = tonumber(body.value or 0)

    if action == "set_quota" then
        -- 设置月度总额
        if not val or val < 0 then
            r:set_keepalive(POOL_SIZE, POOL_IDLE)
            ngx.status = ngx.HTTP_BAD_REQUEST
            return reply(1, "value must be a number >= 0")
        end
        local ok_set, serr = r:set(cfg_key, val)
        if not ok_set then
            r:set_keepalive(POOL_SIZE, POOL_IDLE)
            return reply(1, "set fail: " .. tostring(serr))
        end
        r:set_keepalive(POOL_SIZE, POOL_IDLE)
        ngx.log(ngx.INFO, "quota admin: [", consumer, "] 月度总额设为 ", val)
        return reply(0, "ok", { action = "set_quota", total = val })

    elseif action == "add" then
        -- 增减额度（正数加 / 负数减，总额 clamp 到 0 防负额度）
        local total, terr = r:get(cfg_key)
        if total == nil and terr then
            r:set_keepalive(POOL_SIZE, POOL_IDLE)
            return reply(1, "get cfg fail: " .. terr)
        end
        local total_n = math.max(0, (tonumber(total) or 0) + val)
        local ok_set, serr = r:set(cfg_key, total_n)
        if not ok_set then
            r:set_keepalive(POOL_SIZE, POOL_IDLE)
            return reply(1, "set fail: " .. tostring(serr))
        end
        r:set_keepalive(POOL_SIZE, POOL_IDLE)
        ngx.log(ngx.INFO, "quota admin: [", consumer, "] 额度调整 ", val,
            "，当前总额 ", total_n)
        return reply(0, "ok", { action = "add", delta = val, total = total_n })

    elseif action == "reset" then
        -- 重置当月消耗（连同预占台账一起清，防止旧台账被惰性回收时回滚 used）
        r:del(used_key)
        r:del(KEY_PREFIX .. "pending:" .. consumer)
        r:set_keepalive(POOL_SIZE, POOL_IDLE)
        ngx.log(ngx.INFO, "quota admin: [", consumer, "] 当月消耗已重置")
        return reply(0, "ok", { action = "reset" })

    else
        r:set_keepalive(POOL_SIZE, POOL_IDLE)
        ngx.status = ngx.HTTP_BAD_REQUEST
        return reply(1, "unknown action: " .. tostring(action))
    end
end

return _M
