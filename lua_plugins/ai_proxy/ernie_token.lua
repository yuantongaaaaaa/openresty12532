-- ernie_token.lua
-- 百度文心一言 AK/SK -> access_token 管理
--
-- 百度鉴权与其他三家完全不同：Bearer 后面不是固定 sk，而是通过
--   GET /oauth/2.0/token?grant_type=client_credentials&client_id={AK}&client_secret={SK}
-- 动态换取的 access_token（有效期约 30 天，文档误写为 24h，以接口返回
-- expires_in 为准）。
--
-- 策略（修正文档"每次请求都拿 token"的问题）：
--   1. shared_dict 缓存 token，带 TTL（expires_in 提前 300s 过期）
--   2. 惰性刷新：请求到来时缓存命中直接用；未命中/过期才同步换取一次
--   3. 换取成功后写回缓存，后续请求全部走缓存，不重复打 oauth 接口
--   （进一步优化可加 ngx.timer 后台协程定时预拉取，见 README 说明）
local cjson = require "cjson"

local _M = {}

-- 过期提前量：缓存 TTL = expires_in - 300
local REFRESH_MARGIN = 300

local function dict()
    local d = ngx.shared.ai_ernie_token_dict
    if not d then
        ngx.log(ngx.ERR, "ernie_token: shared dict ai_ernie_token_dict 未定义")
    end
    return d
end

-- 用 cosocket 调百度 oauth 接口（不依赖 lua-resty-http，bundle 自带能力即可）
local function fetch_token(ak, sk, token_api)
    local backend = token_api and token_api.backend or "aip.baidubce.com"
    local port    = token_api and token_api.port    or 443
    local use_ssl = token_api and token_api.ssl ~= false
    local uri_prefix = token_api and token_api.uri_prefix or "/oauth/2.0/token"

    local path = uri_prefix
        .. "?grant_type=client_credentials"
        .. "&client_id=" .. ngx.escape_uri(ak)
        .. "&client_secret=" .. ngx.escape_uri(sk)

    local sock = ngx.socket.tcp()
    sock:settimeouts(5000, 5000, 5000)
    local ok, err = sock:connect(backend, port)
    if not ok then
        return nil, "connect fail: " .. tostring(err)
    end
    if use_ssl then
        local session, serr = sock:sslhandshake(nil, backend, false)
        if not session then
            sock:close()
            return nil, "sslhandshake fail: " .. tostring(serr)
        end
    end

    local req = "GET " .. path .. " HTTP/1.1\r\n"
        .. "Host: " .. backend .. "\r\n"
        .. "Accept: application/json\r\n"
        .. "Connection: close\r\n\r\n"
    local bytes, werr = sock:send(req)
    if not bytes then
        sock:close()
        return nil, "send fail: " .. tostring(werr)
    end

    local body, rerr = sock:receive("*a")
    sock:close()
    if not body then
        return nil, "receive fail: " .. tostring(rerr)
    end

    -- 剥离 HTTP 头，取 JSON body
    local json_str = body:match("\r\n\r\n(.*)$") or ""
    local obj, jerr = cjson.decode(json_str)
    if not obj or not obj.access_token then
        return nil, "oauth response invalid: " .. json_str:sub(1, 200)
    end
    return obj.access_token, nil, obj.expires_in or 2592000
end

-- 获取文心 access_token（带缓存）
function _M.get_token(ak, sk, token_api)
    if not ak or not sk then
        return nil, "missing ernie ak/sk in gateway config"
    end

    local d = dict()
    if not d then return nil, "shared dict missing" end

    local cache_key = "ernie_token:" .. ak

    -- 命中缓存直接返回
    local raw = d:get(cache_key)
    if raw then
        local item = cjson.decode(raw)
        if item and item.token and item.expire_at > ngx.time() then
            return item.token
        end
    end

    -- 未命中/过期：同步换取一次（写入缓存，后续请求不重复打 oauth）
    local token, err, expires_in = fetch_token(ak, sk, token_api)
    if not token then
        return nil, err
    end

    local ttl = math.max(60, (expires_in or 2592000) - REFRESH_MARGIN)
    local item = { token = token, expire_at = ngx.time() + ttl }
    d:set(cache_key, cjson.encode(item), ttl)

    ngx.log(ngx.INFO, "ernie_token: access_token 已刷新并缓存, ttl=", ttl)
    return token
end

-- 管理接口：主动清缓存（如 AK/SK 轮换后）
function _M.flush(ak)
    local d = dict()
    if not d then return false end
    if ak then
        return d:delete("ernie_token:" .. ak)
    end
    return d:flush_all()
end

return _M
