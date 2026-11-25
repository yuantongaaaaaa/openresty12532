-- ernie_req.lua
-- 百度文心一言请求转换器（4 家中转换工作量最大）
--
-- OpenAI body -> 百度私有格式：
--   1. 基础字段透传：model / messages / temperature / top_p / stream / tools
--   2. OpenAI user -> 文心 user_id（文心强依赖 user_id，必须带）
--   3. 鉴权：清掉客户端网关 token，填 AK/SK 换取的 access_token（带缓存）
--   4. 路径改为百度专属长路径（由主入口的路由表统一设置）
local cjson = require "cjson"

local ernie_token = require "ai_proxy.ernie_token"

local _M = {}

function _M.transform(req, route, cfg)
    -- 1. 字段映射
    local ernie_req = {
        model       = req.model,
        messages    = req.messages,
        temperature = req.temperature,
        top_p       = req.top_p,
        stream      = req.stream,
        tools       = req.tools,
        -- OpenAI user -> 文心 user_id（必填）
        user_id     = req.user or ("proxy_" .. (ngx.var.remote_addr or "unknown")),
    }
    if req.max_tokens then ernie_req.max_tokens = req.max_tokens end
    if req.functions then ernie_req.functions = req.functions end

    -- 2. AK/SK 换 access_token（shared_dict TTL 缓存，不会每次请求都打 oauth）
    local access_token, terr = ernie_token.get_token(
        cfg.ernie_ak, cfg.ernie_sk, route.token_api)
    if not access_token then
        return nil, "get ernie access_token fail: " .. tostring(terr)
    end
    ngx.req.clear_header("Authorization")
    ngx.req.set_header("Authorization", "Bearer " .. access_token)

    -- 3. 写回转换后的 body（nginx 会自动重算 Content-Length）
    ngx.req.set_body_data(cjson.encode(ernie_req))

    return true
end

return _M
