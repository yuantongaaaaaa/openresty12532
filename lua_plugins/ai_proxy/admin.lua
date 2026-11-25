-- admin.lua
-- AI 网关管理接口：写入网关 token 映射 / 覆盖路由表 / 清理文心 token 缓存
--
-- 对应文档"第一步 token 映射"中的 init_admin.lua：
--   网关初始化后通过管理接口把「网关 token -> 4 家厂商密钥」写入共享内存，
--   之后所有客户端只用这一套网关 token 访问全部大模型。
--
-- 使用示例（content_by_lua location）：
--   POST /ai/proxy_admin   body: {"action":"set_token","gateway_token":"...","config":{...}}
--
-- 可用 action：
--   set_token : 写入/更新一个网关 token 的密钥配置
--     config 结构：{
--       allow_models = {"gpt-4o-mini","glm-4","qwen-turbo","ernie-4.0-8k"},
--       openai_sk = "...", glm_sk = "...", qwen_sk = "...",
--       ernie_ak = "...", ernie_sk = "..."
--     }
--   del_token : 删除一个网关 token
--   list      : 列出所有网关 token
--   set_routes: 覆盖路由表（生产指向真实域名；本地测试可指向 mock 地址）
--   flush_ernie: 清空文心 access_token 缓存
--
-- 安全提醒：生产环境必须给该接口加 IP 白名单 + 管理鉴权，不能裸奔！

local cjson = require "cjson"

local _M = {}

local GATEWAY_DICT = "ai_gateway_token_dict"
local ROUTES_DICT  = "ai_proxy_routes_dict"

local function reply(code, msg, data)
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson.encode({ code = code, msg = msg, data = data }))
end

local function read_body()
    ngx.req.read_body()
    local body = ngx.req.get_body_data()
    if not body then return nil end
    return cjson.decode(body)
end

-- 处理一个管理请求（挂在 content_by_lua）
function _M.run()
    if ngx.req.get_method() ~= "POST" then
        return reply(1, "only POST allowed")
    end

    local ok, req = pcall(read_body)
    if not ok or type(req) ~= "table" then
        return reply(1, "json decode fail")
    end

    local action = req.action

    if action == "set_token" then
        local gt = req.gateway_token
        local config = req.config
        if not gt or gt == "" or type(config) ~= "table" then
            return reply(1, "gateway_token / config required")
        end
        local d = ngx.shared[GATEWAY_DICT]
        if not d then return reply(1, "shared dict missing: " .. GATEWAY_DICT) end
        local ok_set, err = d:set(gt, cjson.encode(config))
        if not ok_set then return reply(1, "set fail: " .. tostring(err)) end
        ngx.log(ngx.INFO, "ai_proxy admin: 网关 token [", gt, "] 已写入, models=",
            table.concat(config.allow_models or {}, ","))
        return reply(0, "ok")

    elseif action == "del_token" then
        local d = ngx.shared[GATEWAY_DICT]
        if not d then return reply(1, "shared dict missing") end
        d:delete(req.gateway_token or "")
        return reply(0, "ok")

    elseif action == "list" then
        local d = ngx.shared[GATEWAY_DICT]
        if not d then return reply(1, "shared dict missing") end
        local keys = d:get_keys(0)
        return reply(0, "ok", { tokens = keys or {} })

    elseif action == "set_routes" then
        -- 覆盖路由表：{ openai={backend=..,uri=..}, ..., ernie={..., token_api={...}} }
        if type(req.routes) ~= "table" then return reply(1, "routes required") end
        local d = ngx.shared[ROUTES_DICT]
        if not d then return reply(1, "shared dict missing: " .. ROUTES_DICT) end
        d:set("routes", cjson.encode(req.routes))
        ngx.log(ngx.INFO, "ai_proxy admin: 路由表已覆盖")
        return reply(0, "ok")

    elseif action == "flush_ernie" then
        local ernie_token = require "ai_proxy.ernie_token"
        ernie_token.flush()
        return reply(0, "ok")

    else
        return reply(1, "unknown action: " .. tostring(action))
    end
end

return _M
