-- openai_req.lua
-- OpenAI 请求转换器：协议基准，body 原样透传
-- 唯一要做的事：把客户端网关 token 替换成 OpenAI 真实密钥
-- （文档原版漏掉了这一步，上游会拿到网关 token 导致 401）
local _M = {}

function _M.transform(req, route, cfg)
    local sk = cfg and cfg[route.key]
    if not sk or sk == "" then
        return nil, "missing openai_sk in gateway config"
    end
    ngx.req.set_header("Authorization", "Bearer " .. sk)
    -- body/URI 无需改动（ngx.var.ai_request_uri 已由主入口设置）
    return true
end

return _M
