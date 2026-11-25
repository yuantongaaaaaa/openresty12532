-- qwen_req.lua
-- 阿里千问请求转换器
-- 千问 OpenAI 兼容模式（/compatible-mode/v1/chat/completions）与 OpenAI 协议
-- 全方位对齐，body 直接透传；只需替换上游凭证为 DashScope 真实密钥
local _M = {}

function _M.transform(req, route, cfg)
    local sk = cfg and cfg[route.key]
    if not sk or sk == "" then
        return nil, "missing qwen_sk in gateway config"
    end
    ngx.req.set_header("Authorization", "Bearer " .. sk)
    return true
end

return _M
