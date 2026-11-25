-- glm_req.lua
-- 智谱 GLM 请求转换器
-- GLM 请求结构与 OpenAI 几乎一致（/api/paas/v4/chat/completions），body 直接透传；
-- 只需替换上游凭证为智谱真实密钥
local _M = {}

function _M.transform(req, route, cfg)
    local sk = cfg and cfg[route.key]
    if not sk or sk == "" then
        return nil, "missing glm_sk in gateway config"
    end
    ngx.req.set_header("Authorization", "Bearer " .. sk)
    return true
end

return _M
