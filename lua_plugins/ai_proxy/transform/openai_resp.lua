-- openai_resp.lua
-- OpenAI 响应转换器：基准协议，直接透传
-- SSE 粘包无需处理：客户端 SDK 自己会按流解析；网关切割反而多一次拷贝
local _M = {}

function _M.filter()
    -- 什么都不改，原样返回
end

return _M
