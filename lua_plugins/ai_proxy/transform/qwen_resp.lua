-- qwen_resp.lua
-- 阿里千问响应转换器
-- 千问原生返回完全对齐 OpenAI 协议，无需字段转换；
-- 仅做 SSE 粘包切割透传（保证跨 chunk 事件完整），非流式原样透传
local sse = require "ai_proxy.sse"

local _M = {}

function _M.filter()
    local ctx = ngx.ctx
    local chunk, eof = ngx.arg[1], ngx.arg[2]

    if not sse.is_stream() then
        return -- 非流式直接透传
    end

    local out = sse.filter_chunk(ctx, chunk, function(event_str)
        return event_str .. "\n\n" -- 原样透传
    end)
    if eof then out = out .. sse.flush_rest(ctx) end

    ngx.arg[1] = out
end

return _M
