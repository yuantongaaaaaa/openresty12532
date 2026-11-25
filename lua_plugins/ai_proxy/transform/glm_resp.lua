-- glm_resp.lua
-- 智谱 GLM 响应转换器 -> OpenAI 标准格式
-- 重点修复（文档提到的两个经典坑）：
--   1. 老版本返回 object:chat.completion（非 chunk），OpenAI SDK 会报错 -> 强制改 chunk
--   2. tool_calls 里经常缺失 index 字段，OpenAI SDK 强依赖 -> 自动补齐
-- 同时兼容非流式（stream=false）响应
local cjson = require "cjson"

local sse = require "ai_proxy.sse"

local _M = {}

-- 补齐 object 与 tool_calls index（流式 chunk / 非流式完整对象共用）
local function patch_obj(obj, is_chunk)
    obj.object = is_chunk and "chat.completion.chunk" or "chat.completion"
    local choices = obj.choices or {}
    for _, ch in ipairs(choices) do
        local tools
        if is_chunk then
            local delta = ch.delta or {}
            tools = delta.tool_calls
        else
            tools = ch.message and ch.message.tool_calls
        end
        for idx, tc in ipairs(tools or {}) do
            if tc.index == nil then tc.index = idx - 1 end
        end
    end
    return obj
end

-- 处理一个完整 SSE 事件
local function on_event(event_str)
    local data = sse.extract_data(event_str)
    if not data then
        return event_str .. "\n\n" -- 含 event:/id: 行，原样透传
    end
    if data == "[DONE]" then
        return sse.DONE_EVENT
    end

    local ok, obj = pcall(cjson.decode, data)
    if not ok or type(obj) ~= "table" then
        return event_str .. "\n\n" -- 解析失败原样透传
    end

    -- 上游错误对象（如 {"code":...}）原样透传，客户端能读懂
    -- code 可能是 null（cjson.null 是 truthy userdata），需 type 检查
    if type(obj.code) == "number" and not obj.choices then
        return event_str .. "\n\n"
    end

    return sse.emit_json(patch_obj(obj, true))
end

function _M.filter()
    local ctx = ngx.ctx
    local chunk, eof = ngx.arg[1], ngx.arg[2]
    local out

    if sse.is_stream() then
        out = sse.filter_chunk(ctx, chunk, on_event)
        if eof then out = out .. sse.flush_rest(ctx) end
    else
        -- 非流式：完整 JSON 也要修 object / index
        out = sse.filter_json(ctx, chunk, eof, function(obj)
            if obj.error then
                return cjson.encode(obj) -- OpenAI 风格错误直接透传
            end
            if type(obj.code) == "number" and not obj.choices then
                return cjson.encode(obj) -- GLM 私有错误码透传
            end
            return cjson.encode(patch_obj(obj, false))
        end)
    end

    ngx.arg[1] = out
end

return _M
