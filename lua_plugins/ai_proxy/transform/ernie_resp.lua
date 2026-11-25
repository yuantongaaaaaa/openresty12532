-- ernie_resp.lua
-- 百度文心一言响应转换器：百度私有结构 -> 拼装标准 OpenAI chunk
--
-- 百度私有流式分片结构（与 OpenAI 差异巨大）：
--   data: {"id":"as-xxx","object":"chat.completion","created":1766...,
--          "sentence_id":0,"is_end":false,"is_truncated":false,
--          "result":"你好","need_clear_history":false,
--          "function_call":{"name":"get_weather","thoughts":"","arguments":"{\"city\":\"上海\"}"},
--          "usage":{...}}
--   data: {"is_end":true,"result":"","function_call":null}   <- 结束标记，不是 [DONE]
--
-- 转换规则（对应文档列出的核心差异）：
--   1. 无 choices/delta/tool_calls -> 逐片拼装 choices[0].delta
--   2. function_call 单层对象 -> tool_calls 数组（name/arguments 跨分片累积）
--   3. 顶层 result -> delta.content
--   4. is_end:true -> 先输出 finish_reason chunk 再输出 [DONE]
--   5. error_code 错误对象 -> OpenAI 错误格式
local cjson = require "cjson"

local sse = require "ai_proxy.sse"

local _M = {}

-- 前置声明：make_chunk 定义在 on_event 之后，Lua 局部变量需先声明
local make_chunk

-- 处理一个完整 SSE 事件
local function on_event(event_str)
    local ctx = ngx.ctx
    local data = sse.extract_data(event_str)
    if not data then
        return event_str .. "\n\n" -- 含 event:/id: 行，原样透传
    end

    local ok, obj = pcall(cjson.decode, data)
    if not ok or type(obj) ~= "table" then
        return event_str .. "\n\n" -- 解析失败原样透传
    end

    -- 百度错误对象（token 过期/限流等）-> OpenAI 错误格式
    -- 注意：cjson 把 JSON null 解码为 cjson.null（userdata，truthy），
    -- 判断字段存在性必须用 type 检查，不能直接 if obj.xxx
    if type(obj.error_code) == "number" then
        return sse.emit_json({
            error = {
                message = obj.error_msg or ("baidu error " .. obj.error_code),
                type = "upstream_error",
                code = obj.error_code,
            }
        }) .. sse.DONE_EVENT
    end

    -- 缓存会话 id（第一个分片带 id，后续分片不带；null 时需 type 检查）
    if type(obj.id) == "string" then ctx.ernie_msg_id = obj.id end

    -- 累积 function_call 的 name/arguments（百度跨分片拆开发送）
    -- function_call 可能是 null（cjson.null userdata），需 type 检查
    local fc = obj.function_call
    if type(fc) == "table" then
        if fc.name and fc.name ~= "" then
            ctx.ernie_fn_name = fc.name
        end
        if fc.arguments and fc.arguments ~= "" then
            ctx.ernie_fn_args = (ctx.ernie_fn_args or "") .. fc.arguments
        end
    end

    -- is_end 分片也可能带最后一包 result，不能直接丢（文档原版会丢内容）
    local out = {}
    if obj.is_end then
        if type(obj.result) == "string" and obj.result ~= "" then
            out[#out + 1] = sse.emit_json(make_chunk(ctx, obj))
        end
        -- 结束前补一个 finish_reason chunk，OpenAI SDK 依赖它收尾
        out[#out + 1] = sse.emit_json({
            id = ctx.ernie_msg_id or "ernie-proxy",
            object = "chat.completion.chunk",
            created = obj.created or ngx.time(),
            model = ctx.openai_req.model,
            choices = {{
                index = 0,
                delta = {},
                finish_reason = (ctx.ernie_fn_name and "tool_calls") or "stop",
            }},
        })
        out[#out + 1] = sse.DONE_EVENT
        return table.concat(out)
    end

    -- 普通内容分片 -> 标准 chunk
    return sse.emit_json(make_chunk(ctx, obj))
end

-- 组装一个标准 OpenAI chunk
make_chunk = function(ctx, obj)
    local delta = {}

    -- 首片标记 role（函数需要在使用前声明，见文件顶部）
    if not ctx.ernie_first_chunk then
        delta.role = "assistant"
        ctx.ernie_first_chunk = true
    end

    -- 顶层 result -> delta.content（null 时 cjson.null 是 truthy，需 type 检查）
    if type(obj.result) == "string" and obj.result ~= "" then
        delta.content = obj.result
    end

    -- 百度 function_call（单层对象）-> tool_calls 数组
    if type(obj.function_call) == "table" or ctx.ernie_fn_name then
        delta.tool_calls = {{
            index = 0,
            id = ctx.ernie_fn_id or ("call_" .. (ctx.ernie_msg_id or "ernie")),
            type = "function",
            -- Lua 5.1 中 function 是保留字，表字段需用字符串形式
            ["function"] = {
                name = ctx.ernie_fn_name or "",
                arguments = ctx.ernie_fn_args or "",
            },
        }}
        if not ctx.ernie_fn_id then
            ctx.ernie_fn_id = "call_" .. (ctx.ernie_msg_id or "ernie")
        end
    end

    return {
        id = ctx.ernie_msg_id or "ernie-proxy",
        object = "chat.completion.chunk",
        created = obj.created or ngx.time(),
        model = ctx.openai_req.model,
        choices = {{
            index = 0,
            delta = delta,
            finish_reason = nil,
        }},
    }
end

function _M.filter()
    local ctx = ngx.ctx
    local chunk, eof = ngx.arg[1], ngx.arg[2]
    local out

    if sse.is_stream() then
        out = sse.filter_chunk(ctx, chunk, on_event)
        if eof then out = out .. sse.flush_rest(ctx) end
    else
        -- 非流式：{"id","result","is_end":true,...} -> OpenAI 完整 completion
        out = sse.filter_json(ctx, chunk, eof, function(obj)
            if type(obj.error_code) == "number" then
                return cjson.encode({
                    error = {
                        message = obj.error_msg or ("baidu error " .. obj.error_code),
                        type = "upstream_error",
                        code = obj.error_code,
                    }
                })
            end
            local message = {
                role = "assistant",
                content = type(obj.result) == "string" and obj.result or "",
            }
            if type(obj.function_call) == "table" then
                message.tool_calls = {{
                    id = "call_" .. (type(obj.id) == "string" and obj.id or "ernie"),
                    type = "function",
                    ["function"] = {
                        name = obj.function_call.name,
                        arguments = obj.function_call.arguments,
                    },
                }}
            end
            return cjson.encode({
                id = type(obj.id) == "string" and obj.id or "ernie-proxy",
                object = "chat.completion",
                created = type(obj.created) == "number" and obj.created or ngx.time(),
                model = ctx.openai_req.model,
                choices = {{
                    index = 0,
                    message = message,
                    finish_reason = "stop",
                }},
                usage = obj.usage,
            })
        end)
    end

    ngx.arg[1] = out
end

return _M
