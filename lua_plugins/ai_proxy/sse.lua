-- sse.lua
-- 通用 SSE 工具：body_filter 阶段的粘包切割 + 事件解析
--
-- 背景：proxy_buffering off 时，body_filter 拿到的 chunk 边界是网络包边界，
-- 一个 SSE 事件可能被拆到多个 chunk，多个事件也可能挤在一个 chunk 里。
-- 本模块维护 ngx.ctx.sse_buf 做跨 chunk 缓冲，按 "\n\n" 切出完整事件。

local cjson = require "cjson"

local _M = {}

-- 从单个 SSE 事件块中提取所有 data 行内容
-- 返回：data（拼好的字符串）或 nil（事件含 event:/id:/注释行，应原样透传）
-- 兼容 "data: xxx" 和 "data:xxx"（有无空格都行）
local function extract_data(event_str)
    local parts = {}
    for line in event_str:gmatch("[^\r\n]+") do
        local d = line:match("^data:%s?(.*)$")
        if not d then
            return nil -- 非 data 行（event: / id: / 注释），整块原样透传
        end
        parts[#parts + 1] = d
    end
    return table.concat(parts, "\n")
end

-- 把厂商返回的 JSON 对象编码为一条标准 SSE data 事件
local function emit_json(obj)
    return "data: " .. cjson.encode(obj) .. "\n\n"
end

-- 通用流式处理框架：
--   ctx    : ngx.ctx（内部用 ctx.sse_buf 做缓冲）
--   handler: function(event_str) -> output_string（转换一个完整事件）
-- 返回：本次 body_filter 应输出的字符串（含未消费的完整事件透传）
function _M.filter_chunk(ctx, chunk, handler)
    local buf = (ctx.sse_buf or "") .. (chunk or "")
    local out = {}

    while true do
        local pos = buf:find("\n\n", 1, true)
        if not pos then break end
        local event_str = buf:sub(1, pos - 1)
        buf = buf:sub(pos + 2)
        out[#out + 1] = handler(event_str)
    end

    ctx.sse_buf = buf
    return table.concat(out)
end

-- 流结束：残留的半个事件无法构成完整 JSON，原样输出避免丢数据
function _M.flush_rest(ctx)
    local rest = ctx.sse_buf or ""
    ctx.sse_buf = ""
    return rest
end

-- 非流式响应处理框架：body_filter 各 chunk 拼起来，eof 时一次性转换
--   ctx    : ngx.ctx（内部用 ctx.json_buf 缓冲）
--   chunk  : 本次 chunk
--   eof    : ngx.arg[2]
--   handler: function(full_json_str) -> output_string
-- 返回：本次 body_filter 应输出的字符串（仅 eof 时有内容）
function _M.filter_json(ctx, chunk, eof, handler)
    ctx.json_buf = (ctx.json_buf or "") .. (chunk or "")
    if not eof then return "" end

    local full = ctx.json_buf
    ctx.json_buf = ""
    if full == "" then return "" end

    local ok, obj = pcall(cjson.decode, full)
    if not ok or type(obj) ~= "table" then
        return full -- 解析失败原样返回
    end
    return handler(obj)
end

-- 判断当前响应是否为 SSE 流式
function _M.is_stream()
    local ct = ngx.header["Content-Type"] or ""
    -- plain 模式下 %- 不会被转义，直接用字面串匹配
    return ct:find("text/event-stream", 1, true) ~= nil
end

_M.extract_data = extract_data
_M.emit_json = emit_json
_M.DONE_EVENT = "data: [DONE]\n\n"

return _M
