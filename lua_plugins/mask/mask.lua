-- mask.lua
-- AI 数据脱敏插件：网关侧对 AI 请求/响应做敏感信息脱敏（手机号、身份证）
--
-- 阶段一（access）：用户请求发出前，对 OpenAI 请求体中的 messages content
--   （含多模态数组格式）与 tool_calls.function.arguments 做正则脱敏，
--   上游 LLM 收到的就是脱敏后的数据（敏感信息不离开网关）。
-- 阶段二（body_filter）：对模型响应做脱敏。SSE 流式按帧解析 delta.content，
--   带跨帧尾部拼接（号码被拆到相邻帧之间也能识别，见 tail 机制）；非流式
--   JSON 整段缓冲到 eof 后整体脱敏（等长替换，Content-Length 不变）；其他
--   Content-Type（错误页等）直通不处理。
--
-- 设计（对应文档"AI-数据脱敏插件"，修复实现缺陷）：
--   1) 文档 mask_text 两处 "return send" 笔误（应为 return s）：
--      返回 nil 会把 content 置空，用户数据直接丢失。
--   2) 文档先跑手机号再跑身份证：18 位身份证中间恰好夹着 11 位"类手机号"
--      （如 110101199001011234 中的 19900101123），手机号正则先命中会
--      破坏身份证、后续身份证正则再也匹配不上。必须先身份证后手机号，
--      且所有正则加 (?<!\d)...(?!\d) 数字边界断言，防止订单号/时间戳等
--      更长数字串被部分匹配破坏。
--   3) 文档 access 只处理 messages.content，且无 type 守卫：messages 里混入
--      JSON null（cjson 解码为 userdata，truthy）时访问 item.content 直接崩
--      500；大 body 落盘临时文件时 get_body_data 返回 nil 直接跳过（漏脱敏）；
--      tool_calls.function.arguments 漏脱敏。
--   4) 文档 body_filter 只处理 delta.content：非流式（stream:false）响应的
--      choices[1].message.content 完全漏脱敏；非流式 JSON 不含 "\n\n" 会整段
--      滞留缓冲直到 eof 才原样吐出（内存膨胀 + 未脱敏）。修正：按
--      Content-Type 分流 SSE / JSON / 直通三种模式。
--   5) 文档按 "\n\n" 拆帧不兼容 CRLF 行尾，data 行正则 ^data: (.*)$ 匹配
--      不到多行 data。修正：\r 归一 + 多行 data 拼接。
--   6) 文档没有跨 chunk 拼接：流式下手机号被切到相邻帧之间就漏脱敏。
--      修正：delta.content 掩码时带 24 字节尾部拼接（tail 机制），延迟的
--      尾部内容在 [DONE] 帧之前以合成帧补发，客户端拼接后内容完整。
--   7) 文档独立 location + proxy_pass http://llm-upstream 与本项目统一入口
--      冲突（同名 location 重复定义直接起不来）；本项目按惯例链式接入
--      /v1/chat/completions 的 access / body_filter 链。
--   8) 文档 lua_regex_match_limit 100000 实际是把 PCRE 匹配上限调低（默认
--      更高），这里的正则无回溯风险，无需设置。
--
-- 挂载方式（见 nginx.conf，每阶段只有一个 *_by_lua_block，必须链式调用）：
--   access_by_lua_block {
--       require("prompt_template.prompt_template").access()
--       require("mask.mask").access()        -- 必须在 ai_proxy 之前
--       require("ai_proxy.ai_proxy").access()
--       ...
--   }
--   body_filter_by_lua_block {
--       require("ai_proxy.ai_proxy").body_filter()
--       require("mask.mask").body_filter()   -- 紧跟 ai_proxy 之后
--       ...
--   }
-- 开关（server 级 set，见 nginx.conf）：
--   set $mask_enable_req  "on";   -- 请求侧脱敏开关
--   set $mask_enable_resp "on";   -- 响应侧脱敏开关
-- 自检接口：_M.test()，挂 content_by_lua location（/ai/mask_selftest）

local cjson = require "cjson"

local _M = {}
local PLUGIN = "[mask]"

-- ---------------------------------------------------------------------------
-- 可调配置
-- ---------------------------------------------------------------------------
local TAIL_LEN     = 24            -- 跨帧号码前缀最大保留字节（身份证前缀 17 位 + 余量）
local MAX_JSON_BUF = 4 * 1024 * 1024   -- 非流式 JSON 缓冲上限（超限直通保护）
local SSE_MAX_BUF  = 64 * 1024         -- SSE 残留缓冲上限（超限直通保护）

-- ---------------------------------------------------------------------------
-- 正则
--   (?<!\d)...(?!\d) 数字边界断言：号码两侧不能还有数字，防止订单号/时间戳
--   等更长数字串被"部分匹配"破坏（如 19 位数字串里抠出 18 位身份证）。
--   PCRE / PCRE2 都支持固定长度 lookbehind，OpenResty 可用。
--   替换均为等长：手机号 11 -> 11、身份证 18 -> 18 / 15 -> 15，
--   保证改写前后 Content-Length 一致。
-- ---------------------------------------------------------------------------
local RE_ID18   = [[(?<!\d)(\d{6})(\d{8})([\dXx]{4})(?!\d)]]   -- 110101199001011234 -> 110101********1234
local RE_ID15   = [[(?<!\d)(\d{6})(\d{5})(\d{4})(?!\d)]]       -- 110101900101123    -> 110101*****1123
local RE_MOBILE = [[(?<!\d)(1[3-9]\d)(\d{4})(\d{4})(?!\d)]]   -- 13800138000 -> 138****8000（3+4+4 位等长；文档原文 (1[3-9])(\d{4})(\d{4}) 只 9 位匹配不到 11 位手机号）

-- ---------------------------------------------------------------------------
-- 内部工具函数
-- ---------------------------------------------------------------------------

-- 读取完整请求体（兼容超过 client_body_buffer_size 落盘临时文件的大 body）
local function read_request_body()
    ngx.req.read_body()
    local body = ngx.req.get_body_data()
    if body then return body end

    local file = ngx.req.get_body_file()
    if not file then return nil end
    local f = io.open(file, "rb")
    if not f then return nil end
    local data = f:read("*a")
    f:close()
    return data
end

-- 核心掩码：先身份证后手机号（身份证中间的 11 位数字可能长得像手机号，
-- 必须先处理长模式），等长替换
local function mask_text(s)
    if type(s) ~= "string" or s == "" then return s end
    s = ngx.re.gsub(s, RE_ID18, "$1********$3", "jo")
    s = ngx.re.gsub(s, RE_ID15, "$1*****$3", "jo")
    s = ngx.re.gsub(s, RE_MOBILE, "$1****$3", "jo")
    return s
end

-- 递归掩码 OpenAI 请求体中的敏感字段，返回是否有改动
--   messages[].content                （字符串 / 多模态数组 part.text）
--   messages[].tool_calls[].function.arguments
local function mask_req_obj(req)
    local changed = false
    local messages = req.messages
    if type(messages) ~= "table" then return changed end
    for _, item in ipairs(messages) do
        -- 坑：cjson 把 JSON null 解码为 userdata（truthy），必须 type 守卫
        if type(item) == "table" then
            local c = item.content
            if type(c) == "string" then
                local mc = mask_text(c)
                if mc ~= c then item.content = mc; changed = true end
            elseif type(c) == "table" then
                -- 多模态 content：[{type="text",text=...}, {type="image_url",...}]
                for _, part in ipairs(c) do
                    if type(part) == "table" and type(part.text) == "string" then
                        local mt = mask_text(part.text)
                        if mt ~= part.text then part.text = mt; changed = true end
                    end
                end
            end
            local tcs = item.tool_calls
            if type(tcs) == "table" then
                for _, tc in ipairs(tcs) do
                    -- 坑：function 是 Lua 保留字，必须用 tc["function"] 下标访问
                    if type(tc) == "table" then
                        local fn = tc["function"]
                        if type(fn) == "table" and type(fn.arguments) == "string" then
                            local ma = mask_text(fn.arguments)
                            if ma ~= fn.arguments then
                                fn.arguments = ma
                                changed = true
                            end
                        end
                    end
                end
            end
        end
    end
    return changed
end

-- ---------------------------------------------------------------------------
-- 阶段一：access —— 请求侧脱敏
-- ---------------------------------------------------------------------------
function _M.access()
    if ngx.var.mask_enable_req ~= "on" then return end

    local raw = read_request_body()
    if not raw or raw == "" then
        ngx.log(ngx.WARN, PLUGIN, " empty body, skip")
        return
    end
    local req
    local ok, err = pcall(function() req = cjson.decode(raw) end)
    if not ok or type(req) ~= "table" then
        -- fail-open：非 JSON / 非对象请求体不改写，交上游自行报错
        ngx.log(ngx.ERR, PLUGIN, " json decode fail: ", tostring(err or req))
        return
    end

    if not mask_req_obj(req) then return end

    -- 写回 body（set_body_data 自动更新内部 content_length，proxy 转发时
    -- 生成正确的 Content-Length，无需手动改头）
    ngx.req.set_body_data(cjson.encode(req))
    ngx.log(ngx.INFO, PLUGIN, " request masked, body_len=", #raw)
end

-- ---------------------------------------------------------------------------
-- 阶段二：body_filter —— 响应侧脱敏
-- ---------------------------------------------------------------------------

-- 从 s 末尾取最多 max_bytes 字节，切点前移到 UTF-8 字符边界
-- （tail 若切在多字节字符中间，输出帧与合成帧拼回来会乱码）
local function utf8_tail(s, max_bytes)
    local n = #s
    if n <= max_bytes then return s end
    local start = n - max_bytes + 1
    -- 切点落在续字节（0x80-0xBF）上时向前移，保证字符完整
    while start > 1 and s:byte(start) >= 0x80 and s:byte(start) < 0xC0 do
        start = start - 1
    end
    return s:sub(start)
end

-- 处理一个完整 SSE 事件帧，返回替换后的帧文本（可能包含补发的 tail 合成帧）
local function mask_sse_frame(ev, mc)
    -- 只有纯 data 行的事件才处理；含 event:/id:/注释行的事件原样透传（罕见）
    local parts = {}
    for line in ev:gmatch("[^\r\n]+") do
        local d = line:match("^data:%s?(.*)$")
        if not d then return ev end
        parts[#parts + 1] = d
    end
    local data = table.concat(parts, "\n")
    if data == "" then return ev end

    -- [DONE] 帧：先把 tail 里延迟的内容合成一帧补发（必须在 [DONE] 之前，
    -- 否则客户端看到 [DONE] 就停止拼接，延迟内容会丢）
    if data == "[DONE]" then
        if mc.tail == "" then return "data: [DONE]" end
        local done_out = "data: " .. cjson.encode({
            choices = { { index = 0, delta = { content = mc.tail },
                           finish_reason = nil } },
        }) .. "\n\n" .. "data: [DONE]"
        mc.tail = ""
        return done_out
    end

    local ok, obj = pcall(cjson.decode, data)
    if not ok or type(obj) ~= "table" then return ev end  -- 非 JSON 帧透传
    local choices = obj.choices
    if type(choices) ~= "table" or type(choices[1]) ~= "table" then
        return ev  -- usage 帧（choices 为空）等原样透传
    end
    local delta = choices[1].delta
    -- 坑：delta/content 可能是 cjson.null（userdata），type 检查天然跳过
    if type(delta) ~= "table" or type(delta.content) ~= "string" then
        return ev  -- role 帧 / finish 帧等无 content 的帧原样透传
    end

    -- tail 机制：上一帧掩码后末尾 24 字节 + 本帧 content 一起掩码，
    -- 号码被切到相邻帧之间也能识别；掩码后重新截尾延迟输出
    local combined = mc.tail .. delta.content
    if combined == "" then return ev end
    -- 透传条件：无延迟内容且文本不含任何数字片段（含数字就可能是被
    -- 截断的号码前缀，必须进 tail 机制等待与后续帧拼接，否则漏脱敏）
    if mc.tail == "" and not combined:find("%d") then
        return ev  -- 无敏感且无延迟内容：原帧透传（不改写，避免键序变化）
    end
    local masked = mask_text(combined)
    if #masked > TAIL_LEN then
        local tail = utf8_tail(masked, TAIL_LEN)
        delta.content = masked:sub(1, #masked - #tail)
        mc.tail = tail
    else
        delta.content = ""
        mc.tail = masked
    end
    return "data: " .. cjson.encode(obj)
end

function _M.body_filter()
    local ctx = ngx.ctx
    local mc = ctx.mask_resp_ctx
    if not mc then
        -- 首包：读开关 + 按 Content-Type 确定响应模式
        if ngx.var.mask_enable_resp ~= "on" then
            ctx.mask_resp_ctx = { mode = "off" }
            return
        end
        local ct = string.lower(ngx.header["Content-Type"] or "")
        local mode = "passthrough"
        if ct:find("text/event-stream", 1, true) then
            mode = "sse"
        elseif ct:find("application/json", 1, true) then
            mode = "json"
        end
        mc = { mode = mode, buf = "", tail = "" }
        ctx.mask_resp_ctx = mc
        if mode == "passthrough" then
            return  -- 非 AI 响应（错误页等）直通
        end
    elseif mc.mode == "off" or mc.mode == "passthrough" then
        return
    end

    local chunk = ngx.arg[1] or ""
    local eof = ngx.arg[2]

    -- ---------- 非流式 JSON：整段缓冲到 eof 后整体掩码（等长替换，
    -- ---------- Content-Length 不变；本 location 的 header_filter 也已清掉
    -- ---------- Content-Length 走 chunked，双保险）
    if mc.mode == "json" then
        if eof then
            ngx.arg[1] = mask_text(mc.buf .. chunk)
            mc.buf = ""
        else
            mc.buf = mc.buf .. chunk
            if #mc.buf > MAX_JSON_BUF then
                -- 超限保护：直通已缓冲内容，后续转 passthrough（fail-open）
                ngx.arg[1] = mc.buf
                mc.buf = ""
                mc.mode = "passthrough"
            else
                ngx.arg[1] = ""
            end
        end
        return
    end

    -- ---------- SSE 流式：拆帧 + 帧内掩码 + 跨帧 tail 拼接 ----------
    local buf = mc.buf .. chunk
    if buf == "" then
        if eof and mc.tail ~= "" then
            -- 上游没发 [DONE] 就结束（异常截断）：补发 tail 合成帧防丢内容
            ngx.arg[1] = "data: " .. cjson.encode({
                choices = { { index = 0, delta = { content = mc.tail },
                               finish_reason = nil } },
            }) .. "\n\n"
            mc.tail = ""
        end
        return
    end
    buf = buf:gsub("\r", "")   -- CRLF 行尾归一（JSON 字符串内 \r 都是转义字面量，安全）

    -- 不做字节流级快速透传：号码可能被 chunk 边界切成任意片段，若把"无数字
    -- 的帧前半段"透传出去，后半段滞留后只能拆出残缺帧，号码片段再也拼不回
    -- 来。必须无条件缓冲拆帧，帧级（decode 后 content）拼接由 tail 机制保证。

    -- 拆帧处理
    local out = {}
    while true do
        local pos = buf:find("\n\n", 1, true)
        if not pos then break end
        local ev = buf:sub(1, pos - 1)
        buf = buf:sub(pos + 2)
        out[#out + 1] = mask_sse_frame(ev, mc)
        out[#out + 1] = "\n\n"
    end

    if #buf > SSE_MAX_BUF then
        -- 超长残留保护：直接透传（防异常上游无 \n\n 的假 SSE 撑爆内存）
        out[#out + 1] = buf
        mc.buf = ""
    elseif eof then
        if mc.tail ~= "" then
            -- 上游没发 [DONE] 就结束：补发 tail 合成帧
            out[#out + 1] = "data: " .. cjson.encode({
                choices = { { index = 0, delta = { content = mc.tail },
                               finish_reason = nil } },
            }) .. "\n\n"
            mc.tail = ""
        end
        -- 残留半帧：整体掩码后输出（避免丢数据）
        if buf ~= "" then out[#out + 1] = mask_text(buf) end
        mc.buf = ""
    else
        mc.buf = buf  -- 残留滞留，等后续 chunk 拼出完整帧
    end

    ngx.arg[1] = table.concat(out)
end

-- ---------------------------------------------------------------------------
-- 自检接口：模仿 test 插件，验证开关变量 / 正则行为 / lookbehind 支持
-- ---------------------------------------------------------------------------
function _M.test()
    ngx.log(ngx.INFO, "=== mask 插件自检 ===")
    ngx.header["Content-Type"] = "application/json"

    local status = { code = 0, msg = "mask plugin ok", config = {
        mask_enable_req = ngx.var.mask_enable_req,
        mask_enable_resp = ngx.var.mask_enable_resp,
        tail_len = TAIL_LEN,
    }, checks = {} }
    local checks = status.checks

    -- 1. 开关变量是否声明（未 set 的变量读出来是 nil）
    if ngx.var.mask_enable_req ~= nil then
        checks[#checks + 1] = { item = "nginx var $mask_enable_req", ok = true,
            detail = ngx.var.mask_enable_req }
    else
        status.code = 1
        checks[#checks + 1] = { item = "nginx var $mask_enable_req", ok = false,
            hint = 'server 级添加 set $mask_enable_req "on";' }
    end
    if ngx.var.mask_enable_resp ~= nil then
        checks[#checks + 1] = { item = "nginx var $mask_enable_resp", ok = true,
            detail = ngx.var.mask_enable_resp }
    else
        status.code = 1
        checks[#checks + 1] = { item = "nginx var $mask_enable_resp", ok = false,
            hint = 'server 级添加 set $mask_enable_resp "on";' }
    end

    -- 2. PCRE lookbehind（(?<!\d)）支持检测：不支持时 ngx.re 编译抛错
    local ok_lb = pcall(function()
        return ngx.re.match("x13800138000y", RE_MOBILE, "jo") ~= nil
    end)
    checks[#checks + 1] = { item = "PCRE lookbehind 支持", ok = ok_lb }
    if not ok_lb then status.code = 1 end

    -- 3. 正则自测样例（等长掩码 + 边界断言）
    local samples = {
        { name = "手机号", input = "电话13800138000，谢谢",
          expect = "电话138****8000，谢谢" },
        { name = "18位身份证", input = "110101199001011234",
          expect = "110101********1234" },
        { name = "15位身份证", input = "110101900101123",
          expect = "110101*****1123" },
        { name = "长数字串不误伤", input = "订单20231128123456789号",
          expect = "订单20231128123456789号" },
        { name = "身份证内手机号不误伤", input = "110101199001011234",
          expect = "110101********1234" },
        { name = "手机号后跟数字不误伤", input = "138001380001",
          expect = "138001380001" },
    }
    for _, s in ipairs(samples) do
        local got = mask_text(s.input)
        local ok = got == s.expect
        if not ok then status.code = 1 end
        checks[#checks + 1] = { item = "正则自测 " .. s.name, ok = ok,
            detail = s.input .. " -> " .. got }
    end

    -- 4. 挂载顺序提示
    checks[#checks + 1] = { item = "挂载顺序", ok = true,
        hint = "access 挂在 ai_proxy 之前（协议转换前脱敏原始请求，向量缓存的"
            .. "key/入库 prompt 才不会拿到明文）；body_filter 紧跟 ai_proxy 之后"
            .. "（标准格式响应脱敏，下游累积的 answer 也是脱敏后的）" }

    ngx.say(cjson.encode(status))
end

return _M
