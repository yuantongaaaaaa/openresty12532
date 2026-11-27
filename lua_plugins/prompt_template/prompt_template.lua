-- prompt_template.lua
-- AI 提示词模板插件：access 阶段把渲染好的 system prompt 注入 OpenAI 请求 messages 头部
-- 用途：提示词工程在网关层统一管控（模板 / 占位变量都收敛在 nginx.conf，业务方无感知）
--
-- 配置全部走 nginx 变量（location 内 set，见 nginx.conf 示例）：
--   set $prompt_template "...";                      -- 提示词模板，占位符 ${key}
--   set $prompt_var_header_map "key:Header名,...";   -- 占位符 <- 请求头映射
--   set $prompt_force_override "on";                 -- on: 已有 system 也覆盖；off: 已有则跳过
--   set $prompt_template_file "/path/tpl.txt";       -- 可选：模板外置文件（优先于内联变量）
--
-- ⚠ 内联模板的占位符写法：nginx 的 set 等变量插值指令无法转义 $，
--   直接写 ${key} 会被当作 nginx 变量引用，未定义时 nginx -t 报
--   invalid variable name，服务起不来。必须写成 $dollar{key}：
--   http{} 已定义 geo $dollar { default "$"; }，运行时插值为字面 ${key}。
--   长模板推荐用 $prompt_template_file 外置文件，文件里可直接写 ${key}。
--
-- 挂载方式（见 nginx.conf 示例）：
--   access_by_lua_block { require("prompt_template.prompt_template").access() }
-- 挂载顺序：必须放在 ai_proxy.access() 之前（access 链第一个），
--   在 ai_proxy 解析 body / 协议转换之前改写原始 OpenAI 请求体；
--   放它后面会拿 OpenAI 格式 body 覆盖 ernie 等已转换的上游协议格式。
-- 自检接口：_M.test()，可挂在任意 content_by_lua location

local cjson = require "cjson"

local _M = {}
local PLUGIN = "[prompt-tpl]"

-- ---------------------------------------------------------------------------
-- 内部工具函数
-- ---------------------------------------------------------------------------

-- 读取完整请求体（兼容超过 client_body_buffer_size 的大 body）
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

-- 读取模板：文件优先（文件里可直接写 ${key}），其次 nginx 内联变量
local function get_template()
    local tpl_file = ngx.var.prompt_template_file
    if tpl_file and tpl_file ~= "" then
        local f = io.open(tpl_file, "rb")
        if not f then
            ngx.log(ngx.ERR, PLUGIN, " template file open fail: ", tpl_file)
            return nil
        end
        local content = f:read("*a")
        f:close()
        return content or ""
    end
    return ngx.var.prompt_template or ""
end

-- 解析占位符映射，格式示例：tenant_id:X-Tenant-Id,source:X-Source
local function parse_header_map(s)
    local m = {}
    if not s or s == "" then return m end
    for pair in s:gmatch("[^,]+") do
        local k, v = pair:match("^%s*(.-)%s*:%s*(.-)%s*$")
        if k and v and k ~= "" and v ~= "" then
            m[k] = v
        end
    end
    return m
end

-- 从请求头提取占位变量：
--   ngx.req.get_headers() 的 key 全部小写，配置里大小写都要兼容；
--   同名多值 header 的值是 table，取第一个。
local function get_vars(header_map)
    local vars = {}
    if next(header_map) == nil then return vars end
    local headers = ngx.req.get_headers()
    for var_key, h_name in pairs(header_map) do
        local v = headers[string.lower(h_name)]
        if type(v) == "table" then v = v[1] end
        vars[var_key] = v or ""
    end
    return vars
end

-- 模板渲染：把 ${key} 逐个替换为 vars[key]（缺失替换为空串）
-- 用 find(..., plain=true) 逐段字面替换，规避 gsub 的两个坑：
--   1) key 含 -/. 等 pattern 魔法字符会错配/报错
--   2) 替换值含 % 会被 gsub 当作捕获引用（如 %1）篡改内容
local function render(tpl, vars)
    local out = ""
    local i = 1
    while true do
        local s, e = tpl:find("%${", i)          -- %$ 转义 $，{ 是字面量
        if not s then
            out = out .. tpl:sub(i)
            break
        end
        local close_p = tpl:find("}", e + 1, true) -- 找配对的 }，plain 模式
        if not close_p then
            out = out .. tpl:sub(i)                -- 未闭合的 ${ 原样保留
            break
        end
        local key = tpl:sub(e + 1, close_p - 1)
        out = out .. tpl:sub(i, s - 1) .. (vars[key] or "")
        i = close_p + 1
    end
    return out
end

-- ---------------------------------------------------------------------------
-- 阶段：access —— 注入 system prompt
-- ---------------------------------------------------------------------------
function _M.access()
    -- 1. 读 nginx 变量配置（未 set 的变量为 nil；模板为空时插件完全不生效）
    local template = get_template()
    if not template or template == "" then
        return
    end
    local force_override = ngx.var.prompt_force_override == "on"
    local map_str = ngx.var.prompt_var_header_map or ""

    -- 2. 读请求体
    local raw = read_request_body()
    if not raw or raw == "" then
        ngx.log(ngx.WARN, PLUGIN, " empty body, skip")
        return
    end
    local req
    local ok, err = pcall(function() req = cjson.decode(raw) end)
    if not ok or type(req) ~= "table" then
        ngx.log(ngx.ERR, PLUGIN, " json decode err: ", tostring(err or req))
        return
    end
    if type(req.messages) ~= "table" then
        ngx.log(ngx.WARN, PLUGIN, " messages missing, skip")
        return
    end

    -- 3. 提取变量并渲染模板
    local header_map = parse_header_map(map_str)
    local vars = get_vars(header_map)
    -- 内置变量：${model} 取请求体 model（仅当 header 映射未提供同名 key 时）
    if vars.model == nil and type(req.model) == "string" then
        vars.model = req.model
    end
    local system_content = render(template, vars)
    if system_content:match("^%s*$") then
        ngx.log(ngx.INFO, PLUGIN, " rendered empty, skip inject")
        return
    end

    -- 4. 检查是否已有 system 消息（item 可能是 cjson.null，必须 type 守卫）
    local exist_system = false
    for _, item in ipairs(req.messages) do
        if type(item) == "table" and item.role == "system" then
            exist_system = true
            break
        end
    end
    if exist_system and not force_override then
        ngx.log(ngx.INFO, PLUGIN, " system exists, skip inject "
            .. "(set $prompt_force_override on to override)")
        return
    end

    -- 5. 注入：system 放头部；force_override 时移除客户端自带的所有 system
    local system_msg = { role = "system", content = system_content }
    local new_list = { system_msg }
    for _, item in ipairs(req.messages) do
        if not (type(item) == "table" and item.role == "system") then
            new_list[#new_list + 1] = item
        end
    end
    req.messages = new_list

    -- 6. 写回 body（set_body_data 自动更新 Content-Length，无需手动改头）
    ngx.req.set_body_data(cjson.encode(req))
    ngx.ctx.prompt_injected = true
    ngx.log(ngx.INFO, PLUGIN, " inject success, system_len=", #system_content,
        ", force=", tostring(force_override))
end

-- ---------------------------------------------------------------------------
-- 自检接口：模仿 test 插件，验证 nginx 变量配置 / 映射解析 / 渲染结果
-- ---------------------------------------------------------------------------
function _M.test()
    ngx.log(ngx.INFO, "=== prompt_template 插件自检 ===")
    ngx.header["Content-Type"] = "application/json"

    local status = { code = 0, msg = "prompt_template plugin ok", checks = {} }
    local checks = status.checks

    -- 1. nginx 变量是否声明（未 set 的变量读出来是 nil）
    if ngx.var.prompt_template ~= nil then
        checks[#checks + 1] = { item = "nginx var $prompt_template", ok = true,
            detail = "len=" .. #(ngx.var.prompt_template or "") }
    else
        status.code = 1
        checks[#checks + 1] = { item = "nginx var $prompt_template", ok = false,
            hint = 'location 内添加 set $prompt_template "...";' }
    end
    local tpl_file = ngx.var.prompt_template_file
    if tpl_file ~= nil and tpl_file ~= "" then
        checks[#checks + 1] = { item = "nginx var $prompt_template_file", ok = true,
            detail = tpl_file }
    end
    if ngx.var.prompt_force_override ~= nil then
        checks[#checks + 1] = { item = "nginx var $prompt_force_override", ok = true,
            detail = ngx.var.prompt_force_override }
    else
        status.code = 1
        checks[#checks + 1] = { item = "nginx var $prompt_force_override", ok = false,
            hint = 'location 内添加 set $prompt_force_override "off";' }
    end
    if ngx.var.prompt_var_header_map ~= nil then
        checks[#checks + 1] = { item = "nginx var $prompt_var_header_map", ok = true,
            detail = ngx.var.prompt_var_header_map }
    else
        status.code = 1
        checks[#checks + 1] = { item = "nginx var $prompt_var_header_map", ok = false,
            hint = 'location 内添加 set $prompt_var_header_map "key:Header名";' }
    end

    -- 2. 映射解析
    local m = parse_header_map(ngx.var.prompt_var_header_map or "")
    local pairs_out = {}
    for k, v in pairs(m) do
        pairs_out[#pairs_out + 1] = k .. " -> " .. v
    end
    checks[#checks + 1] = { item = "header map 解析", ok = true,
        detail = #pairs_out > 0 and table.concat(pairs_out, ", ") or "(空)" }

    -- 3. 渲染示例（假变量）
    local tpl = get_template() or ""
    if tpl ~= "" then
        local demo = render(tpl, { tenant_id = "demo-123", model = "gpt-4o-mini", lang = "zh" })
        checks[#checks + 1] = { item = "渲染示例", ok = true,
            detail = demo:sub(1, 120) .. (#demo > 120 and "..." or "") }
        -- 提示：模板里若出现未被转义的 nginx 变量残留（值被 nginx 提前替换成空），
        -- 渲染结果会缺内容，此处只做展示不判错
    else
        checks[#checks + 1] = { item = "渲染示例", ok = true,
            detail = "(模板为空，插件不生效；按 nginx.conf 注释配置后 reload)" }
    end

    ngx.say(cjson.encode(status))
end

return _M
