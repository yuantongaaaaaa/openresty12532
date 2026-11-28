-- ai_proxy.lua
-- AI 网关统一代理插件：一套 OpenAI 标准协议入口，代理 4 家不同协议的大模型
--   openai : gpt-*   -> api.openai.com       （基准协议，透传）
--   glm    : glm-*   -> open.bigmodel.cn     （近 OpenAI，修 object / tool_calls index）
--   qwen   : qwen-*  -> dashscope.aliyuncs.com（OpenAI 兼容模式，转换成本最低）
--   ernie  : ernie-* -> aip.baidubce.com     （百度私有协议，转换工作量最大）
--
-- 挂载方式（见 nginx.conf 示例）：
--   access_by_lua_block        { require("ai_proxy.ai_proxy").access() }
--   header_filter_by_lua_block { require("ai_proxy.ai_proxy").header_filter() }
--   body_filter_by_lua_block   { require("ai_proxy.ai_proxy").body_filter() }
-- 必须配合变量化 proxy_pass（动态路由的关键）：
--   set $ai_backend ""; set $ai_request_uri "";
--   proxy_pass http://$ai_backend$ai_request_uri;   -- 生产换 https:// 并配 proxy_ssl_server_name on
--
-- 管理接口（写网关 token 映射 / 覆盖路由表）：见 admin.lua
-- 自检接口：_M.test()，可挂在任意 content_by_lua location

local cjson = require "cjson"

local _M = {}

-- ---------------------------------------------------------------------------
-- 默认路由表（生产域名；可用 admin 接口覆盖为内网/测试 mock 地址）
-- ---------------------------------------------------------------------------
local DEFAULT_ROUTES = {
    openai = {
        prefix  = "gpt",
        backend = "api.openai.com",
        uri     = "/v1/chat/completions",
        key     = "openai_sk",   -- secret_cfg 中对应密钥字段
    },
    glm = {
        prefix  = "glm",
        backend = "open.bigmodel.cn",
        uri     = "/api/paas/v4/chat/completions",
        key     = "glm_sk",
    },
    qwen = {
        prefix  = "qwen",
        backend = "dashscope.aliyuncs.com",
        uri     = "/compatible-mode/v1/chat/completions",
        key     = "qwen_sk",
    },
    ernie = {
        prefix  = "ernie",
        backend = "aip.baidubce.com",
        uri     = "/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/completions",
        key     = "ernie",        -- 文心用 AK/SK 换 access_token，不走固定 sk
        -- 文心 access_token 换取接口（AK/SK oauth）
        token_api = {
            backend = "aip.baidubce.com",
            port    = 443,
            ssl     = true,
            uri_prefix = "/oauth/2.0/token",
        },
    },
}

-- ---------------------------------------------------------------------------
-- 内部工具函数
-- ---------------------------------------------------------------------------

local function err_json(status, message, err_type, code)
    ngx.status = status
    ngx.header["Content-Type"] = "application/json"
    local e = { message = message, type = err_type or "invalid_request_error" }
    if code then e.code = code end
    ngx.say(cjson.encode({ error = e }))
    return ngx.exit(status)
end

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

-- 读取路由表：admin 覆盖 > 默认
local function get_routes()
    local dict = ngx.shared.ai_proxy_routes_dict
    if not dict then return DEFAULT_ROUTES end
    local raw = dict:get("routes")
    if not raw then return DEFAULT_ROUTES end
    local ok, routes = pcall(cjson.decode, raw)
    if not ok or type(routes) ~= "table" then return DEFAULT_ROUTES end
    return routes
end

-- 按 model 前缀精确路由
local function match_route(model)
    local routes = get_routes()
    for provider, r in pairs(routes) do
        local prefix = r.prefix or ""
        if prefix ~= "" and model:sub(1, #prefix) == prefix then
            return provider, r
        end
    end
    return nil
end

-- 模型白名单校验
local function model_allowed(allow_models, model)
    for _, m in ipairs(allow_models or {}) do
        if m == model then return true end
    end
    return false
end

-- ---------------------------------------------------------------------------
-- 阶段一：access 阶段 —— 统一鉴权 + 模型白名单 + 路由 + 请求协议转换
-- ---------------------------------------------------------------------------
function _M.access()
    -- 1. 解析客户端网关 Token（客户端只用这一套 token）
    local auth_header = ngx.var.http_authorization
    if not auth_header then
        return err_json(ngx.HTTP_UNAUTHORIZED, "missing Authorization header")
    end
    local gateway_token = string.match(auth_header, "^Bearer%s+(.+)$")
    if not gateway_token then
        return err_json(ngx.HTTP_UNAUTHORIZED, "invalid bearer format")
    end

    -- 2. 从共享内存读取该网关 token 绑定的后端密钥组
    local token_dict = ngx.shared.ai_gateway_token_dict
    if not token_dict then
        ngx.log(ngx.ERR, "ai_proxy: shared dict ai_gateway_token_dict 未定义，"
            .. "请在 http{} 添加 lua_shared_dict ai_gateway_token_dict 10m;")
        return err_json(ngx.HTTP_INTERNAL_SERVER_ERROR, "gateway internal error")
    end
    local raw_cfg = token_dict:get(gateway_token)
    if not raw_cfg then
        return err_json(ngx.HTTP_UNAUTHORIZED, "gateway token invalid")
    end
    local ok, cfg = pcall(cjson.decode, raw_cfg)
    if not ok or type(cfg) ~= "table" then
        ngx.log(ngx.ERR, "ai_proxy: 网关 token 映射数据损坏")
        return err_json(ngx.HTTP_INTERNAL_SERVER_ERROR, "gateway config corrupted")
    end
    -- 存入 ctx：后续 transform 脚本从这里拿各家真实密钥
    ngx.ctx.secret_cfg = cfg
    ngx.ctx.gateway_token = gateway_token

    -- 3. 读取并解析 OpenAI 格式请求体
    local raw_body = read_request_body()
    if not raw_body or raw_body == "" then
        return err_json(ngx.HTTP_BAD_REQUEST, "empty body")
    end
    local req
    ok = pcall(function() req = cjson.decode(raw_body) end)
    if not ok or type(req) ~= "table" then
        return err_json(ngx.HTTP_BAD_REQUEST, "json decode fail")
    end
    local model = req.model
    if type(model) ~= "string" or model == "" then
        return err_json(ngx.HTTP_BAD_REQUEST, "missing model")
    end

    -- 4. 模型白名单（前置权限校验）
    if not model_allowed(cfg.allow_models, model) then
        return err_json(ngx.HTTP_FORBIDDEN, "token not allowed access model: " .. model)
    end

    -- 5. 路由：根据 model 选择厂商
    local provider, route = match_route(model)
    if not provider then
        return err_json(ngx.HTTP_BAD_REQUEST, "unsupported model: " .. model)
    end

    -- 5.1 意图路由覆盖（AI-意图识别插件 ai_intent，位于本插件之前的 access
    --     链，识别结果写 ngx.ctx.intent_route）。两种覆盖形态：
    --       a) intent_route.model    指定意图目标大模型 -> 改写请求模型并
    --          重新路由（新模型同样受该 token 的 allow_models 白名单约束）
    --       b) intent_route.backend / uri 直接覆盖上游地址（同厂商不同实例
    --          分流，沿用原厂商协议转换与密钥配置）
    local intent_route = ngx.ctx.intent_route
    if type(intent_route) == "table" then
        if type(intent_route.model) == "string" and intent_route.model ~= "" then
            if not model_allowed(cfg.allow_models, intent_route.model) then
                return err_json(ngx.HTTP_FORBIDDEN,
                    "token not allowed access model: " .. intent_route.model)
            end
            local p2, r2 = match_route(intent_route.model)
            if p2 then
                provider, route = p2, r2
                req.model = intent_route.model
            else
                ngx.log(ngx.WARN, "ai_proxy: 意图路由模型无匹配厂商，回退原路由: ",
                    intent_route.model)
            end
        elseif type(intent_route.backend) == "string" and intent_route.backend ~= ""
                and type(route) == "table" then
            -- 必须新建表副本，不能原地改共享的默认路由表
            route = {
                backend = intent_route.backend,
                uri      = (type(intent_route.uri) == "string"
                            and intent_route.uri ~= "" and intent_route.uri
                            or route.uri),
                key      = route.key,
                prefix   = route.prefix,
                token_api = route.token_api,
            }
        end
    end

    -- 6. 请求协议转换（改 path / 替换上游凭证 / 字段映射）
    ngx.ctx.provider = provider
    ngx.ctx.openai_req = req
    local ok_tf, tf = pcall(require, "ai_proxy.transform." .. provider .. "_req")
    if not ok_tf then
        ngx.log(ngx.ERR, "ai_proxy: 加载转换器失败 ", tostring(tf))
        return err_json(ngx.HTTP_INTERNAL_SERVER_ERROR, "transform module load fail")
    end
    local ok_run, terr = pcall(tf.transform, req, route, cfg)
    if not ok_run then
        ngx.log(ngx.ERR, "ai_proxy: 请求转换失败 ", tostring(terr))
        return err_json(ngx.HTTP_INTERNAL_SERVER_ERROR, "transform fail: " .. tostring(terr))
    end

    -- 7. 设置动态上游变量（配合变量化 proxy_pass）
    --    不能写 ngx.var.proxy_pass，那是无效操作
    ngx.var.ai_backend = route.backend
    ngx.var.ai_request_uri = route.uri

    ngx.log(ngx.INFO, "ai_proxy: [", gateway_token, "] model=", model,
        " provider=", provider, " -> ", route.backend, route.uri,
        (type(ngx.ctx.ai_intent) == "string"
            and (" intent=" .. ngx.ctx.ai_intent) or ""))
end

-- ---------------------------------------------------------------------------
-- 阶段二：header_filter —— 响应体会被改写，清除 Content-Length 让 nginx 自动 chunked
-- ---------------------------------------------------------------------------
function _M.header_filter()
    if not ngx.ctx.provider then return end
    ngx.header["Content-Length"] = nil
    ngx.header["Accept-Ranges"] = nil
end

-- ---------------------------------------------------------------------------
-- 阶段三：body_filter —— 厂商私有响应 -> 标准 OpenAI 格式
-- ---------------------------------------------------------------------------
function _M.body_filter()
    local provider = ngx.ctx.provider
    if not provider then return end

    local ok_tf, tf = pcall(require, "ai_proxy.transform." .. provider .. "_resp")
    if not ok_tf then
        ngx.log(ngx.ERR, "ai_proxy: 加载响应转换器失败 ", tostring(tf))
        return
    end
    local ok_run, terr = pcall(tf.filter)
    if not ok_run then
        ngx.log(ngx.ERR, "ai_proxy: 响应转换失败 ", tostring(terr))
    end
end

-- ---------------------------------------------------------------------------
-- 自检接口：模仿 test 插件，验证插件环境是否就绪
-- ---------------------------------------------------------------------------
function _M.test()
    ngx.log(ngx.INFO, "=== ai_proxy 插件自检 ===")
    ngx.header["Content-Type"] = "application/json"

    local status = { code = 0, msg = "ai_proxy plugin ok", checks = {} }
    local checks = status.checks

    -- 1. 共享内存是否定义
    if ngx.shared.ai_gateway_token_dict then
        checks[#checks + 1] = { item = "shared_dict ai_gateway_token_dict", ok = true }
    else
        status.code = 1
        checks[#checks + 1] = { item = "shared_dict ai_gateway_token_dict", ok = false,
            hint = "http{} 添加 lua_shared_dict ai_gateway_token_dict 10m;" }
    end
    if ngx.shared.ai_ernie_token_dict then
        checks[#checks + 1] = { item = "shared_dict ai_ernie_token_dict", ok = true }
    else
        status.code = 1
        checks[#checks + 1] = { item = "shared_dict ai_ernie_token_dict", ok = false,
            hint = "http{} 添加 lua_shared_dict ai_ernie_token_dict 1m;" }
    end

    -- 2. 动态路由变量是否声明
    if ngx.var.ai_backend ~= nil then
        checks[#checks + 1] = { item = "nginx var $ai_backend", ok = true }
    else
        status.code = 1
        checks[#checks + 1] = { item = "nginx var $ai_backend", ok = false,
            hint = "location 内添加 set $ai_backend \"\"; set $ai_request_uri \"\";" }
    end

    -- 3. 转换器模块是否可加载
    for _, p in ipairs({ "openai", "glm", "qwen", "ernie" }) do
        local ok_req, _ = pcall(require, "ai_proxy.transform." .. p .. "_req")
        local ok_resp, _ = pcall(require, "ai_proxy.transform." .. p .. "_resp")
        checks[#checks + 1] = { item = "transform/" .. p .. " (req/resp)", ok = ok_req and ok_resp }
        if not (ok_req and ok_resp) then status.code = 1 end
    end

    -- 4. 网关 token 映射数量
    local n = 0
    if ngx.shared.ai_gateway_token_dict then
        local keys = ngx.shared.ai_gateway_token_dict:get_keys(0)
        n = keys and #keys or 0
    end
    checks[#checks + 1] = { item = "已配置网关 token 数", ok = true, detail = n }

    ngx.say(cjson.encode(status))
end

return _M
