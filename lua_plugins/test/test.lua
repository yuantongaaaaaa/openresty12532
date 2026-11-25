-- ai_demo.lua
local _M = {}

-- 测试接口：返回JSON+打日志
function _M.test()
    -- 日志会直接出现在CLion控制台
    ngx.log(ngx.INFO, "=== test插件执行成功，请求进入 ===")

    -- 设置响应头+返回内容
    ngx.header["Content-Type"] = "application/json"
    ngx.say('{"code": 0, "msg": "OpenResty 1.25.3.2 插件运行正常"}')
    ngx.say("yuantong")
end

return _M
