#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# AI-proxy 插件测试用 mock 上游：模拟 4 家大模型厂商 + 百度 oauth 接口
# 用途：本地验证网关的鉴权替换、协议转换、SSE 粘包处理，无需真实密钥
# 用法：python3 test_tools/mock_upstream.py  （监听 127.0.0.1:19001）
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 19001
CREATED = 1766400000


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def _auth(self):
        return self.headers.get("Authorization", "NONE")

    def _read_body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n).decode("utf-8")) if n else {}

    def _send_json(self, obj, status=200):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_sse(self, events, with_done=True):
        """chunked 分块发送 SSE：把事件切成小片+打断事件模拟真实网络粘包/拆包"""
        payload = "".join(
            "data: " + json.dumps(e, ensure_ascii=False) + "\n\n" for e in events
        )
        if with_done:
            payload += "data: [DONE]\n\n"

        # 故意切成不规则小片（第 1 片切在事件中间），验证网关的粘包缓冲
        parts = []
        i, k = 0, 0
        while i < len(payload):
            k += 1
            size = min(len(payload) - i, 13 + (k % 7) * 11)
            parts.append(payload[i:i + size])
            i += size

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        for part in parts:
            raw = part.encode("utf-8")
            self.wfile.write(("%X\r\n" % len(raw)).encode() + raw + b"\r\n")
            self.wfile.flush()
            time.sleep(0.03)
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    # ------------------------------------------------------------------
    def do_POST(self):
        body = self._read_body()
        path = self.path.split("?")[0]
        stream = bool(body.get("stream"))
        model = body.get("model", "")
        print(f"[MOCK] POST {self.path} model={model} stream={stream} "
              f"auth={self._auth()}", flush=True)

        if path == "/v1/chat/completions":
            # ---- OpenAI mock：标准协议 ----

            # ===== 可观测插件测试场景（model 名携带特殊标记触发）=====
            # 1. 业务错误：HTTP 200 但响应内带 error 字段（biz_error 指标）
            if "biz_error" in model:
                if not stream:
                    return self._send_json(
                        {"error": {"message": "mock quota exceeded",
                                    "type": "quota_error", "code": "mock_quota"}})
                return self._send_sse(
                    [{"error": {"message": "mock quota exceeded",
                                 "type": "quota_error"}}])
            # 2. 流中断：正常吐几个事件后不发 [DONE] 直接结束（stream_break 指标）
            if "no_done" in model:
                return self._send_sse([
                    {"id": "chatcmpl-mock", "object": "chat.completion.chunk",
                     "created": CREATED, "model": model,
                     "choices": [{"index": 0, "finish_reason": None,
                                  "delta": {"role": "assistant",
                                            "content": "你好"}}]},
                    {"id": "chatcmpl-mock", "object": "chat.completion.chunk",
                     "created": CREATED, "model": model,
                     "choices": [{"index": 0, "finish_reason": None,
                                  "delta": {"content": "，网络断了"}}]},
                ], with_done=False)

            # ===== AI-历史对话插件测试场景 =====
            # 3. history 前缀模型：首个 delta.content 回显上游收到的 messages
            #    概况（条数 + 最后一条 user 内容），用于断言网关是否拼接历史
            if "history" in model:
                msgs = body.get("messages") or []
                last_user = "<none>"
                for m in reversed(msgs):
                    if isinstance(m, dict) and m.get("role") == "user":
                        last_user = m.get("content", "")
                        break
                roles = ",".join(m.get("role", "?") for m in msgs
                                 if isinstance(m, dict))
                first_content = (f"[hist_echo msgs={len(msgs)} roles={roles} "
                                 f"last_user={last_user}]")
                if not stream:
                    return self._send_json({
                        "id": "chatcmpl-hist", "object": "chat.completion",
                        "created": CREATED, "model": model,
                        "choices": [{"index": 0, "finish_reason": "stop",
                                     "message": {"role": "assistant",
                                                 "content": first_content}}],
                        "usage": {"total_tokens": 10}})
                return self._send_sse([
                    {"id": "chatcmpl-hist", "object": "chat.completion.chunk",
                     "created": CREATED, "model": model,
                     "choices": [{"index": 0, "finish_reason": None,
                                  "delta": {"role": "assistant",
                                            "content": first_content}}]},
                    {"id": "chatcmpl-hist", "object": "chat.completion.chunk",
                     "created": CREATED, "model": model,
                     "choices": [{"index": 0, "finish_reason": None,
                                  "delta": {"content": "，历史对话插件测试完成"}}]},
                    {"id": "chatcmpl-hist", "object": "chat.completion.chunk",
                     "created": CREATED, "model": model,
                     "choices": [{"index": 0, "finish_reason": "stop",
                                  "delta": {}}]},
                ])

            if not stream:
                return self._send_json({
                    "id": "chatcmpl-mock", "object": "chat.completion",
                    "created": CREATED, "model": model,
                    "choices": [{"index": 0, "finish_reason": "stop",
                                 "message": {"role": "assistant",
                                             "content": "OpenAI 非流式回答"}}],
                    "usage": {"total_tokens": 10}})
            events = [
                {"id": "chatcmpl-mock", "object": "chat.completion.chunk",
                 "created": CREATED, "model": model,
                 "choices": [{"index": 0, "finish_reason": None,
                              "delta": {"role": "assistant", "content": "你好"}}]},
                {"id": "chatcmpl-mock", "object": "chat.completion.chunk",
                 "created": CREATED, "model": model,
                 "choices": [{"index": 0, "finish_reason": None,
                              "delta": {"content": "，我是 OpenAI"}}]},
                {"id": "chatcmpl-mock", "object": "chat.completion.chunk",
                 "created": CREATED, "model": model,
                 "choices": [{"index": 0, "finish_reason": "stop",
                              "delta": {}}]},
            ]
            # OpenAI 流式默认不带 usage；只有带 stream_options.include_usage
            # 才在末尾补 usage chunk（choices 为空），限流插件靠它抓真实消耗。
            # model 名带 no_usage 标记时无视 include_usage（可观测插件的
            # token 估算兜底分支测试用）
            if (body.get("stream_options") or {}).get("include_usage") \
                    and "no_usage" not in model:
                events.append({
                    "id": "chatcmpl-mock", "object": "chat.completion.chunk",
                    "created": CREATED, "model": model,
                    "choices": [],
                    "usage": {"prompt_tokens": 7, "completion_tokens": 3,
                              "total_tokens": 10}})
            return self._send_sse(events)

        if path == "/api/paas/v4/chat/completions":
            # ---- GLM mock：老版本坑（object=chat.completion 非 chunk、tool_calls 缺 index）----
            if not stream:
                return self._send_json({
                    "id": "888abc", "object": "chat.completion",
                    "created": CREATED, "model": model,
                    "choices": [{"index": 0, "finish_reason": "stop",
                                 "message": {"role": "assistant",
                                             "content": "GLM 非流式回答",
                                             "tool_calls": [{"id": "call_glm01",
                                                             "type": "function",
                                                             "function": {"name": "get_weather",
                                                                          "arguments": "{\"city\":\"上海\"}"}}]}}],
                    "usage": {"total_tokens": 20}})
            # GLM 流式默认最后一包带 usage（choices 为空）
            return self._send_sse([
                {"id": "888abc", "object": "chat.completion", "created": CREATED,
                 "model": model,
                 "choices": [{"index": 0, "finish_reason": None,
                              "delta": {"role": "assistant"}}]},
                {"id": "888abc", "object": "chat.completion", "created": CREATED,
                 "model": model,
                 "choices": [{"index": 0, "finish_reason": None,
                              "delta": {"tool_calls": [{"id": "call_glm01",
                                                        "type": "function",
                                                        "function": {"name": "get_weather",
                                                                     "arguments": "{\"city\""}}]}}]},
                {"id": "888abc", "object": "chat.completion", "created": CREATED,
                 "model": model,
                 "choices": [{"index": 0, "finish_reason": None,
                              "delta": {"tool_calls": [{"type": "function",
                                                        "function": {"arguments": ":\"上海\"}"}}]}}]},
                {"id": "888abc", "object": "chat.completion", "created": CREATED,
                 "model": model,
                 "choices": [{"index": 0, "finish_reason": "stop",
                              "delta": {}}]},
                {"id": "888abc", "object": "chat.completion.chunk", "created": CREATED,
                 "model": model, "choices": [],
                 "usage": {"prompt_tokens": 12, "completion_tokens": 8,
                           "total_tokens": 20}},
            ])

        if path == "/compatible-mode/v1/chat/completions":
            # ---- 千问 mock：对齐 OpenAI 协议 ----
            if not stream:
                return self._send_json({
                    "id": "qwen-msg-001", "object": "chat.completion",
                    "created": CREATED, "model": model,
                    "choices": [{"index": 0, "finish_reason": "stop",
                                 "message": {"role": "assistant",
                                             "content": "千问非流式回答"}}]})
            return self._send_sse([
                {"id": "qwen-msg-001", "object": "chat.completion.chunk",
                 "created": CREATED, "model": model,
                 "choices": [{"index": 0, "finish_reason": None,
                              "delta": {"role": "assistant", "content": "你好"}}]},
                {"id": "qwen-msg-001", "object": "chat.completion.chunk",
                 "created": CREATED, "model": model,
                 "choices": [{"index": 0, "finish_reason": "stop",
                              "delta": {"content": "，我是千问"}}]},
            ])

        if path == "/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/completions":
            # ---- 文心 mock：百度私有结构（无 choices/delta、result 顶层、is_end 结束）----
            if not stream:
                return self._send_json({
                    "id": "as-ernie-mock", "object": "chat.completion",
                    "created": CREATED, "is_end": True,
                    "result": "文心非流式回答",
                    "function_call": {"name": "get_weather",
                                      "arguments": "{\"city\":\"上海\"}"},
                    "usage": {"total_tokens": 30}})
            return self._send_sse([
                {"id": "as-ernie-mock", "object": "chat.completion", "created": CREATED,
                 "sentence_id": 0, "is_end": False, "is_truncated": False,
                 "result": "", "need_clear_history": False,
                 "function_call": {"name": "get_weather", "thoughts": ""}},
                {"is_end": False, "result": "",
                 "function_call": {"arguments": "{\"city\":\"上海\"}"}},
                {"is_end": False, "result": "已调用工具查询上海天气",
                 "function_call": None},
                # 最后一包同时带 result 和 is_end（网关不能丢内容）
                {"is_end": True, "result": "上海今天晴，25 度",
                 "function_call": None, "usage": {"total_tokens": 30}},
            ], with_done=False)

        return self._send_json({"error": "mock: unknown path"}, 404)

    def do_GET(self):
        # ---- 百度 oauth mock：AK/SK 换 access_token ----
        if self.path.startswith("/oauth/2.0/token"):
            print(f"[MOCK] GET {self.path}", flush=True)
            return self._send_json({
                "access_token": "MOCK_ERNIE_ACCESS_TOKEN_123",
                "expires_in": 2592000,
                "refresh_token": "mock_refresh", "session_key": "mock_session",
            })
        return self._send_json({"error": "mock: unknown path"}, 404)


if __name__ == "__main__":
    print(f"[MOCK] 上游 mock 启动: 127.0.0.1:{PORT}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
