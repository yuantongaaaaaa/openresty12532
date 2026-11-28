#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AI-意图识别插件测试 mock

用法1（意图分类小模型）：
    python3 mock_intent_upstream.py intent 8001
  正常：按用户消息关键词返回 {"choices":[{"message":{"content":"<intent>"}}]}
      code/写代码/编程   -> code
      translate/翻译     -> translate
      summary/总结/摘要  -> summary
      query/查询/搜索    -> query
      其他               -> chat
  异常行为由请求头 X-Intent-Mode 控制（模拟小模型各类故障）：
      ok       -> 正常分类（默认）
      garbage  -> 输出不在白名单的乱串 "3.み permission granted！"
      empty    -> choices[0].message.content 置 null
      fail     -> HTTP 500
      slow     -> 休眠 15 秒（触发网关内部 location proxy_read_timeout 10s）
  响应头 X-Debug-Input-Len：上游收到的用户消息长度（验证截断生效）

用法2（意图路由目标 mock，模拟按意图区分的多个大模型上游）：
    python3 mock_intent_upstream.py target <PORT>
  端口-角色映射：8000=default 8002=code 8003=translate 8004=summary
  返回 content="answered by <ROLE> upstream"（含上游角色名，供 curl 断言
  请求确实被分流到对应意图上游）
"""
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROLES = {8000: "default", 8002: "code", 8003: "translate", 8004: "summary"}

INTENT_KEYWORDS = [
    ("code", ("code", "代码", "写代码", "编程", "函数", "bug", "python", "java")),
    ("translate", ("translate", "翻译")),
    ("summary", ("summary", "总结", "摘要", "概括")),
    ("query", ("query", "查询", "查一下", "搜索")),
]


def classify(text):
    low = text.lower()
    for intent, kws in INTENT_KEYWORDS:
        for kw in kws:
            if kw in low:
                return intent
    return "chat"


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n).decode("utf-8", "ignore")
        try:
            body = json.loads(raw)
        except Exception:
            body = {}

        if self.server.mode == "intent":
            self._handle_intent(body, raw)
        else:
            self._handle_target(body)

    def _handle_intent(self, body, raw):
        mode = self.headers.get("X-Intent-Mode") or "ok"
        print(f"[INTENT#{self.server.port}] mode={mode}", flush=True)
        print(f"[INTENT#{self.server.port}][DEBUG] auth={self.headers.get('Authorization')}",
              flush=True)
        if mode == "fail":
            self.send_response(500)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if mode == "slow":
            time.sleep(15)

        msgs = body.get("messages") or []
        text = ""
        for m in msgs:
            if isinstance(m, dict) and m.get("role") == "user":
                text = m.get("content") or ""
        if isinstance(text, list):  # 多模态 content 数组
            text = " ".join(p.get("text", "")
                            for p in text if isinstance(p, dict))

        if mode == "garbage":
            content = "3.み permission granted！"
        elif mode == "empty":
            content = None
        else:
            content = classify(str(text))

        resp = {
            "id": "chatcmpl-intent",
            "object": "chat.completion",
            "model": body.get("model", ""),
            "choices": [{"index": 0,
                         "message": {"role": "assistant", "content": content},
                         "finish_reason": "stop"}],
        }
        data = json.dumps(resp, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-Debug-Input-Len", str(len(text)))
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _handle_target(self, body):
        role = ROLES.get(self.server.port, "unknown")
        print(f"[TARGET#{self.server.port}] role={role} model={body.get('model')}",
              flush=True)
        # content 返回合法 JSON 字符串：兼容网关常开的 ai_json 自动校验
        answered_mark = json.dumps({"answered_by": role,
                                    "port": self.server.port},
                                   ensure_ascii=False)
        resp = {
            "id": "chatcmpl-target",
            "object": "chat.completion",
            "model": body.get("model", ""),
            "choices": [{"index": 0,
                         "message": {"role": "assistant",
                                     "content": answered_mark},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5,
                      "total_tokens": 15},
        }
        data = json.dumps(resp, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "intent"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8001
    srv = ThreadingHTTPServer(("127.0.0.1", port), H)
    srv.mode = mode
    srv.port = port
    print(f"[MOCK] {mode} mock 启动: 127.0.0.1:{port}", flush=True)
    srv.serve_forever()