#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 提示词模板插件测试用 echo mock：把收到的完整请求体原样回显
# 用途：验证网关在 access 阶段注入的 system prompt 是否真正到达上游
# 用法：python3 test_tools/mock_echo_upstream.py  （监听 127.0.0.1:19002）
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 19002


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n).decode("utf-8")
        try:
            body = json.loads(raw)
        except Exception:
            body = {"raw": raw}

        print(f"[ECHO] POST {self.path}", flush=True)
        print(f"[ECHO] messages={json.dumps(body.get('messages'), ensure_ascii=False)}",
              flush=True)

        resp = {
            "id": "chatcmpl-echo",
            "object": "chat.completion",
            "model": body.get("model", ""),
            "received": body,            # 回显上游真正收到的 body
            "header_echo": dict(self.headers),
        }
        data = json.dumps(resp, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    print(f"[ECHO] echo mock 启动: 127.0.0.1:{PORT}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
