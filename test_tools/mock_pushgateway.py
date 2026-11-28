#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# AI-可观测插件测试用 mock PushGateway
# 用途：接收网关推送的 Prometheus 指标，模拟 Prometheus 抓取行为
# 用法：python3 test_tools/mock_pushgateway.py  （监听 127.0.0.1:9091）
#
# 接口：
#   POST /metrics/job/llm_gateway 接收指标推送（与真实 PushGateway 路径一致）
#   GET  /metrics                 返回最近一次推送内容（模拟 Prometheus 抓取）
#   GET  /api/history             返回全部推送历史（次数、时间、body，测试断言用）
#   GET  /-/healthy               健康检查（插件自检接口探测连通性）
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 9091
history = []  # 每条：{seq, ts, path, body}


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n).decode("utf-8")
        entry = {
            "seq": len(history) + 1,
            "ts": time.strftime("%H:%M:%S"),
            "path": self.path,
            "body": body,
        }
        history.append(entry)
        print(f"[PUSHGW] POST {self.path}  body={len(body)}B  "
              f"total_pushes={len(history)}", flush=True)
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        # 必须带 Content-Length：HTTP/1.1 keep-alive 下客户端（resty.http）
        # 无法判断响应体结束，会一直等到超时
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if self.path == "/-/healthy":
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path == "/metrics":
            body = history[-1]["body"] if history else "# no metrics pushed yet\n"
        elif self.path == "/api/history":
            body = json.dumps(history, ensure_ascii=False, indent=2)
        else:
            self.send_response(404)
            self.end_headers()
            return
        data = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    print(f"[PUSHGW] mock PushGateway 启动: 127.0.0.1:{PORT}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
