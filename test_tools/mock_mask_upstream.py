#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# AI-数据脱敏插件测试用 mock 上游：
#   1) 非流式：回显收到的请求体（received 字段）——验证 access 阶段请求脱敏结果；
#      响应的 message.content 里带手机号/身份证——验证非流式响应脱敏。
#   2) 流式：SSE 返回 content 含手机号 13800138000 / 身份证 110101199001011234，
#      号码故意跨"帧"拆分，且整个 payload 切成不规则小片发送——同时验证
#      网关的 chunk 级粘包缓冲与跨帧 tail 拼接。
# 用法：python3 test_tools/mock_mask_upstream.py  （监听 127.0.0.1:19003）
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 19003
CREATED = 1766400000


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def _read_body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n).decode("utf-8")) if n else {}

    def do_POST(self):
        body = self._read_body()
        stream = bool(body.get("stream"))
        model = body.get("model", "")
        print(f"[MASK-MOCK] POST {self.path} model={model} stream={stream}",
              flush=True)
        print(f"[MASK-MOCK] received.messages="
              f"{json.dumps(body.get('messages'), ensure_ascii=False)}",
              flush=True)
        if stream:
            return self._send_sse(body)
        return self._send_json(body)

    # ---------------- 非流式：响应含敏感号码 + 回显请求体 ----------------
    def _send_json(self, body):
        data = json.dumps({
            "id": "chatcmpl-mask", "object": "chat.completion",
            "created": CREATED, "model": body.get("model", ""),
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant",
                                     "content": "已记录：手机号13800138000，"
                                                "身份证110101199001011234"}}],
            "received": body,          # 回显上游真正收到的请求体
        }, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    # ---------------- 流式：号码故意跨帧拆分 + 不规则小片发送 ----------------
    def _send_sse(self, body):
        model = body.get("model", "")
        events = [
            {"id": "chatcmpl-mask", "object": "chat.completion.chunk",
             "created": CREATED, "model": model,
             "choices": [{"index": 0, "finish_reason": None,
                          "delta": {"role": "assistant",
                                    "content": "您的手机号是138"}}]},
            {"id": "chatcmpl-mask", "object": "chat.completion.chunk",
             "created": CREATED, "model": model,
             "choices": [{"index": 0, "finish_reason": None,
                          "delta": {"content": "0013"}}]},
            {"id": "chatcmpl-mask", "object": "chat.completion.chunk",
             "created": CREATED, "model": model,
             "choices": [{"index": 0, "finish_reason": None,
                          "delta": {"content": "8000，身份证号1101011990010"}}]},
            {"id": "chatcmpl-mask", "object": "chat.completion.chunk",
             "created": CREATED, "model": model,
             "choices": [{"index": 0, "finish_reason": None,
                          "delta": {"content": "11234，请核对。"}}]},
            {"id": "chatcmpl-mask", "object": "chat.completion.chunk",
             "created": CREATED, "model": model,
             "choices": [{"index": 0, "finish_reason": "stop",
                          "delta": {}}]},
        ]
        payload = "".join("data: " + json.dumps(e, ensure_ascii=False) + "\n\n"
                          for e in events)
        payload += "data: [DONE]\n\n"

        # 故意切成不规则小片（第 1 片切在号码中间），验证网关的粘包缓冲
        parts = []
        i, k = 0, 0
        while i < len(payload):
            k += 1
            size = min(len(payload) - i, 7 + (k % 5) * 13)
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
            time.sleep(0.02)
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()


if __name__ == "__main__":
    print(f"[MASK-MOCK] mask mock 启动: 127.0.0.1:{PORT}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
