#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# vec_cache 插件测试用 mock 后端：模拟 TEI embedding 服务 + Milvus REST v2 API
# 用途：本机没有 docker 时，无需真实部署 Milvus/TEI 即可端到端验证
#       语义缓存插件的完整链路（embedding -> 检索 -> 命中/未命中 -> 写缓存）
# 用法：python3 test_tools/mock_vec_backend.py [--dim 768]
#   8081: POST /v1/embeddings         （TEI OpenAI 兼容接口）
#   9091: /v2/vectordb/**              （Milvus REST v2 子集，内存向量库）
#
# 向量算法：char-bigram 词袋 + L2 归一化。
#   语义相近的文本（共享词组）余弦相似度高，无关文本相似度低，
#   可真实驱动"语义缓存命中"逻辑，无需真实 embedding 模型。
import argparse
import hashlib
import json
import math
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DIM = 768
COLLECTION_NAME = "ai_chat_cache"

# 内存向量库：list of {id, vec, prompt, answer, consumer_key}
STORE = []
LOCK = threading.Lock()
SCHEMA = None  # 建表后置为 field 定义


def bigram_vec(text):
    """unigram(3x) + bigram(1x) 混合词袋 -> L2 归一化，维度 DIM。
    字符级重合占主导，轻微改写/同义变体的余弦相似度可达 0.9+，
    语义差异大的文本相似度低，可真实驱动"语义缓存命中"逻辑。"""
    vec = [0.0] * DIM
    chars = list(text.lower())
    for c in chars:
        vec[int(hashlib.md5(c.encode("utf-8")).hexdigest(), 16) % DIM] += 3.0
    for i in range(len(chars) - 1):
        gram = chars[i] + chars[i + 1]
        vec[int(hashlib.md5(gram.encode("utf-8")).hexdigest(), 16) % DIM] += 1.0
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


def cosine(a, b):
    return sum(x * y for x, y in zip(a, b))


def json_reply(code, data=None, message=""):
    return {"code": code, "data": data if data is not None else {}, "message": message}


def ok(code, data=None, message=""):
    print(f"[VEC-MOCK] -> code={code} {message}", flush=True)
    return json_reply(code, data, message)


def read_json(handler):
    n = int(handler.headers.get("Content-Length") or 0)
    return json.loads(handler.rfile.read(n).decode("utf-8")) if n else {}


def send_json(handler, obj, status=200):
    data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


# ---------------------------------------------------------------------------
# TEI OpenAI 兼容接口：/v1/embeddings
# ---------------------------------------------------------------------------
class EmbedHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def do_POST(self):
        body = read_json(self)
        text = body.get("input", "")
        if isinstance(text, list):
            text = " ".join(str(t) for t in text)
        send_json(self, {
            "object": "list",
            "data": [{"object": "embedding", "index": 0, "embedding": bigram_vec(text)}],
            "model": "mock-bge",
            "usage": {"prompt_tokens": len(text), "total_tokens": len(text)},
        })
        print(f"[VEC-MOCK] embedding text={text[:30]!r}...", flush=True)


# ---------------------------------------------------------------------------
# Milvus REST v2 子集：/v2/vectordb/**
# ---------------------------------------------------------------------------
class MilvusHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def do_POST(self):
        global SCHEMA
        body = read_json(self)
        path = self.path.split("?")[0]
        print(f"[VEC-MOCK] POST {path} {str(body)[:120]}", flush=True)

        # ---------- collections ----------
        if path == "/v2/vectordb/collections/create":
            with LOCK:
                if SCHEMA is not None:
                    return send_json(self, ok(65535, message="collection already exist"))
                SCHEMA = body.get("schema", {})
                print(f"[VEC-MOCK] collection created: {COLLECTION_NAME}", flush=True)
            return send_json(self, ok(200))

        if path == "/v2/vectordb/collections/drop":
            with LOCK:
                if SCHEMA is None:
                    return send_json(self, ok(65535, message="collection not exist"))
                SCHEMA = None
                STORE.clear()
            return send_json(self, ok(200))

        if path == "/v2/vectordb/collections/describe":
            with LOCK:
                if SCHEMA is None:
                    return send_json(self, ok(65535, message="collection not exist"))
            return send_json(self, ok(200, {
                "collectionName": COLLECTION_NAME, "schema": SCHEMA}))

        # ---------- indexes ----------
        if path == "/v2/vectordb/indexes/create":
            with LOCK:
                if SCHEMA is None:
                    return send_json(self, ok(65535, message="collection not exist"))
            metric = (body.get("indexParams") or [{}])[0].get("metricType", "COSINE")
            return send_json(self, ok(200, {"metricType": metric}))

        # ---------- entities ----------
        if path == "/v2/vectordb/entities/insert":
            with LOCK:
                if SCHEMA is None:
                    return send_json(self, ok(65535, message="collection not exist"))
                rows = body.get("data", [])
                count = 0
                for r in rows:
                    if any(s["id"] == r.get("id") for s in STORE):
                        print(f"[VEC-MOCK] duplicate primary key: {r.get('id')}", flush=True)
                        continue
                    STORE.append(r)
                    count += 1
                if count == 0:
                    return send_json(self, ok(1100, message="primary key duplicate"))
            return send_json(self, ok(200, {"insertCount": count}))

        if path == "/v2/vectordb/entities/search":
            with LOCK:
                if SCHEMA is None:
                    return send_json(self, ok(65535, message="collection not exist"))
                q = body.get("data", [[]])[0]
                f = body.get("filter", "")
                # 解析 consumer_key == "xxx" 过滤表达式
                consumer = None
                if 'consumer_key == "' in f:
                    consumer = f.split('consumer_key == "', 1)[1].rsplit('"', 1)[0]
                limit = body.get("limit", 3)
                scored = []
                for s in STORE:
                    if consumer is not None and s.get("consumer_key") != consumer:
                        continue
                    scored.append((cosine(q, s["vec"]), s))
                scored.sort(key=lambda t: -t[0])
                hits = [{"id": s["id"], "distance": round(sc, 6),
                         "entity": {"prompt": s["prompt"], "answer": s["answer"]}}
                        for sc, s in scored[:limit]]
            return send_json(self, ok(200, hits))

        if path == "/v2/vectordb/entities/query":
            with LOCK:
                if SCHEMA is None:
                    return send_json(self, ok(65535, message="collection not exist"))
                limit = body.get("limit", 10)
                offset = body.get("offset", 0)
                f = body.get("filter", "")
                consumer = None
                if 'consumer_key == "' in f:
                    consumer = f.split('consumer_key == "', 1)[1].rsplit('"', 1)[0]
                rows = [{"id": s["id"], "prompt": s["prompt"],
                         "consumer_key": s["consumer_key"]} for s in STORE
                        if consumer is None or s.get("consumer_key") == consumer]
            return send_json(self, ok(200, rows[offset:offset + limit]))

        return send_json(self, json_reply(65535, message="unknown path"), 404)


def main():
    global DIM
    ap = argparse.ArgumentParser()
    ap.add_argument("--dim", type=int, default=DIM)
    ap.add_argument("--embed-port", type=int, default=8081)
    ap.add_argument("--milvus-port", type=int, default=9091)
    args = ap.parse_args()
    DIM = args.dim

    emb = ThreadingHTTPServer(("127.0.0.1", args.embed_port), EmbedHandler)
    mil = ThreadingHTTPServer(("127.0.0.1", args.milvus_port), MilvusHandler)
    print(f"[VEC-MOCK] TEI  mock: 127.0.0.1:{args.embed_port}/v1/embeddings (dim={DIM})",
          flush=True)
    print(f"[VEC-MOCK] Milvus mock: 127.0.0.1:{args.milvus_port}/v2/vectordb", flush=True)
    threading.Thread(target=emb.serve_forever, daemon=True).start()
    mil.serve_forever()


if __name__ == "__main__":
    main()
