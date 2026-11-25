#!/usr/bin/env python3
"""DeepSeek worker for agent-bus-mcp: claims role 'deepseek' tasks, answers via
the DSH-configured scnet DeepSeek provider, completes the task with the answer.

Configuration (nothing secret lives in this file):
  BUS_URL          bus MCP endpoint        (default http://127.0.0.1:8484/mcp)
  BUS_TOKEN_FILE   file holding the bus bearer token
                   (default: bus-token.txt next to this script; never committed)
  DSH_CREDENTIALS  DSH credentials YAML    (default ~/.dsh/.credentials.yaml);
                   the provider key is read from its refs section by name.
Requires: PyYAML.
"""
import json, os, time, urllib.request, sys
import yaml

BUS = os.environ.get("BUS_URL", "http://127.0.0.1:8484/mcp")
BUS_TOKEN = open(os.environ.get(
    "BUS_TOKEN_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "bus-token.txt"),
)).read().strip()
CREDS = yaml.safe_load(open(os.environ.get(
    "DSH_CREDENTIALS", os.path.expanduser("~/.dsh/.credentials.yaml"))))
DS_KEY = CREDS["refs"]["SHUGUANGCN_DEEP_API_KEY"]
DS_URL = "https://api.scnet.cn/api/llm/v1/chat/completions"
DS_MODEL = "DeepSeek-V4.1-Flash-1017-Event"
ROLE = "deepseek"
AGENT_ID = "deepseek-vps-1"

def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)

class Bus:
    def __init__(self):
        self.session = None
        self._id = 0
        self.rpc("initialize", {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "deepseek-worker", "version": "1.0"}})
        self.rpc("notifications/initialized", {}, notif=True)
    def rpc(self, method, params=None, notif=False):
        self._id += 1
        body = {"jsonrpc": "2.0", "method": method}
        if not notif: body["id"] = self._id
        if params is not None: body["params"] = params
        req = urllib.request.Request(BUS, data=json.dumps(body).encode(), method="POST")
        req.add_header("Authorization", f"Bearer {BUS_TOKEN}")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json, text/event-stream")
        if self.session: req.add_header("Mcp-Session-Id", self.session)
        with urllib.request.urlopen(req, timeout=30) as r:
            sid = r.headers.get("Mcp-Session-Id")
            if sid: self.session = sid
            txt = r.read().decode()
        for line in txt.splitlines():
            if line.startswith("data: "):
                return json.loads(line[6:])
        return None
    def call(self, name, args):
        res = self.rpc("tools/call", {"name": name, "arguments": args})
        if res and "result" in res and "structuredContent" in res["result"]:
            return res["result"]["structuredContent"]
        log("bus call error:", name, json.dumps(res)[:300])
        return None

def ask_deepseek(payload, title, max_tokens=4000):
    prompt = f"Task: {title}\n\n{payload}\n\nAnswer directly and completely. Be concise but thorough."
    body = {"model": DS_MODEL, "messages": [{"role": "user", "content": prompt}], "max_tokens": max_tokens, "stream": False}
    req = urllib.request.Request(DS_URL, data=json.dumps(body).encode(), method="POST")
    req.add_header("Authorization", f"Bearer {DS_KEY}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=180) as r:
        d = json.loads(r.read())
    return d["choices"][0]["message"]["content"].strip()

def main():
    log(f"worker up: role={ROLE} model={DS_MODEL}")
    while True:
        try:
            bus = Bus()
            got = bus.call("claim_next_task", {"role": ROLE, "agent_id": AGENT_ID})
            if got and got.get("found"):
                t = got["task"]
                log("claimed", t["id"], t["title"][:60])
                try:
                    answer = ask_deepseek(t["payload"], t["title"], 4000)
                    if not answer:
                        log("empty answer at 4000 tokens; retrying at 9000")
                        answer = ask_deepseek(t["payload"], t["title"], 9000)
                    if not answer:
                        log("empty answer at both budgets; failing the task honestly")
                        bus.call("complete_task", {"task_id": t["id"], "agent_id": AGENT_ID, "summary": "DeepSeek returned empty answers at 4000 and 9000 token budgets (reasoning consumed the budget); no content produced.", "failed": True})
                    else:
                        summary = answer[:280].replace("\n", " ")
                        out = bus.call("complete_task", {"task_id": t["id"], "agent_id": AGENT_ID, "summary": summary, "full_content": answer, "artifact_key": t.get("artifact_key", "")})
                        log("completed", out)
                except Exception as e:
                    log("task failed:", type(e).__name__, str(e)[:200])
                    bus.call("complete_task", {"task_id": t["id"], "agent_id": AGENT_ID, "summary": f"Worker error: {type(e).__name__}: {str(e)[:200]}", "failed": True})
            else:
                time.sleep(5)
        except Exception as e:
            log("loop error:", type(e).__name__, str(e)[:200])
            time.sleep(5)

if __name__ == "__main__":
    main()
