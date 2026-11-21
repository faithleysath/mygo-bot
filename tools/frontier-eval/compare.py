"""Read Harbor trial files offline. The report pairs attempts, never arbitrary turn numbers."""

import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from urllib.parse import quote


def read(path, *, partial=False):
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        if partial:  # An interrupted Harbor write can leave an empty or incomplete JSON file.
            return {}
        raise


def lines(path):
    if path.is_file():
        with path.open() as stream:
            for line in stream:
                try:
                    yield json.loads(line)
                except ValueError:  # Pi mixes CLI diagnostics with events; a stopped stream may end mid-line.
                    continue


def unpack(value):
    if isinstance(value, str) and value.startswith(("{", "[")):
        try:
            return json.loads(value)
        except ValueError:
            pass
    return value


def display(value):
    if isinstance(value, list) and all(isinstance(b, dict) and "type" in b for b in value):
        value = "\n".join(b.get("text") or f"[{b['type']}]" for b in value)
    if isinstance(value, dict) and set(value) == {"code"}:
        value = value["code"]
    if not isinstance(value, str):
        value = json.dumps(value, ensure_ascii=False, indent=2)
    value = re.sub(r"[A-Za-z0-9+/=]{256,}", "[编码数据]", value)
    return value if len(value) <= 12000 else value[:12000] + "\n…（完整内容见原始轨迹）"


def mygo_trace(agent):
    steps, by_call = [], {}
    instruction = ""
    for item in lines(agent / "items.jsonl"):
        payload = unpack(item["payload"])
        kind = item["kind"]
        if kind == "user_message":
            instruction = display(payload.get("content", ""))
        elif kind == "assistant_message":
            steps.append({"name": "回复", "text": display(payload.get("content", ""))})
        elif kind == "function_call":
            step = {
                "name": payload["name"],
                "args": display(unpack(payload["arguments"])),
                "call": item.get("llm_call_id"),
            }
            steps.append(step)
            by_call[payload["call_id"]] = step
        elif kind == "function_call_output" and payload["call_id"] in by_call:
            by_call[payload["call_id"]]["result"] = display(unpack(payload["output"]))
        elif kind == "codemode_event":
            steps.append(
                {
                    "name": f"后台 {payload['event']} · {payload['task_id']}",
                    "result": display(payload.get("content", "")),
                }
            )
    path = agent / "llm_calls.jsonl"
    calls = list(lines(path))
    billed = [c for c in calls if c.get("output_tokens") is not None]
    metrics = {
        "calls": len(calls) if path.exists() else None,
        "input": sum(
            sum(c.get(k) or 0 for k in ("input_uncached_tokens", "cache_read_tokens", "cache_write_tokens"))
            for c in billed
        )
        if billed
        else None,
        "output": sum(c["output_tokens"] for c in billed) if billed else None,
    }
    requests = [
        {
            "id": c["id"],
            "status": c["status"],
            "seconds": round((c["finished_at"] - c["started_at"]) / 1000, 2)
            if c.get("finished_at") is not None
            else None,
            "output": c.get("output_tokens"),
            "cached": c.get("cache_read_tokens"),
            "input": sum(
                c.get(k) or 0 for k in ("input_uncached_tokens", "cache_read_tokens", "cache_write_tokens")
            )
            if c.get("input_uncached_tokens") is not None
            else None,
        }
        for c in calls
    ]
    return instruction, steps, metrics, None, requests


def pi_trace(agent):
    # The event stream also exists on partial trials, before Harbor writes its ATIF trajectory.
    steps, by_call, messages, requests = [], {}, [], []
    instruction, error = "", None
    for event in lines(agent / "pi.txt"):
        kind = event.get("type")
        msg = event.get("message") or {}
        if kind == "message_end" and msg.get("role") == "user":
            instruction = display(msg.get("content", ""))
        elif kind == "message_end" and msg.get("role") == "assistant":
            usage = msg.get("usage") or {}
            messages.append(usage)
            requests.append(
                {
                    "id": len(messages),
                    "status": msg.get("stopReason"),
                    "seconds": round(msg["durationMs"] / 1000, 2)
                    if msg.get("durationMs") is not None
                    else None,
                    "input": sum(usage.get(k) or 0 for k in ("input", "cacheRead", "cacheWrite"))
                    if usage
                    else None,
                    "cached": usage.get("cacheRead"),
                    "output": usage.get("output"),
                }
            )
            if msg.get("stopReason") in ("error", "aborted"):
                error = msg.get("errorMessage") or msg["stopReason"]
            else:
                error = None
            for block in msg.get("content", []):
                if block.get("type") == "text" and block.get("text"):
                    steps.append({"name": "回复", "text": display(block["text"])})
        elif kind == "tool_execution_start":
            step = {"name": event["toolName"], "args": display(event.get("args", {})), "call": len(messages)}
            by_call[event["toolCallId"]] = step
            steps.append(step)
        elif kind == "tool_execution_end" and event["toolCallId"] in by_call:
            step = by_call[event["toolCallId"]]
            step["result"] = display(event.get("result", {}).get("content", ""))
            if event.get("durationMs") is not None:
                step["seconds"] = round(event["durationMs"] / 1000, 2)
    billed = [u for u in messages if u.get("output") is not None]
    return (
        instruction,
        steps,
        {
            "calls": len(messages) if messages else None,
            "input": sum(sum(u.get(k) or 0 for k in ("input", "cacheRead", "cacheWrite")) for u in billed)
            if billed
            else None,
            "output": sum(u["output"] for u in billed) if billed else None,
        },
        error,
        requests,
    )


def duration(result, key):
    timing = result.get(key) or {}
    if timing.get("started_at") and timing.get("finished_at"):
        return round(
            (
                datetime.fromisoformat(timing["finished_at"]) - datetime.fromisoformat(timing["started_at"])
            ).total_seconds(),
            1,
        )


def trial(path, harness, output):
    result = read(path / "result.json", partial=True)
    agent = path / "agent"
    prompt, steps, metrics, error, requests = (pi_trace if harness == "pi" else mygo_trace)(agent)
    score = ((result.get("verifier_result") or {}).get("rewards") or {}).get("reward")
    exception = result.get("exception_info") or {}
    if exception and not (score is not None and exception.get("exception_type") == "AgentTimeoutError"):
        error = exception.get("exception_type", "运行异常")
    testlog = path / "verifier/test-stdout.txt"
    if score == 0 and not error and testlog.is_file():
        text = testlog.read_text(errors="replace")
        if not re.search(r"collected [1-9]\d* item|\b[1-9]\d* (?:passed|failed)\b", text) and re.search(
            r"failed to download|uvx: command not found|Could not resolve host|SSL_ERROR_SYSCALL",
            text,
            re.IGNORECASE,
        ):
            error = "判分依赖下载失败"
    metrics.update(
        setup=duration(result, "agent_setup"),
        execution=duration(result, "agent_execution"),
        verifier=duration(result, "verifier"),
    )
    files = {
        "原始轨迹": agent / ("pi.txt" if harness == "pi" else "items.jsonl"),
        "判分日志": testlog,
        "场次结果": path / "result.json",
    }
    checks = read(path / "verifier/ctrf.json", partial=True).get("results", {})
    return prompt, {
        "version": (result.get("agent_info") or {}).get("version"),
        "failed_tests": [c["name"] for c in checks.get("tests", []) if c.get("status") == "failed"],
        "omitted_image": any(
            "image will be omitted from this request" in s.get("result", "").lower() for s in steps
        ),
        "reward": score if not error else None,
        "status": str(error) if error else ("已计分" if score is not None else "未完成"),
        "steps": steps,
        "requests": requests,
        "metrics": metrics,
        "files": {n: quote(os.path.relpath(f, output.parent)) for n, f in files.items() if f.exists()},
    }


def report(root, output, task=None, attempt=None):
    root, output = root.resolve(), output.expanduser().resolve()
    if output.suffix.lower() != ".html":
        raise ValueError("轨迹报告需要 .html 输出文件")
    manifest = read(root / "run.json")
    legacy = read(root / "progress.json")
    if not manifest and not legacy.get("results") and not legacy.get("interrupted"):
        raise ValueError("未找到评测 run.json 或已保存的 progress.json：" + str(root))
    pairs = {}

    def add(t, i, h, path=None):
        if (task and t != task) or (attempt and attempt != i):
            return
        pair = pairs.setdefault((t, i), {"task": t, "attempt": i, "prompt": "", "agents": {}})
        if path:
            prompt, data = trial(path, h, output)
            pair["agents"][h] = data
            if prompt:
                pair["prompt"] = prompt

    if manifest:
        for i in range(1, manifest["attempts"] + 1):
            for t in manifest["tasks"]:
                add(t, i, None)
            for f in sorted((root / "jobs" / f"round-{i:03d}").glob("*/config.json")):
                cfg = read(f, partial=True)
                if not cfg:
                    continue
                t = cfg["task"]["name"].split("/")[-1]
                h = "mygo" if cfg["agent"]["import_path"].endswith(":MyGo") else "pi"
                add(t, i, h, f.parent)
    else:
        # Old experiments assigned one Harbor job to each side. Only the accepted retry belongs in the pair.
        for row in legacy.get("results", []) + legacy.get("interrupted", []):
            job = root / "jobs" / row["job"]
            paths = list(job.glob("*/config.json"))
            add(row["task"], row["attempt"], row["harness"], paths[0].parent if paths else None)
    if not pairs:
        raise ValueError("没有匹配的题目和尝试序号")
    data = [pairs[key] for key in sorted(pairs)]
    scores = {h: [pair["agents"].get(h, {}).get("reward") for pair in data] for h in ("mygo", "pi")}
    meta = manifest or legacy
    payload = {"model": meta["model"], "effort": meta["effort"], "pairs": data}
    embedded = (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        .replace("<", r"\u003c")
        .replace("&", r"\u0026")
    )
    html = Path(__file__).with_name("comparison.html").read_text().replace("/*__RUN_DATA__*/", embedded)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output.parent, delete=False) as tmp:
        tmp.write(html)
    os.replace(tmp.name, output)
    for h, values in scores.items():
        print(f"{h}: {sum(v == 1 for v in values)}/{sum(v is not None for v in values)} 通过")
    print("轨迹对照", output, flush=True)
    return {"unscored": sum(v is None for values in scores.values() for v in values)}
