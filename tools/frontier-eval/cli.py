"""Run explicitly selected FrontierHarness v1.1.0 tasks in paired Docker trials."""

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from compare import report

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
DATASET = "frontier-harness-eval/frontier-harness-eval@v1.1.0"
CONTROLLER = "mygo-frontier-controller:v1.1.0"
with (HERE / "tasks.tsv").open() as source:
    TASKS = {t["name"]: t for t in csv.DictReader(source, delimiter="\t")}


def capture(*cmd):
    return subprocess.check_output(cmd, text=True).strip()


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("需要正整数")
    return number


def pin_image(task):
    tag, digest = task["image"], task["image_digest"]
    if "@" in tag:  # Already pinned in the task definition. Harbor pulls it as needed.
        return
    probe = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{json .RepoDigests}}", tag],
        capture_output=True,
        text=True,
        check=False,
    )
    if not probe.returncode:
        if not any(ref.endswith("@" + digest) for ref in json.loads(probe.stdout) or []):
            raise ValueError(f"{tag} 的本地摘要与本次题库不一致，请检查该标签")
        return
    ref = tag.rsplit(":", 1)[0] + "@" + digest
    subprocess.run(["docker", "pull", "--platform=linux/amd64", ref], check=True)
    subprocess.run(["docker", "tag", ref, tag], check=True)


def socket_path():
    host = os.environ.get("DOCKER_HOST") or capture(
        "docker", "context", "inspect", "--format", "{{ .Endpoints.docker.Host }}"
    )
    if not host.startswith("unix://"):
        raise ValueError("本地评测需要 Docker Unix socket，当前 endpoint 为 " + host.split(":")[0])
    return host.removeprefix("unix://")


def endpoint(url):
    u = urlsplit(url)
    if (
        u.scheme not in ("http", "https")
        or not u.hostname
        or u.username
        or u.password
        or u.query
        or u.fragment
    ):
        raise ValueError("模型地址须为 http(s) base URL，可用环境变量 OPENAI_API_KEY 传入密钥")
    if u.hostname not in ("127.0.0.1", "localhost", "::1"):
        return url.rstrip("/")
    host = "host.docker.internal"
    if sys.platform.startswith("linux"):
        host = capture(
            "docker", "network", "inspect", "bridge", "--format", "{{(index .IPAM.Config 0).Gateway}}"
        )
    return urlunsplit((u.scheme, host + (f":{u.port}" if u.port else ""), u.path.rstrip("/"), "", ""))


def api_key(args):
    if key := os.environ.get("OPENAI_API_KEY"):
        return key
    if args.base_url.rstrip("/") in (
        "http://127.0.0.1:3425/v1",
        "http://localhost:3425/v1",
        "http://host.docker.internal:3425/v1",
    ):
        raw = capture(
            "magpie",
            "gateway-key",
            "issue",
            "--ttl",
            "168h",
            "--name",
            "mygo-frontier-eval",
            "--models",
            args.model,
            "--json",
        )
        data = json.loads(raw)
        for field in ("key", "token", "value", "secret"):
            if isinstance(data.get(field), str) and data[field]:
                return data[field]
    raise ValueError("请为选定模型设置 OPENAI_API_KEY")


def build(runtime):
    subprocess.run(
        ["docker", "buildx", "build", "--load", "-t", CONTROLLER, "-"],
        input=(HERE / "Dockerfile").read_bytes(),
        check=True,
    )
    subprocess.run(
        [
            "docker",
            "buildx",
            "build",
            "--platform=linux/amd64",
            "--output",
            f"type=local,dest={runtime}",
            "-",
        ],
        input=(HERE / "pi.Dockerfile").read_bytes(),
        check=True,
    )
    cache = ROOT / "target/frontier-eval/build"
    cache.mkdir(parents=True, exist_ok=True)
    user = ["--user", f"{os.getuid()}:{os.getgid()}"] if sys.platform.startswith("linux") else []
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--platform=linux/amd64",
            *user,
            "-w",
            "/src",
            "-v",
            f"{ROOT}:/src:ro",
            "-v",
            f"{cache}:/build",
            "-v",
            f"{runtime}:/out",
            "-e",
            "CARGO_HOME=/build/cargo",
            "-e",
            "CARGO_TARGET_DIR=/build/target",
            "rust:1.100.0-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea",
            "sh",
            "tools/frontier-eval/pack-mygo.sh",
        ],
        check=True,
    )


def stop_round(container, job_dir):
    subprocess.run(["docker", "stop", "--time", "20", container], capture_output=True, check=False)
    # The trials are siblings of the controller. A forced controller exit can leave
    # them running. Match only the Compose projects belonging to this round's trials.
    trials = {p.parent.name.lower() for p in job_dir.glob("*/config.json")}
    projects = capture("docker", "ps", "--format", '{{.ID}} {{.Label "com.docker.compose.project"}}')
    for line in projects.splitlines():
        cid, _, project = line.partition(" ")
        if any(project == t + "__env" or project.startswith(t + "__verifier__") for t in trials):
            subprocess.run(["docker", "stop", "--time", "5", cid], check=False)


def run(args):
    names = list(dict.fromkeys(args.tasks))
    unknown = set(names) - TASKS.keys()
    if unknown:
        raise ValueError("未收录的题目 " + ", ".join(sorted(unknown)) + "，可用 eval list 查题")
    sock = socket_path()
    url = endpoint(args.base_url)
    if args.output:
        dest = args.output.expanduser().resolve()
        dest.mkdir(parents=True, exist_ok=False, mode=0o700)
    else:
        base = ROOT / "target/frontier-eval/runs"
        base.mkdir(parents=True, exist_ok=True)
        dest = Path(tempfile.mkdtemp(prefix=datetime.now().strftime("%Y%m%d-%H%M%S-"), dir=base))
    print(f"结果目录 {dest}", flush=True)
    runtime = dest / "runtime"
    build(runtime)
    for name in names:
        pin_image(TASKS[name])
    key = api_key(args)
    env = dict(
        os.environ,
        OPENAI_API_KEY=key,
        OPENAI_BASE_URL=url,
        FRONTIER_EFFORT=args.effort,
        FRONTIER_INPUT="text" if args.text_only else "image",
        FRONTIER_CONTEXT=str(args.context_window or ""),
    )
    hashes = {p.stem: hashlib.sha256(p.read_bytes()).hexdigest() for p in runtime.glob("*.tgz")}
    manifest = {
        "dataset": DATASET,
        "tasks": names,
        "attempts": args.attempts,
        "concurrency": args.jobs,
        "model": args.model,
        "base_url": url,
        "effort": args.effort,
        "modalities": env["FRONTIER_INPUT"],
        "context_window": args.context_window,
        "pi": "1.4.0",
        "runtime_sha256": hashes,
        "git_commit": capture("git", "-C", str(ROOT), "rev-parse", "HEAD"),
        "working_tree_dirty": bool(capture("git", "-C", str(ROOT), "status", "--porcelain")),
    }
    (dest / "run.json").write_text(json.dumps(manifest, indent=2) + "\n")
    agents = [
        {"import_path": "frontier_agents:MyGo", "kwargs": {"version": hashes["mygo"][:12]}},
        {
            "import_path": "frontier_agents:StockPi",
            "kwargs": {"version": "1.4.0", "model_api": "openai-responses", "thinking": args.effort},
        },
    ]
    for agent in agents:
        agent.update(model_name="openai/" + args.model, override_setup_timeout_sec=1200)
    cmd = [
        "docker",
        "run",
        "--rm",
        "--init",
        "-i",
        "-v",
        f"{sock}:/var/run/docker.sock",
        "-v",
        f"{dest}:{dest}",
        "-w",
        str(dest),
        "-v",
        f"{HERE}:/opt/frontier-tool:ro",
        "-v",
        f"{runtime}:/opt/frontier-runtime:ro",
        "-e",
        f"HOME={dest}/.harbor",
        "-e",
        "PYTHONPATH=/opt/frontier-tool",
        "-e",
        "PYTHONDONTWRITEBYTECODE=1",
        "-e",
        "DOCKER_DEFAULT_PLATFORM=linux/amd64",
    ]
    if sys.platform.startswith("linux"):
        cmd += ["--user", f"{os.getuid()}:{os.getgid()}", "--group-add", str(os.stat(sock).st_gid)]
    for var in ("OPENAI_API_KEY", "OPENAI_BASE_URL", "FRONTIER_EFFORT", "FRONTIER_INPUT", "FRONTIER_CONTEXT"):
        cmd += ["-e", var]
    try:
        for attempt in range(1, args.attempts + 1):
            job = f"round-{attempt:03d}"
            config = {
                "job_name": job,
                "jobs_dir": str(dest / "jobs"),
                "n_concurrent_trials": args.jobs,
                "environment": {"import_path": "frontier_agents:TaskDocker", "override_storage_mb": 32768},
                "agents": agents,
                "tasks": [
                    {
                        "name": "terminal-bench/" + n,
                        "ref": TASKS[n]["package_digest"],
                        "source": DATASET.split("@")[0],
                    }
                    for n in names
                ],
            }
            cfg = dest / (job + ".json")
            cfg.write_text(json.dumps(config, indent=2) + "\n")
            container = "frontier-eval-" + uuid.uuid4().hex[:12]
            try:
                subprocess.run(
                    [*cmd, "--name", container, CONTROLLER, "run", "-c", str(cfg)], env=env, check=True
                )
            except (KeyboardInterrupt, subprocess.CalledProcessError):
                stop_round(container, dest / "jobs" / job)
                raise
    finally:
        summary = report(dest, dest / "comparison.html")
    return int(summary["unscored"] != 0)


def main():
    parser = argparse.ArgumentParser(
        prog="mise run eval",
        description="FrontierHarness v1.1.0 · MyGo 与原版 Pi 轨迹对照",
        epilog="示例：mise run eval run fix-git adaptive-rejection-sampler --attempts 2 --jobs 2",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("list", help="列出可选题目、CPU、内存和作答时限")
    listing.add_argument("match", nargs="?", default="", help="题名子串")
    runner = commands.add_parser(
        "run",
        help="在新容器中成对运行所选题目",
        description="需要 Docker Engine、Buildx 和容器可访问的 Responses API。默认使用本机 Magpie；未设置 OPENAI_API_KEY 时签发限当前模型的临时密钥。",
    )
    runner.add_argument("tasks", nargs="+", metavar="TASK")
    runner.add_argument("--attempts", type=positive, default=1, help="每个 harness 每题次数（默认 1）")
    runner.add_argument("--jobs", type=positive, default=2, help="同时运行的 trial 数（默认 2）")
    runner.add_argument("--model", default="auto/gpt-6.1-terra")
    runner.add_argument(
        "--effort", choices=["minimal", "low", "medium", "high", "xhigh", "max"], default="xhigh"
    )
    runner.add_argument("--base-url", default=os.getenv("OPENAI_BASE_URL") or "http://127.0.0.1:3425/v1")
    runner.add_argument("--context-window", type=positive, help="两个 harness 的模型上下文上限（token）")
    runner.add_argument("--text-only", action="store_true", help="两边均使用纯文本模型（默认支持图片）")
    runner.add_argument("--output", type=Path, help="新结果目录（默认 target/frontier-eval/runs 下）")
    compare = commands.add_parser("compare", help="从已有或中断的运行目录生成双栏 HTML")
    compare.add_argument("run_dir", type=Path)
    compare.add_argument("--task", help="只查看这一题")
    compare.add_argument("--attempt", type=positive)
    compare.add_argument("--output", type=Path, help="默认写入运行目录的 comparison.html")
    args = parser.parse_args()
    if args.command == "list":
        print("题目".ljust(37) + "vCPU   MiB   时限(分)")
        for name, t in TASKS.items():
            if args.match in name:
                print(f"{name:38} {t['cpu']:>2}  {t['memory_mb']:>4}  {int(t['timeout_sec']) // 60:>5}")
    elif args.command == "run":
        return run(args)
    else:
        dest = args.run_dir.expanduser().resolve()
        report(dest, args.output or dest / "comparison.html", args.task, args.attempt)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
    except KeyboardInterrupt:
        sys.exit(130)
