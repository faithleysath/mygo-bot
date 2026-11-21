"""Harbor 0.24 adapters for paired Responses trials in local Docker."""

import asyncio
import json
import os
import secrets
import shlex
from pathlib import Path

from harbor.agents.installed.base import BaseInstalledAgent
from harbor.agents.installed.pi import Pi
from harbor.environments.docker.docker import DockerEnvironment
from harbor.environments.docker.utils import ensure_docker_image_built

RUNTIMES = Path("/opt/frontier-runtime")


class TaskDocker(DockerEnvironment):
    async def _ensure_egress_control_sidecar_image_built(self):
        # Harbor 0.24 chooses the daemon architecture for this helper. Compose starts
        # every service as linux/amd64 here, including an isolated verifier's sidecar.
        self._env_vars.egress_control_sidecar_image_name = await ensure_docker_image_built(
            docker_name=self._EGRESS_CONTROL_SIDECAR_DOCKER_NAME,
            docker_build_context=self._EGRESS_CONTROL_SIDECAR_CONTEXT_PATH,
            dockerfile_path=self._egress_control_sidecar_dockerfile_path(),
            build_args={},
            platform="linux/amd64",
            logger=self.logger,
        )


def modalities():
    return ["text", "image"] if os.environ["FRONTIER_INPUT"] == "image" else ["text"]


async def unpack(agent, environment, archive, directory):
    remote = "/tmp/frontier-" + secrets.token_hex(8) + ".tgz"
    await environment.upload_file(RUNTIMES / archive, remote)
    try:
        await agent.exec_as_root(environment, f"chmod 644 {remote}")
        await agent.exec_as_agent(
            environment, f"mkdir -p {directory} && tar --no-same-owner -xzf {remote} -C {directory}"
        )
    finally:
        await agent.exec_as_root(environment, f"rm -f {remote}")


class StockPi(Pi):
    def _build_custom_models_json(self, access, model_id):
        config = super()._build_custom_models_json(access, model_id)
        model = config["providers"]["harbor-endpoint"]["models"][0]
        model["input"] = modalities()
        if os.environ.get("FRONTIER_CONTEXT"):
            model["contextWindow"] = int(os.environ["FRONTIER_CONTEXT"])
        return config

    async def install(self, environment):
        await self.ensure_system_dependencies(environment, ("curl",))
        # The upstream adapter sources ~/.nvm/nvm.sh. Preserve an existing task installation.
        existing = await environment.exec(command='test ! -e "$HOME/.nvm"')
        if existing.return_code:
            return await super().install(environment)
        await unpack(self, environment, "pi.tgz", '"$HOME/.nvm"')
        await self.exec_as_agent(environment, '. ~/.nvm/nvm.sh && test "$(pi --version)" = 1.4.0')


class MyGo(BaseInstalledAgent):
    @staticmethod
    def name():
        return "mygo-bot"

    async def install(self, environment):
        await self.ensure_system_dependencies(environment, ("curl",))
        self.state = "/tmp/mygo-frontier-" + secrets.token_hex(8)
        await self.exec_as_agent(environment, f"mkdir -m 700 {self.state}")
        await unpack(self, environment, "mygo.tgz", self.state)

    async def api(self, environment, method, path, body=None):
        command = (
            f"curl -fsS --max-time 30 --unix-socket {self.state}/api.sock "
            f"-X {method} {shlex.quote('http://localhost' + path)}"
        )
        if body is not None:
            await self._upload_config_text(
                environment, content=json.dumps(body), remote_path=f"{self.state}/request", filename="request"
            )
            command += f' -H "Content-Type: application/json" --data-binary @{self.state}/request'
        result = await self.exec_as_agent(environment, command)
        return json.loads(result.stdout)

    async def run(self, instruction, environment, context):
        s = self.state
        logs = shlex.quote(str(self.environment_logs_dir))
        # Keep the task's original working directory for tool execution. SQLite resolves
        # the private symlink for its WAL too. Never replace a task-owned file.
        start = f"""set -eu
mkdir -p {logs}
test ! -e mygo-bot.sqlite3 && test ! -L mygo-bot.sqlite3 || exit 1
ln -s {s}/state.db mygo-bot.sqlite3
nohup {s}/lib/ld-linux-x86-64.so.2 --library-path {s}/lib {s}/mygo-bot --daemon --foreground --unix-socket {s}/api.sock --token "$MYGO_LOCAL_TOKEN" > {logs}/daemon.log 2>&1 </dev/null &
echo $! > {s}/pid
for i in $(seq 100); do test ! -S {s}/api.sock || exit 0; kill -0 $! || exit 1; sleep 0.1; done
exit 1"""
        sid = None
        try:
            await self.exec_as_agent(environment, start, env={"MYGO_LOCAL_TOKEN": secrets.token_hex(24)})
            provider = await self.api(
                environment,
                "POST",
                "/providers",
                {
                    "name": "Frontier",
                    "base_url": os.environ["OPENAI_BASE_URL"],
                    "api_key": os.environ["OPENAI_API_KEY"],
                },
            )
            effort = os.environ["FRONTIER_EFFORT"]
            model = await self.api(
                environment,
                "POST",
                "/models",
                {
                    "provider_id": provider["id"],
                    "model_key": self.model_name.split("/", 1)[1],
                    "context_window_tokens": int(os.environ["FRONTIER_CONTEXT"])
                    if os.environ.get("FRONTIER_CONTEXT")
                    else None,
                    "input_modalities": modalities(),
                    "reasoning_efforts": [effort],
                },
            )
            session = await self.api(
                environment, "POST", "/sessions", {"model_id": model["id"], "reasoning_effort": effort}
            )
            sid = session["id"]
            await self.api(
                environment,
                "POST",
                f"/sessions/{sid}/messages",
                {"content": [{"type": "text", "text": instruction}]},
            )
            await self.api(environment, "PUT", f"/sessions/{sid}/run")
            await self.follow(environment, sid)
        finally:
            try:
                if sid is not None:
                    await self.save_calls(environment, sid, context)
            finally:
                await environment.exec(
                    command=f"""if test -f {s}/pid; then
  pid=$(cat {s}/pid)
  kill -TERM "$pid" 2>/dev/null || true
  for i in $(seq 100); do
    test -r /proc/$pid/stat && test "$(awk '{{print $3}}' /proc/$pid/stat)" != Z || break
    sleep 0.1
  done
fi
if test "$(readlink mygo-bot.sqlite3)" = {s}/state.db; then rm -f mygo-bot.sqlite3; fi
rm -rf {s}""",
                )

    async def follow(self, environment, sid):
        path = f"/sessions/{sid}"
        after = 0
        outstanding = set()
        with (self.logs_dir / "items.jsonl").open("w", buffering=1) as log:
            while True:
                state = await self.api(environment, "GET", path)
                items = await self.api(environment, "GET", f"{path}/history?after={after}&limit=100")
                for item in items:
                    print(json.dumps(item, ensure_ascii=False), file=log)
                    after = item["id"]
                    payload = item["payload"]
                    if item["kind"] == "function_call_output" and isinstance(payload["output"], str):
                        try:
                            receipt = json.loads(payload["output"])
                        except ValueError:
                            continue
                        if isinstance(receipt, dict) and receipt.get("delivery") == "background":
                            outstanding.add(receipt["id"])
                    if item["kind"] == "codemode_event" and payload["event"] == "completed":
                        outstanding.discard(payload["task_id"])
                if items:
                    continue
                if state["status"] == "paused":
                    raise RuntimeError("MyGo paused: " + state["last_error"])
                if state["status"] == "idle" and not outstanding:
                    # A completion may be committed before its asynchronous wake. PUT
                    # consumes pending events atomically, or reports that work is finished.
                    settled = await self.api(environment, "PUT", path + "/run")
                    if settled["status"] == "idle" and not await self.api(
                        environment, "GET", f"{path}/history?after={after}&limit=1"
                    ):
                        return
                await asyncio.sleep(0.5)

    async def save_calls(self, environment, sid, context):
        after = 0
        with (self.logs_dir / "llm_calls.jsonl").open("w") as log:
            while calls := await self.api(
                environment, "GET", f"/calls?session_id={sid}&after={after}&limit=100"
            ):
                for call in calls:
                    after = call["id"]
                    print(json.dumps(call, ensure_ascii=False), file=log)
                    if call.get("output_tokens") is not None:
                        cached = call["cache_read_tokens"] or 0
                        context.n_input_tokens = (
                            (context.n_input_tokens or 0)
                            + cached
                            + (call["cache_write_tokens"] or 0)
                            + (call["input_uncached_tokens"] or 0)
                        )
                        context.n_cache_tokens = (context.n_cache_tokens or 0) + cached
                        context.n_output_tokens = (context.n_output_tokens or 0) + call["output_tokens"]
