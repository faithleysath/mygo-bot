"""Build MyGo-Bot and pack release artifacts for the requested Rust target."""

import argparse
import filecmp
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def build(target):
    env = os.environ.copy()
    if target.endswith("-apple-darwin") or "-linux-" in target:
        flag = "-C force-unwind-tables=no"
        target_var = f"CARGO_TARGET_{target.upper().replace('-', '_')}_RUSTFLAGS"
        if "CARGO_ENCODED_RUSTFLAGS" in env:
            items = [x for x in env["CARGO_ENCODED_RUSTFLAGS"].split("\x1f") if x]
            if "-Cforce-unwind-tables=no" not in items and "force-unwind-tables=no" not in items:
                items.extend(["-C", "force-unwind-tables=no"])
            env["CARGO_ENCODED_RUSTFLAGS"] = "\x1f".join(items)
        elif target_var in env:
            if "force-unwind-tables=" not in env[target_var]:
                env[target_var] = f"{env[target_var]} {flag}".strip()
        elif "RUSTFLAGS" in env:
            if "force-unwind-tables=" not in env["RUSTFLAGS"]:
                env["RUSTFLAGS"] = f"{env['RUSTFLAGS']} {flag}".strip()
        else:
            env[target_var] = flag
    command = [
        "cargo", "build", "--locked", "--release", "--bin", "mygo-bot",
        "--target", target, "--message-format=json-render-diagnostics",
    ]
    output = subprocess.check_output(command, text=True, encoding="utf-8", env=env)
    for line in output.splitlines():
        event = json.loads(line)
        if event["reason"] != "compiler-artifact" or not event["executable"]:
            continue
        if event["target"]["name"] == "mygo-bot":
            return Path(event["executable"])
    raise SystemExit("Cargo did not report the mygo-bot executable")


def linux_requirements(executable, artifact):
    # UPX rewrites ELF headers. Read the original target binary without executing it.
    headers = subprocess.check_output(
        [os.environ.get("OBJDUMP", "objdump"), "-p", executable],
        text=True, env={**os.environ, "LC_ALL": "C"},
    )
    libraries = [fields[1] for fields in map(str.split, headers.splitlines())
                 if len(fields) == 2 and fields[0] == "NEEDED"]
    requirements = {
        "artifact": artifact.name,
        "tls_backend": "OpenSSL (native-tls)",
        "needed_shared_libraries": libraries,
        "trusted_ca": {
            "source": "system trust store (ca-certificates)",
            "bundle_env": "SSL_CERT_FILE",
            "directory_env": "SSL_CERT_DIR",
        },
    }
    path = artifact.with_name(artifact.name + ".requirements.json")
    path.write_text(json.dumps(requirements, indent=2) + "\n", encoding="utf-8")
    return path


def release(target, host):
    mac = target.endswith("-apple-darwin")
    if not (mac or "-linux-" in target or "-windows-" in target):
        raise SystemExit(f"Unsupported release target: {target}")
    if mac and sys.platform != "darwin":
        raise SystemExit("Build macOS disk images on a Mac with hdiutil")
    if not shutil.which("upx"):
        raise SystemExit("Install UPX 5.3 or later on PATH to package a release")

    executable = build(target)
    dist = executable.parent / "dist"
    dist.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".pack-", dir=dist) as tmp:
        stage = Path(tmp)
        name = f"mygo-bot-{target}"
        packed = stage / (name + (".upx" if mac else executable.suffix))
        options = ["--force-macos"] if mac else []
        subprocess.run(["upx", "--best", *options, "-o", packed, executable], check=True)
        subprocess.run(["upx", "-t", packed], check=True)
        artifacts = [packed]
        if "-linux-" in target:
            artifacts.append(linux_requirements(executable, packed))
        runnable = packed
        if mac:
            # Current macOS can run the original signed Mach-O directly from a compressed image.
            volume = stage / "volume"
            volume.mkdir()
            runnable = volume / "mygo-bot"
            subprocess.run(["upx", "-d", "-o", runnable, packed], check=True)
            if not filecmp.cmp(executable, runnable, shallow=False):
                raise SystemExit("UPX did not restore the original Mach-O byte for byte")
            image = stage / (name + ".dmg")
            subprocess.run([
                "hdiutil", "create", "-quiet", "-fs", "HFS+", "-format", "ULMO",
                "-volname", "MyGo-Bot", "-srcfolder", volume, image,
            ], check=True)
            artifacts.append(image)
        if target == host:
            subprocess.run([runnable, "--version"], check=True, timeout=15)
        for artifact in artifacts:
            destination = dist / artifact.name
            artifact.replace(destination)
            print(f"{destination} ({destination.stat().st_size:,} bytes)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", help="Rust target triple (defaults to CARGO_BUILD_TARGET or the host)")
    args = parser.parse_args()
    version = subprocess.check_output(["rustc", "-vV"], text=True)
    host = next(line.split()[1] for line in version.splitlines() if line.startswith("host: "))
    release(args.target or os.environ.get("CARGO_BUILD_TARGET") or host, host)
