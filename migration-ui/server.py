#!/usr/bin/env python3
import base64
import json
import os
import queue
import shlex
import shutil
import socket
import subprocess
import tarfile
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parent
TOOLS_DIR = WORKSPACE / "dattobd-migrate-tools"
DATTOBD_DIR = WORKSPACE / "dattobd-inspect"
STATE_DIR = ROOT / "state"
JOBS_DIR = STATE_DIR / "jobs"
STATE = {
    "jobs": {},
}


def now():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def short_cmd(command, limit=700):
    compact = " ".join(str(command).split())
    if len(compact) <= limit:
        return compact
    return compact[:limit] + " ..."


def json_response(handler, data, status=200):
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def ensure_state_dirs():
    JOBS_DIR.mkdir(parents=True, exist_ok=True)


def read_body(handler):
    length = int(handler.headers.get("Content-Length", "0"))
    if length <= 0:
        return {}
    return json.loads(handler.rfile.read(length).decode("utf-8"))


def run_local(args, timeout=None, input_text=None):
    proc = subprocess.run(
        args,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )
    return proc.returncode, proc.stdout


def popen_lines(args):
    return subprocess.Popen(
        args,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
    )


def redact(value):
    if not value:
        return ""
    if len(value) <= 8:
        return "***"
    return value[:4] + "***" + value[-4:]


def q(value):
    return shlex.quote(str(value))


def normalize_choice(value, allowed, default):
    value = str(value or default).strip()
    return value if value in allowed else default


def normalize_int(value, default, min_value, max_value):
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = default
    return max(min_value, min(max_value, number))


def transport_options(cfg, data=None):
    data = data or {}
    compression = normalize_choice(
        data.get("compression") or cfg.get("compression"),
        {"none", "zlib", "auto"},
        "none",
    )
    decompression = normalize_choice(
        data.get("decompression") or cfg.get("decompression"),
        {"auto", "none"},
        "auto",
    )
    compression_level = normalize_int(
        data.get("compressionLevel") or cfg.get("compressionLevel"),
        1,
        1,
        9,
    )
    return compression, decompression, compression_level


def default_metrics():
    return {
        "bytes_total": 0,
        "bytes_done": 0,
        "wire_bytes_done": 0,
        "speed_bps": 0,
        "cow_size_current": 0,
        "nr_changed_blocks": 0,
        "ranges_count": 0,
        "ranges_preview": [],
        "last_transfer": {},
    }


class Endpoint:
    def __init__(self, cfg, tmpdir):
        self.host = cfg["host"].strip()
        self.user = cfg.get("user", "root").strip() or "root"
        self.port = int(cfg.get("port") or 22)
        self.key_text = cfg.get("key", "")
        self.password = cfg.get("password", "")
        self.tmpdir = Path(tmpdir)
        self.key_file = None
        if self.key_text.strip():
            stripped = self.key_text.strip()
            if stripped.startswith("ssh-") or "BEGIN OPENSSH PRIVATE KEY" not in stripped:
                raise ValueError(
                    f"{self.host} 的私钥字段不是私钥。请填 BEGIN OPENSSH PRIVATE KEY 开头的私钥，或留空使用本机默认 SSH key。"
                )
            self.key_file = self.tmpdir / ("key-" + self.host.replace(".", "_"))
            self.key_file.write_text(self.key_text.replace("\r\n", "\n"), encoding="utf-8")
            os.chmod(self.key_file, 0o600)

    def target(self):
        return f"{self.user}@{self.host}"

    def ssh_base(self):
        args = [
            "ssh",
            "-p", str(self.port),
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=10",
            "-o", "BatchMode=yes",
            "-o", "ServerAliveInterval=10",
            "-o", "ServerAliveCountMax=3",
        ]
        if self.key_file:
            args += ["-i", str(self.key_file)]
        return args

    def scp_base(self):
        args = [
            "scp",
            "-P", str(self.port),
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=10",
            "-o", "BatchMode=yes",
        ]
        if self.key_file:
            args += ["-i", str(self.key_file)]
        return args

    def ssh(self, command, timeout=None):
        return run_local(self.ssh_base() + [self.target(), command], timeout=timeout)

    def scp_to(self, local_path, remote_path, timeout=None):
        return run_local(self.scp_base() + [str(local_path), f"{self.target()}:{remote_path}"], timeout=timeout)


class MigrationJob:
    def __init__(self, cfg):
        self.id = uuid.uuid4().hex[:12]
        self.cfg = cfg
        self.created_at = now()
        self.updated_at = now()
        self.phase = "created"
        self.status = "running"
        self.error = None
        self.logs = []
        self.events = queue.Queue()
        self.worker = None
        self.metrics = default_metrics()
        self.transfer_base = 0
        self.transfer_wire_base = 0
        self.tmpdir_obj = tempfile.TemporaryDirectory(prefix="dattobd-ui-")
        self.tmpdir = Path(self.tmpdir_obj.name)
        self.source = Endpoint(cfg["source"], self.tmpdir)
        self.target = Endpoint(cfg["target"], self.tmpdir)
        self.source_dev = cfg["source"]["device"].strip()
        self.target_dev = cfg["target"]["device"].strip()
        self.minor = int(cfg.get("minor") or 0)
        self.port = int(cfg.get("socketPort") or 19090)
        self.remote_dir = cfg.get("remoteDir", "/root/dattobd-ui-work")
        self.compression, self.decompression, self.compression_level = transport_options(cfg)
        self.cfg["compression"] = self.compression
        self.cfg["decompression"] = self.decompression
        self.cfg["compressionLevel"] = self.compression_level
        self.cow_index = 0
        self.active_cow = f"/.datto-ui-{self.id}-0"
        self.previous_cow = None
        self.save()

    def log(self, msg):
        line = f"[{now()}] {msg}"
        self.logs.append(line)
        self.logs = self.logs[-2000:]
        self.updated_at = now()
        self.save()

    def save(self):
        try:
            ensure_state_dirs()
            path = JOBS_DIR / f"{self.id}.json"
            tmp = path.with_suffix(".tmp")
            data = self.snapshot(include_logs=True)
            tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(path)
        except Exception:
            pass

    def log_command_start(self, host, kind, command):
        self.log(f"{kind} START host={host} cmd={short_cmd(command)}")

    def log_command_end(self, host, kind, rc, elapsed, output=""):
        self.log(f"{kind} END host={host} rc={rc} elapsed={elapsed:.2f}s")
        if output:
            self.log(f"{kind} OUTPUT host={host}\n{output.strip()[-6000:]}")

    def fail(self, msg):
        self.status = "failed"
        self.error = msg
        self.phase = "failed"
        self.log("ERROR: " + msg)

    def snapshot(self, include_logs=True):
        return {
            "id": self.id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "phase": self.phase,
            "status": self.status,
            "error": self.error,
            "metrics": self.metrics,
            "logs": self.logs[-2000:] if include_logs else self.logs[-80:],
            "log_count": len(self.logs),
            "config": self.cfg,
            "minor": self.minor,
            "socketPort": self.port,
            "remoteDir": self.remote_dir,
            "compression": self.compression,
            "decompression": self.decompression,
            "compressionLevel": self.compression_level,
            "active_cow": self.active_cow,
            "previous_cow": self.previous_cow,
            "cow_index": self.cow_index,
            "source": {
                "host": self.source.host,
                "user": self.source.user,
                "device": self.source_dev,
            },
            "target": {
                "host": self.target.host,
                "user": self.target.user,
                "device": self.target_dev,
            },
        }

    @classmethod
    def from_saved(cls, data):
        obj = cls.__new__(cls)
        obj.id = data["id"]
        obj.cfg = data.get("config", {})
        obj.created_at = data.get("created_at", now())
        obj.updated_at = data.get("updated_at", now())
        obj.phase = data.get("phase", "loaded")
        obj.status = data.get("status", "loaded")
        if obj.status == "running":
            obj.status = "interrupted"
            obj.phase = "interrupted"
        obj.error = data.get("error")
        obj.logs = data.get("logs", [])
        obj.events = queue.Queue()
        obj.worker = None
        obj.metrics = default_metrics()
        obj.metrics.update(data.get("metrics", {}))
        obj.transfer_base = 0
        obj.transfer_wire_base = 0
        obj.tmpdir_obj = tempfile.TemporaryDirectory(prefix="dattobd-ui-")
        obj.tmpdir = Path(obj.tmpdir_obj.name)
        obj.source = Endpoint(obj.cfg["source"], obj.tmpdir)
        obj.target = Endpoint(obj.cfg["target"], obj.tmpdir)
        obj.source_dev = obj.cfg["source"]["device"].strip()
        obj.target_dev = obj.cfg["target"]["device"].strip()
        obj.minor = int(data.get("minor") or obj.cfg.get("minor") or 0)
        obj.port = int(data.get("socketPort") or obj.cfg.get("socketPort") or 19090)
        obj.remote_dir = data.get("remoteDir") or obj.cfg.get("remoteDir", "/root/dattobd-ui-work")
        obj.compression, obj.decompression, obj.compression_level = transport_options(obj.cfg, data)
        obj.cfg["compression"] = obj.compression
        obj.cfg["decompression"] = obj.decompression
        obj.cfg["compressionLevel"] = obj.compression_level
        obj.active_cow = data.get("active_cow") or f"/.datto-ui-{obj.id}-0"
        obj.previous_cow = data.get("previous_cow")
        obj.cow_index = int(data.get("cow_index") or 0)
        if data.get("status") == "running":
            obj.log("服务重启后恢复任务：原运行中任务已标记为 interrupted，可使用远端状态刷新查看残留进程。")
        return obj

    def refresh_remote_status(self):
        self.phase = "remote-refresh"
        self.log("刷新远端运行状态")
        cmd = (
            "echo ---DATTO---; cat /proc/datto-info 2>/dev/null || echo no-datto; "
            "echo ---PROCS---; pgrep -af '[b]lock_socket.py|[d]bdctl|[d]d if=/dev/datto' || true; "
            "echo ---RECEIVER_FULL---; tail -30 /root/dattobd-ui-work/receiver-full.log 2>/dev/null || true; "
            "echo ---RECEIVER_INC---; ls /root/dattobd-ui-work/receiver-inc-*.log 2>/dev/null | tail -1 | xargs -r tail -30; "
            "echo ---DISKSTATS---; cat /proc/diskstats | grep -E ' vdb | vdb1 ' || true; "
            f"echo ---TARGET_MOUNT---; findmnt -rn -S {self.target_dev} || true"
        )
        out = self.ssh_checked(self.source, cmd, timeout=30)
        self.parse_remote_status(out)
        if self.status in ("interrupted", "failed", "ready", "tracking"):
            if "block_socket.py send-full" in out or "block_socket.py receive" in out:
                self.status = "remote-running"
                self.phase = "remote-running"
            elif '"devices": [' in out and '"minor"' in out:
                self.status = "remote-snapshot"
                self.phase = "remote-snapshot"
        if self.status in ("adopted", "interrupted", "failed", "ready", "tracking", "remote-snapshot"):
            if "block_socket.py send-full" in out or "block_socket.py receive" in out:
                self.status = "remote-running"
                self.phase = "remote-running"
            elif '"state": 2' in out:
                self.status = "remote-tracking"
                self.phase = "remote-tracking"
            elif '"state": 3' in out:
                self.status = "remote-snapshot"
                self.phase = "remote-snapshot"
        self.save()

    def parse_remote_status(self, out):
        receiver_bytes = None
        receiver_wire_bytes = None
        for line in out.splitlines():
            if "received chunks=" in line and "bytes=" in line:
                for part in line.split():
                    if part.startswith("bytes="):
                        try:
                            receiver_bytes = int(part.split("=", 1)[1])
                        except ValueError:
                            pass
                    elif part.startswith("wire_bytes="):
                        try:
                            receiver_wire_bytes = int(part.split("=", 1)[1])
                        except ValueError:
                            pass
            if '"nr_changed_blocks":' in line:
                try:
                    self.metrics["nr_changed_blocks"] = int(line.split(":", 1)[1].strip().strip(","))
                except Exception:
                    pass
            if '"cow_size_current":' in line:
                try:
                    self.metrics["cow_size_current"] = int(line.split(":", 1)[1].strip().strip(","))
                except Exception:
                    pass
            if '"state":' in line:
                try:
                    self.metrics["dattobd_state"] = int(line.split(":", 1)[1].strip().strip(","))
                except Exception:
                    pass
        if receiver_bytes is not None:
            self.metrics["bytes_done"] = receiver_bytes
        if receiver_wire_bytes is not None:
            self.metrics["wire_bytes_done"] = receiver_wire_bytes

    def make_bundle(self):
        bundle = self.tmpdir / "dattobd-bundle.tar.gz"
        with tarfile.open(bundle, "w:gz") as tar:
            tar.add(TOOLS_DIR, arcname="dattobd-migrate-tools")
            tar.add(DATTOBD_DIR, arcname="dattobd-inspect")
        self.log(f"已生成本地工具包 {bundle} size={bundle.stat().st_size} bytes")
        return bundle

    def remote_env(self):
        return f"export LD_LIBRARY_PATH={self.remote_dir}/dattobd-inspect/lib:$LD_LIBRARY_PATH;"

    def ssh_checked(self, endpoint, command, timeout=None):
        self.log_command_start(endpoint.host, "SSH", command)
        started = time.time()
        rc, out = endpoint.ssh(command, timeout=timeout)
        self.log_command_end(endpoint.host, "SSH", rc, time.time() - started, out)
        if rc != 0:
            tail = out.strip()[-1200:] if out else ""
            detail = f"\n{tail}" if tail else ""
            raise RuntimeError(f"remote command failed on {endpoint.host}: {short_cmd(command)}{detail}")
        return out

    def ssh_stream(self, endpoint, command, on_line):
        self.log_command_start(endpoint.host, "SSH_STREAM", command)
        started = time.time()
        proc = popen_lines(endpoint.ssh_base() + [endpoint.target(), command])
        collected = []
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            collected.append(line)
            on_line(line)
        rc = proc.wait()
        out = "\n".join(collected)
        self.log_command_end(endpoint.host, "SSH_STREAM", rc, time.time() - started, out[-2000:])
        if rc != 0:
            raise RuntimeError(f"remote stream command failed on {endpoint.host}: {command}")
        return out

    def push_bundle(self):
        self.phase = "push"
        bundle = self.make_bundle()
        for ep, name in [(self.source, "source"), (self.target, "target")]:
            self.log(f"推送工具到 {name} {ep.host}")
            self.ssh_checked(ep, f"rm -rf {self.remote_dir}; mkdir -p {self.remote_dir}", timeout=30)
            self.log_command_start(ep.host, "SCP", f"{bundle} -> {self.remote_dir}/bundle.tar.gz")
            started = time.time()
            rc, out = ep.scp_to(bundle, f"{self.remote_dir}/bundle.tar.gz", timeout=120)
            self.log_command_end(ep.host, "SCP", rc, time.time() - started, out)
            if rc != 0:
                raise RuntimeError(f"scp bundle failed to {ep.host}")
            self.ssh_checked(ep, f"cd {self.remote_dir}; tar -xzf bundle.tar.gz; chmod +x dattobd-migrate-tools/*.py dattobd-migrate-tools/*.sh || true", timeout=60)

    def compile_remote(self):
        self.phase = "compile"
        self.log("在源端编译 dattobd 与 list-changed-blocks")
        self.log("编译步骤：CRLF 修正 -> 清理 kernel-config -> make dattobd -> 编译 list-changed-blocks")
        cmd = (
            f"cd {self.remote_dir}/dattobd-inspect && "
            "find . -type f -exec perl -pi -e 's/\\015\\012/\\012/g' {} + && "
            "find . -type f \\( -name '*.sh' -o -name 'genconfig.sh' \\) -exec chmod +x {} + && "
            "chmod +x src/genconfig.sh && "
            "rm -rf src/configure-tests/feature-tests/build src/kernel-config.h && "
            "make && "
            f"cd {self.remote_dir}/dattobd-migrate-tools && "
            "cc -std=gnu99 -O2 -Wall -Wextra -o list-changed-blocks list-changed-blocks.c && "
            "chmod +x list-changed-blocks block_socket.py"
        )
        self.ssh_checked(self.source, cmd, timeout=240)
        self.log("在目标端准备 socket 接收工具")
        self.ssh_checked(self.target, f"cd {self.remote_dir}/dattobd-migrate-tools && chmod +x block_socket.py", timeout=30)

    def load_module(self):
        self.phase = "snapshot"
        cmd = (
            f"cd {self.remote_dir}/dattobd-inspect; "
            "if ! lsmod | grep -q '^dattobd'; then insmod src/dattobd.ko; fi; "
            "cat /proc/datto-info"
        )
        self.log("加载 dattobd 内核模块并读取 /proc/datto-info")
        self.ssh_checked(self.source, cmd, timeout=180)

    def preflight_devices(self):
        self.phase = "preflight"
        self.log("执行迁移前设备检查")
        source_cmd = (
            f"set -e; dev={q(self.source_dev)}; "
            "test -b \"$dev\"; "
            "echo SIZE=$(blockdev --getsize64 \"$dev\"); "
            "echo TYPE=$(lsblk -ndo TYPE \"$dev\" 2>/dev/null || true); "
            "echo FSTYPE=$(lsblk -ndo FSTYPE \"$dev\" 2>/dev/null || true); "
            "mount_line=$(findmnt -rn -S \"$dev\" || true); "
            "if [ -n \"$mount_line\" ]; then echo MOUNTED=$mount_line; else echo MOUNTED=; fi"
        )
        target_cmd = (
            f"set -e; dev={q(self.target_dev)}; "
            "test -b \"$dev\"; "
            "echo SIZE=$(blockdev --getsize64 \"$dev\"); "
            "echo TYPE=$(lsblk -ndo TYPE \"$dev\" 2>/dev/null || true); "
            "if findmnt -rn -S \"$dev\" >/tmp/datto-target-mounted 2>/dev/null; then "
            "  echo TARGET_MOUNTED=$(cat /tmp/datto-target-mounted); exit 23; "
            "fi; "
            "mounted_children=$(lsblk -nrpo NAME,MOUNTPOINT \"$dev\" | awk 'NF > 1 {print $0}' || true); "
            "if [ -n \"$mounted_children\" ]; then echo TARGET_CHILD_MOUNTED=\"$mounted_children\"; exit 24; fi; "
            "echo TARGET_CLEAR=1"
        )
        src_out = self.ssh_checked(self.source, source_cmd, timeout=30)
        tgt_out = self.ssh_checked(self.target, target_cmd, timeout=30)
        if "MOUNTED=" in src_out and not any(line.startswith("MOUNTED=/") for line in src_out.splitlines()):
            raise RuntimeError(
                f"源端 {self.source_dev} 当前没有挂载文件系统，dattobd 不能对它创建快照。"
                "如果是分区盘，请选择实际挂载的分区，例如 /dev/vda1；目标端可以选择整盘，例如 /dev/vdb。"
            )
        try:
            src_size_line = next(line for line in src_out.splitlines() if line.startswith("SIZE="))
            tgt_size_line = next(line for line in tgt_out.splitlines() if line.startswith("SIZE="))
            self.metrics["source_size"] = int(src_size_line.split("=", 1)[1])
            self.metrics["target_size"] = int(tgt_size_line.split("=", 1)[1])
        except Exception:
            pass
        if self.metrics.get("source_size") and self.metrics.get("target_size"):
            if self.metrics["target_size"] < self.metrics["source_size"]:
                raise RuntimeError("目标设备小于源设备，不能进行块级复制")
            self.log(
                f"设备大小 source={self.metrics['source_size']} bytes "
                f"target={self.metrics['target_size']} bytes"
            )

    def preflight_devices(self):
        self.phase = "preflight"
        self.log("执行迁移前设备检查")
        source_cmd = (
            f"set -e; dev={q(self.source_dev)}; "
            "test -b \"$dev\"; "
            "echo SIZE=$(blockdev --getsize64 \"$dev\"); "
            "mount_point=$(lsblk -ndo MOUNTPOINT \"$dev\" 2>/dev/null | head -1); "
            "if [ -n \"$mount_point\" ]; then echo MOUNTED=$mount_point; else echo MOUNTED=; fi"
        )
        target_cmd = (
            f"set -e; dev={q(self.target_dev)}; "
            "test -b \"$dev\"; "
            "echo SIZE=$(blockdev --getsize64 \"$dev\"); "
            "if findmnt -rn -S \"$dev\" >/tmp/datto-target-mounted 2>/dev/null; then "
            "echo TARGET_MOUNTED=$(cat /tmp/datto-target-mounted); exit 23; "
            "fi; "
            "echo TARGET_CLEAR=1"
        )
        src_out = self.ssh_checked(self.source, source_cmd, timeout=12)
        tgt_out = self.ssh_checked(self.target, target_cmd, timeout=12)
        if "MOUNTED=" in src_out and not any(line.startswith("MOUNTED=/") for line in src_out.splitlines()):
            raise RuntimeError(
                f"源端 {self.source_dev} 当前没有挂载文件系统，dattobd 不能对它创建快照。"
                "请把源端改为实际挂载的分区，例如 /dev/vda1。目标端可以是整盘 /dev/vdb。"
            )
        try:
            src_size_line = next(line for line in src_out.splitlines() if line.startswith("SIZE="))
            tgt_size_line = next(line for line in tgt_out.splitlines() if line.startswith("SIZE="))
            self.metrics["source_size"] = int(src_size_line.split("=", 1)[1])
            self.metrics["target_size"] = int(tgt_size_line.split("=", 1)[1])
        except Exception:
            pass
        if self.metrics.get("source_size") and self.metrics.get("target_size"):
            if self.metrics["target_size"] < self.metrics["source_size"]:
                raise RuntimeError("目标设备小于源设备，不能进行块级复制")
            self.log(
                f"设备大小 source={self.metrics['source_size']} bytes "
                f"target={self.metrics['target_size']} bytes"
            )

    def reset_existing_snapshot(self):
        self.log(f"清理旧的 UI 迁移残留 minor={self.minor}")
        cmd = (
            f"{self.remote_env()} "
            f"cd {self.remote_dir}/dattobd-inspect; "
            f"app/dbdctl destroy {self.minor} || true; "
            "pkill -f '[b]lock_socket.py receive' || true; "
            "for f in /.datto-ui-*; do "
            "  [ -e \"$f\" ] || continue; "
            "  chattr -i \"$f\" 2>/dev/null || true; "
            "  chmod 600 \"$f\" 2>/dev/null || true; "
            "  rm -f \"$f\" || true; "
            "done; "
            "cat /proc/datto-info || true"
        )
        self.ssh_checked(self.source, cmd, timeout=60)

    def setup_snapshot(self):
        self.log(f"创建源端快照 {self.source_dev} -> /dev/datto{self.minor}")
        cmd = (
            f"{self.remote_env()} "
            f"cd {self.remote_dir}/dattobd-inspect; "
            f"app/dbdctl setup-snapshot {q(self.source_dev)} {q(self.active_cow)} {self.minor} || "
            "{ rc=$?; echo ---DMESG---; dmesg | tail -30; exit $rc; }; "
            "cat /proc/datto-info; "
            f"test -b /dev/datto{self.minor}"
        )
        out = self.ssh_checked(self.source, cmd, timeout=60)
        if "driver returned an error" in out.lower():
            raise RuntimeError("dattobd 创建快照失败，请看日志里的 DMESG。常见原因是源端不是已挂载的文件系统块设备。")

    def transition_incremental(self):
        self.log("切换到 incremental 模式继续追踪变化块")
        self.ssh_checked(
            self.source,
            f"{self.remote_env()} cd {self.remote_dir}/dattobd-inspect; app/dbdctl transition-to-incremental {self.minor}; cat /proc/datto-info",
            timeout=60,
        )

    def transition_snapshot(self):
        self.previous_cow = self.active_cow
        self.cow_index += 1
        self.active_cow = f"/.datto-ui-{self.id}-{self.cow_index}"
        self.log(f"切换回 snapshot，旧 COW={self.previous_cow}，新 COW={self.active_cow}")
        self.ssh_checked(
            self.source,
            f"{self.remote_env()} cd {self.remote_dir}/dattobd-inspect; app/dbdctl transition-to-snapshot {self.active_cow} {self.minor}; cat /proc/datto-info",
            timeout=60,
        )

    def start_receiver(self, label):
        log_path = f"{self.remote_dir}/receiver-{label}.log"
        pid_path = f"{self.remote_dir}/receiver-{label}.pid"
        cmd = (
            f"cd {self.remote_dir}/dattobd-migrate-tools; "
            f"rm -f {log_path} {pid_path}; "
            f"nohup ./block_socket.py receive --host 0.0.0.0 --port {self.port} --target {self.target_dev} "
            f"--decompression {q(self.decompression)} "
            f"> {log_path} 2>&1 & echo $! > {pid_path}; sleep 0.5; cat {pid_path}"
        )
        out = self.ssh_checked(self.target, cmd, timeout=20)
        self.log(f"目标端 socket receiver 已启动 pid={out.strip().splitlines()[-1] if out.strip() else '?'}")
        self.log(f"receiver log path: {log_path}")
        return log_path

    def send_full(self):
        self.phase = "full-copy"
        size_out = self.ssh_checked(
            self.source,
            f"blockdev --getsize64 /dev/datto{self.minor} 2>/dev/null || stat -c %s /dev/datto{self.minor}",
            timeout=20,
        )
        try:
            self.metrics["bytes_total"] = int(size_out.strip().splitlines()[-1])
        except Exception:
            self.metrics["bytes_total"] = 0
        self.log(f"全量源设备大小 /dev/datto{self.minor} = {self.metrics['bytes_total']} bytes")
        if self.metrics["bytes_total"] <= 0:
            raise RuntimeError(f"/dev/datto{self.minor} size is 0, abort full copy")
        self.start_receiver("full")
        self.log("开始 socket 全量传输")
        start = time.time()
        self.transfer_base = self.metrics["bytes_done"]
        self.transfer_wire_base = self.metrics.get("wire_bytes_done", 0)
        cmd = (
            f"cd {self.remote_dir}/dattobd-migrate-tools; "
            f"./block_socket.py send-full --host {self.target.host} --port {self.port} "
            f"--source /dev/datto{self.minor} --progress-interval 1 "
            f"--compression {q(self.compression)} --compression-level {self.compression_level}"
        )
        out = self.ssh_stream(self.source, cmd, self.handle_transfer_line)
        elapsed = max(time.time() - start, 0.001)
        transfer = self.parse_transfer_summary(out)
        bytes_sent = transfer["bytes"]
        wire_bytes_sent = transfer["wire_bytes"]
        if bytes_sent <= 0:
            raise RuntimeError("full transfer sent 0 bytes, aborting")
        self.metrics["last_transfer"] = {
            "type": "full",
            "bytes": bytes_sent,
            "wire_bytes": wire_bytes_sent,
            "elapsed": elapsed,
            "compression": self.compression,
        }
        self.metrics["bytes_done"] = self.transfer_base + bytes_sent
        self.metrics["wire_bytes_done"] = self.transfer_wire_base + wire_bytes_sent
        self.metrics["speed_bps"] = int(bytes_sent / elapsed)
        self.log(
            f"全量传输完成 bytes={bytes_sent} wire_bytes={wire_bytes_sent} "
            f"compression={self.compression} elapsed={elapsed:.2f}s "
            f"avg_speed={self.metrics['speed_bps']} B/s"
        )

    def list_ranges(self):
        self.phase = "list-ranges"
        ranges_path = f"{self.remote_dir}/ranges-{self.cow_index}.csv"
        meta_path = f"{self.remote_dir}/ranges-{self.cow_index}.meta"
        cmd = (
            f"cd {self.remote_dir}/dattobd-migrate-tools; "
            f"./list-changed-blocks --ranges {self.previous_cow} /dev/datto{self.minor} "
            f"> {ranges_path} 2> {meta_path}; "
            f"echo ---META---; cat {meta_path}; echo ---COUNT---; wc -l {ranges_path}; "
            f"echo ---PREVIEW---; head -30 {ranges_path}"
        )
        out = self.ssh_checked(self.source, cmd, timeout=60)
        self.extract_ranges_metrics(out)
        self.log(
            f"增量列表完成 ranges={self.metrics['ranges_count']} "
            f"changed_blocks={self.metrics['nr_changed_blocks']} path={ranges_path}"
        )
        return ranges_path

    def send_ranges(self, ranges_path):
        self.phase = "incremental-copy"
        self.start_receiver(f"inc-{self.cow_index}")
        self.log("开始 socket 增量传输")
        start = time.time()
        self.transfer_base = self.metrics["bytes_done"]
        self.transfer_wire_base = self.metrics.get("wire_bytes_done", 0)
        cmd = (
            f"cd {self.remote_dir}/dattobd-migrate-tools; "
            f"./block_socket.py send-ranges --host {self.target.host} --port {self.port} "
            f"--source /dev/datto{self.minor} --ranges {ranges_path} --progress-interval 1 "
            f"--compression {q(self.compression)} --compression-level {self.compression_level}"
        )
        out = self.ssh_stream(self.source, cmd, self.handle_transfer_line)
        elapsed = max(time.time() - start, 0.001)
        transfer = self.parse_transfer_summary(out)
        bytes_sent = transfer["bytes"]
        wire_bytes_sent = transfer["wire_bytes"]
        if self.metrics["nr_changed_blocks"] and bytes_sent <= 0:
            raise RuntimeError("incremental transfer sent 0 bytes while changed blocks exist")
        self.metrics["last_transfer"] = {
            "type": "incremental",
            "bytes": bytes_sent,
            "wire_bytes": wire_bytes_sent,
            "elapsed": elapsed,
            "compression": self.compression,
        }
        self.metrics["bytes_done"] = self.transfer_base + bytes_sent
        self.metrics["wire_bytes_done"] = self.transfer_wire_base + wire_bytes_sent
        self.metrics["speed_bps"] = int(bytes_sent / elapsed)
        self.log(
            f"增量传输完成 bytes={bytes_sent} wire_bytes={wire_bytes_sent} "
            f"compression={self.compression} elapsed={elapsed:.2f}s "
            f"avg_speed={self.metrics['speed_bps']} B/s"
        )

    def parse_transfer_summary(self, out):
        for line in reversed(out.splitlines()):
            if not line.startswith("sent "):
                continue
            result = {"bytes": 0, "wire_bytes": 0}
            for token in line.split():
                if token.startswith("bytes="):
                    try:
                        result["bytes"] = int(token.split("=", 1)[1])
                    except ValueError:
                        pass
                elif token.startswith("wire_bytes="):
                    try:
                        result["wire_bytes"] = int(token.split("=", 1)[1])
                    except ValueError:
                        pass
            if result["bytes"]:
                if not result["wire_bytes"]:
                    result["wire_bytes"] = result["bytes"]
                return result
        for token in out.replace("\n", " ").split():
            if token.startswith("bytes="):
                try:
                    value = int(token.split("=", 1)[1])
                    return {"bytes": value, "wire_bytes": value}
                except ValueError:
                    pass
        return {"bytes": 0, "wire_bytes": 0}

    def handle_transfer_line(self, line):
        self.log(line)
        if line.startswith("progress "):
            for part in line.split():
                if part.startswith("bytes="):
                    self.metrics["bytes_done"] = self.transfer_base + int(part.split("=", 1)[1])
                elif part.startswith("wire_bytes="):
                    self.metrics["wire_bytes_done"] = self.transfer_wire_base + int(part.split("=", 1)[1])
                elif part.startswith("speed_bps="):
                    self.metrics["speed_bps"] = int(part.split("=", 1)[1])
            self.log(
                f"传输进度 total={self.metrics['bytes_done']} bytes "
                f"wire={self.metrics['wire_bytes_done']} bytes "
                f"speed={self.metrics['speed_bps']} B/s"
            )

    def extract_ranges_metrics(self, out):
        preview = []
        in_preview = False
        for line in out.splitlines():
            if "changed_blocks=" in line:
                for part in line.split():
                    if part.startswith("changed_blocks="):
                        self.metrics["nr_changed_blocks"] = int(part.split("=", 1)[1])
            if line.startswith("---PREVIEW---"):
                in_preview = True
                continue
            if line.startswith("---"):
                in_preview = False
            elif in_preview and "," in line:
                preview.append(line)
            if " /" in line and "ranges-" in line:
                try:
                    self.metrics["ranges_count"] = int(line.strip().split()[0])
                except Exception:
                    pass
        self.metrics["ranges_preview"] = preview[:30]

    def run_prepare(self):
        try:
            self.log("任务准备开始")
            self.push_bundle()
            self.compile_remote()
            self.status = "ready"
            self.phase = "ready"
            self.log("准备完成，可以开始全量迁移")
        except Exception as exc:
            self.fail(str(exc))

    def run_full(self):
        try:
            self.status = "running"
            self.log("全量迁移开始")
            self.preflight_devices()
            self.load_module()
            self.reset_existing_snapshot()
            self.setup_snapshot()
            self.send_full()
            self.transition_incremental()
            self.status = "tracking"
            self.phase = "tracking"
            self.log("全量完成，已进入增量追踪状态")
        except Exception as exc:
            self.fail(str(exc))

    def run_incremental(self):
        try:
            self.status = "running"
            self.log("增量同步开始")
            self.transition_snapshot()
            ranges_path = self.list_ranges()
            self.send_ranges(ranges_path)
            self.transition_incremental()
            self.status = "tracking"
            self.phase = "tracking"
            self.log("本轮增量完成，已继续追踪下一轮变化")
        except Exception as exc:
            self.fail(str(exc))

    def run_cleanup(self):
        try:
            self.status = "running"
            self.phase = "cleanup"
            self.log("清理迁移状态开始")
            cmd = (
                f"{self.remote_env()} cd {self.remote_dir}/dattobd-inspect; "
                f"app/dbdctl destroy {self.minor} || true; "
                f"rm -f /.datto-ui-{self.id}-*; rmmod dattobd || true"
            )
            self.ssh_checked(self.source, cmd, timeout=60)
            self.status = "completed"
            self.phase = "completed"
            self.log("清理完成，迁移任务结束")
        except Exception as exc:
            self.fail(str(exc))


def load_saved_jobs():
    ensure_state_dirs()
    for path in JOBS_DIR.glob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            job = MigrationJob.from_saved(data)
            STATE["jobs"][job.id] = job
        except Exception:
            continue


def start_thread(job, method_name):
    if job.worker and job.worker.is_alive():
        job.log(f"已有任务线程正在运行，忽略重复操作 {method_name}")
        return False
    thread = threading.Thread(target=getattr(job, method_name), daemon=True)
    job.worker = thread
    thread.start()
    return True


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            self.serve_file(ROOT / "static" / "index.html", "text/html; charset=utf-8")
            return
        if path.startswith("/static/"):
            local = ROOT / path.lstrip("/")
            ctype = "application/javascript" if local.suffix == ".js" else "text/css"
            self.serve_file(local, ctype)
            return
        if path.startswith("/api/jobs/"):
            job_id = path.rsplit("/", 1)[-1]
            job = STATE["jobs"].get(job_id)
            if not job:
                json_response(self, {"error": "job not found"}, 404)
                return
            json_response(self, job.snapshot())
            return
        if path == "/api/jobs":
            jobs = sorted(
                [job.snapshot(include_logs=False) for job in STATE["jobs"].values()],
                key=lambda item: item.get("updated_at", ""),
                reverse=True,
            )
            json_response(self, {"jobs": jobs})
            return
        json_response(self, {"error": "not found"}, 404)

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            if path == "/api/probe":
                cfg = read_body(self)
                with tempfile.TemporaryDirectory(prefix="dattobd-probe-") as tmp:
                    ep = Endpoint(cfg, tmp)
                    rc, out = ep.ssh("uname -a; echo ---; lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT; echo ---; df -hT", timeout=20)
                    json_response(self, {"ok": rc == 0, "output": out})
                return
            if path == "/api/jobs":
                cfg = read_body(self)
                job = MigrationJob(cfg)
                STATE["jobs"][job.id] = job
                start_thread(job, "run_prepare")
                json_response(self, {"id": job.id})
                return
            if path == "/api/adopt":
                cfg = read_body(self)
                job = MigrationJob(cfg)
                job.status = "adopted"
                job.phase = "adopted"
                job.log("创建接管任务：不会启动新迁移，只读取远端运行状态。")
                STATE["jobs"][job.id] = job
                start_thread(job, "refresh_remote_status")
                json_response(self, {"id": job.id})
                return
            if path.startswith("/api/jobs/"):
                parts = path.strip("/").split("/")
                job_id = parts[2]
                action = parts[3] if len(parts) > 3 else ""
                job = STATE["jobs"].get(job_id)
                if not job:
                    json_response(self, {"error": "job not found"}, 404)
                    return
                if action == "start-full":
                    if not start_thread(job, "run_full"):
                        json_response(self, {"ok": False, "error": "job is already running"}, 409)
                        return
                elif action == "start-incremental":
                    if not start_thread(job, "run_incremental"):
                        json_response(self, {"ok": False, "error": "job is already running"}, 409)
                        return
                elif action == "cleanup":
                    if not start_thread(job, "run_cleanup"):
                        json_response(self, {"ok": False, "error": "job is already running"}, 409)
                        return
                elif action == "refresh-remote":
                    if not start_thread(job, "refresh_remote_status"):
                        json_response(self, {"ok": False, "error": "job is already running"}, 409)
                        return
                else:
                    json_response(self, {"error": "bad action"}, 400)
                    return
                json_response(self, {"ok": True})
                return
            json_response(self, {"error": "not found"}, 404)
        except Exception as exc:
            json_response(self, {"error": str(exc)}, 500)

    def serve_file(self, path, ctype):
        if not path.exists():
            json_response(self, {"error": "not found"}, 404)
            return
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


def main():
    load_saved_jobs()
    host = os.environ.get("MIGRATION_UI_HOST", "127.0.0.1")
    port = int(os.environ.get("MIGRATION_UI_PORT", "8765"))
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"migration-ui listening on http://{host}:{port}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
