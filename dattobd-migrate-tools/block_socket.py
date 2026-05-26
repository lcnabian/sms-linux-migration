#!/usr/bin/env python3
"""
Small socket transport for block-level migration prototypes.

The receiver listens on a TCP socket and writes incoming chunks at explicit
offsets. The sender can send a whole source device/file or only ranges produced
by list-changed-blocks.
"""

import argparse
import array
import hashlib
import json
import os
import socket
import struct
import sys
import time


MAGIC = "DBD_SOCKET_V1"
DEFAULT_CHUNK = 4 * 1024 * 1024
BLKGETSIZE64 = 0x80081272


def read_exact(sock, size):
    buf = bytearray()
    while len(buf) < size:
        part = sock.recv(size - len(buf))
        if not part:
            raise EOFError("socket closed while reading")
        buf.extend(part)
    return bytes(buf)


def send_json(sock, obj):
    data = json.dumps(obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sock.sendall(struct.pack("!I", len(data)))
    sock.sendall(data)


def recv_json(sock):
    raw_len = read_exact(sock, 4)
    (size,) = struct.unpack("!I", raw_len)
    if size > 1024 * 1024:
        raise ValueError("json frame too large")
    return json.loads(read_exact(sock, size).decode("utf-8"))


def file_size(path):
    stat = os.stat(path)
    if stat.st_size:
        return stat.st_size

    with open(path, "rb", buffering=0) as f:
        buf = array.array("Q", [0])
        try:
            import fcntl
            fcntl.ioctl(f.fileno(), BLKGETSIZE64, buf, True)
            if buf[0]:
                return int(buf[0])
        except OSError:
            pass

        end = os.lseek(f.fileno(), 0, os.SEEK_END)
        os.lseek(f.fileno(), 0, os.SEEK_SET)
        return end


def iter_full_ranges(src_path, chunk_size):
    total = file_size(src_path)
    offset = 0
    while offset < total:
        length = min(chunk_size, total - offset)
        yield offset, length
        offset += length


def iter_csv_ranges(csv_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            if len(parts) != 4:
                raise ValueError("bad range line {}: {}".format(line_no, line))
            _start_block, offset, length, _blocks = parts
            yield int(offset), int(length)


def connect(host, port, retry_seconds):
    deadline = time.time() + retry_seconds
    last_error = None
    while time.time() <= deadline:
        try:
            return socket.create_connection((host, port), timeout=10)
        except OSError as exc:
            last_error = exc
            time.sleep(0.2)
    raise last_error


def send_ranges(args, ranges):
    total_bytes = 0
    total_chunks = 0
    started = time.time()
    last_report = started
    last_bytes = 0

    with connect(args.host, args.port, args.connect_timeout) as sock:
        src_size = file_size(args.source)
        if src_size <= 0:
            raise RuntimeError("source size is 0; cannot send {}".format(args.source))
        send_json(sock, {
            "magic": MAGIC,
            "type": args.mode,
            "source": args.source,
            "source_size": src_size,
            "truncate": args.truncate,
        })
        ack = recv_json(sock)
        if ack.get("status") != "ok":
            raise RuntimeError("receiver rejected session: {}".format(ack))

        with open(args.source, "rb", buffering=0) as src:
            for offset, length in ranges:
                remaining = length
                cursor = offset
                while remaining:
                    to_read = min(args.chunk_size, remaining)
                    data = os.pread(src.fileno(), to_read, cursor)
                    if len(data) != to_read:
                        raise IOError("short read at offset {}".format(cursor))
                    digest = hashlib.sha256(data).hexdigest()
                    send_json(sock, {
                        "type": "chunk",
                        "offset": cursor,
                        "length": len(data),
                        "sha256": digest,
                    })
                    sock.sendall(data)
                    ack = recv_json(sock)
                    if ack.get("status") != "ok":
                        raise RuntimeError("receiver chunk error: {}".format(ack))
                    total_bytes += len(data)
                    total_chunks += 1
                    now_ts = time.time()
                    if now_ts - last_report >= args.progress_interval:
                        interval = max(now_ts - last_report, 0.001)
                        speed = int((total_bytes - last_bytes) / interval)
                        print("progress chunks={} bytes={} speed_bps={}".format(
                            total_chunks, total_bytes, speed), flush=True)
                        last_report = now_ts
                        last_bytes = total_bytes
                    cursor += len(data)
                    remaining -= len(data)

        send_json(sock, {
            "type": "done",
            "chunks": total_chunks,
            "bytes": total_bytes,
        })
        ack = recv_json(sock)
        if ack.get("status") != "ok":
            raise RuntimeError("receiver final error: {}".format(ack))

    print("sent chunks={} bytes={}".format(total_chunks, total_bytes))


def receive_once(args):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((args.host, args.port))
        server.listen(1)
        print("listening {}:{}".format(args.host, args.port), flush=True)
        conn, addr = server.accept()

        with conn:
            print("accepted {}:{}".format(addr[0], addr[1]), flush=True)
            hello = recv_json(conn)
            if hello.get("magic") != MAGIC:
                send_json(conn, {"status": "error", "error": "bad magic"})
                return 2

            flags = os.O_CREAT | os.O_RDWR
            if args.direct:
                flags |= getattr(os, "O_DIRECT", 0)
            fd = os.open(args.target, flags, 0o600)
            try:
                if hello.get("truncate"):
                    os.ftruncate(fd, int(hello["source_size"]))

                send_json(conn, {"status": "ok"})
                chunks = 0
                total = 0

                while True:
                    msg = recv_json(conn)
                    msg_type = msg.get("type")
                    if msg_type == "done":
                        send_json(conn, {
                            "status": "ok",
                            "chunks": chunks,
                            "bytes": total,
                        })
                        print("received chunks={} bytes={}".format(chunks, total), flush=True)
                        return 0

                    if msg_type != "chunk":
                        send_json(conn, {"status": "error", "error": "bad message"})
                        return 2

                    offset = int(msg["offset"])
                    length = int(msg["length"])
                    data = read_exact(conn, length)
                    digest = hashlib.sha256(data).hexdigest()
                    if digest != msg["sha256"]:
                        send_json(conn, {"status": "error", "error": "sha256 mismatch"})
                        return 2

                    written = os.pwrite(fd, data, offset)
                    if written != length:
                        send_json(conn, {"status": "error", "error": "short write"})
                        return 2
                    chunks += 1
                    total += length
                    send_json(conn, {"status": "ok"})
            finally:
                os.fsync(fd)
                os.close(fd)


def main(argv=None):
    parser = argparse.ArgumentParser(description="socket transport for block ranges")
    sub = parser.add_subparsers(dest="cmd")
    sub.required = True

    recv = sub.add_parser("receive")
    recv.add_argument("--host", default="0.0.0.0")
    recv.add_argument("--port", type=int, required=True)
    recv.add_argument("--target", required=True)
    recv.add_argument("--direct", action="store_true")

    full = sub.add_parser("send-full")
    full.add_argument("--host", required=True)
    full.add_argument("--port", type=int, required=True)
    full.add_argument("--source", required=True)
    full.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK)
    full.add_argument("--connect-timeout", type=float, default=10.0)
    full.add_argument("--progress-interval", type=float, default=1.0)
    full.add_argument("--truncate", action="store_true")

    ranges = sub.add_parser("send-ranges")
    ranges.add_argument("--host", required=True)
    ranges.add_argument("--port", type=int, required=True)
    ranges.add_argument("--source", required=True)
    ranges.add_argument("--ranges", required=True)
    ranges.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK)
    ranges.add_argument("--connect-timeout", type=float, default=10.0)
    ranges.add_argument("--progress-interval", type=float, default=1.0)
    ranges.add_argument("--truncate", action="store_true")

    args = parser.parse_args(argv)
    if args.cmd == "receive":
        return receive_once(args)
    if args.cmd == "send-full":
        args.mode = "full"
        send_ranges(args, iter_full_ranges(args.source, args.chunk_size))
        return 0
    if args.cmd == "send-ranges":
        args.mode = "ranges"
        send_ranges(args, iter_csv_ranges(args.ranges))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
