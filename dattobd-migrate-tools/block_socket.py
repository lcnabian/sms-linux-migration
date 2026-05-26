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
import threading
import time
import zlib


MAGIC = "DBD_SOCKET_V1"
DEFAULT_CHUNK = 4 * 1024 * 1024
DEFAULT_ACK_EVERY_CHUNKS = 32
BLKGETSIZE64 = 0x80081272
COMPRESSION_NONE = "none"
COMPRESSION_ZLIB = "zlib"
COMPRESSION_AUTO = "auto"
COMPRESSION_CHOICES = (COMPRESSION_NONE, COMPRESSION_ZLIB, COMPRESSION_AUTO)


def supported_compressions(decompression):
    if decompression == "none":
        return [COMPRESSION_NONE]
    return [COMPRESSION_NONE, COMPRESSION_ZLIB]


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
            sock = socket.create_connection((host, port), timeout=10)
            sock.settimeout(None)
            return sock
        except OSError as exc:
            last_error = exc
            time.sleep(0.2)
    raise last_error


def build_chunk_payload(data, compression, compression_level):
    if compression == COMPRESSION_NONE:
        return data, COMPRESSION_NONE

    compressed = zlib.compress(data, compression_level)
    if compression == COMPRESSION_AUTO and len(compressed) >= len(data):
        return data, COMPRESSION_NONE
    return compressed, COMPRESSION_ZLIB


def decode_chunk_payload(payload, compression, expected_length):
    if compression == COMPRESSION_NONE:
        data = payload
    elif compression == COMPRESSION_ZLIB:
        data = zlib.decompress(payload)
    else:
        raise ValueError("unsupported compression: {}".format(compression))

    if len(data) != expected_length:
        raise ValueError(
            "decompressed length mismatch: got {}, expected {}".format(
                len(data), expected_length
            )
        )
    return data


class AckState:
    def __init__(self):
        self.lock = threading.Lock()
        self.done = threading.Event()
        self.error = None
        self.final_ack = None
        self.last_ack = None
        self.done_requested = False

    def mark_done_requested(self):
        with self.lock:
            self.done_requested = True

    def set_ack(self, ack):
        with self.lock:
            self.last_ack = ack
            if ack.get("status") != "ok":
                self.error = ack.get("error") or "receiver returned an error"
                self.done.set()
                return

            ack_type = ack.get("type")
            if ack_type == "done":
                self.final_ack = ack
                self.done.set()
            elif ack_type is None and "chunks" in ack and "bytes" in ack:
                # Compatibility with the original receiver final response.
                self.final_ack = ack
                self.done.set()

    def set_error(self, exc):
        with self.lock:
            if not self.error:
                self.error = str(exc)
            self.done.set()

    def raise_if_error(self):
        with self.lock:
            if self.error:
                raise RuntimeError("receiver error: {}".format(self.error))

    def wait_final(self, timeout=None):
        if not self.done.wait(timeout):
            raise TimeoutError("timed out waiting for receiver final ack")
        self.raise_if_error()
        with self.lock:
            if not self.final_ack:
                raise RuntimeError("receiver closed before final ack")
            return self.final_ack


def ack_reader(sock, state):
    try:
        while True:
            ack = recv_json(sock)
            state.set_ack(ack)
            if ack.get("status") != "ok" or ack.get("type") == "done":
                return
            with state.lock:
                if state.final_ack:
                    return
    except Exception as exc:
        state.set_error(exc)


def send_ranges(args, ranges):
    total_bytes = 0
    total_wire_bytes = 0
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
            "ack_every_chunks": args.ack_every_chunks,
            "compression": args.compression,
            "compression_level": args.compression_level,
        })
        ack = recv_json(sock)
        if ack.get("status") != "ok":
            raise RuntimeError("receiver rejected session: {}".format(ack))
        if args.compression != COMPRESSION_NONE:
            receiver_compressions = ack.get("compression") or []
            if COMPRESSION_ZLIB not in receiver_compressions:
                raise RuntimeError(
                    "receiver does not advertise zlib decompression; "
                    "start the receiver with --decompression auto"
                )

        ack_state = AckState()
        reader = threading.Thread(target=ack_reader, args=(sock, ack_state), daemon=True)
        reader.start()

        with open(args.source, "rb", buffering=0) as src:
            for offset, length in ranges:
                remaining = length
                cursor = offset
                while remaining:
                    ack_state.raise_if_error()
                    to_read = min(args.chunk_size, remaining)
                    data = os.pread(src.fileno(), to_read, cursor)
                    if len(data) != to_read:
                        raise IOError("short read at offset {}".format(cursor))
                    digest = hashlib.sha256(data).hexdigest()
                    payload, payload_compression = build_chunk_payload(
                        data, args.compression, args.compression_level
                    )
                    send_json(sock, {
                        "type": "chunk",
                        "offset": cursor,
                        "length": len(data),
                        "wire_length": len(payload),
                        "compression": payload_compression,
                        "sha256": digest,
                    })
                    sock.sendall(payload)
                    total_bytes += len(data)
                    total_wire_bytes += len(payload)
                    total_chunks += 1
                    now_ts = time.time()
                    if now_ts - last_report >= args.progress_interval:
                        interval = max(now_ts - last_report, 0.001)
                        speed = int((total_bytes - last_bytes) / interval)
                        print(
                            "progress chunks={} bytes={} wire_bytes={} speed_bps={}".format(
                                total_chunks, total_bytes, total_wire_bytes, speed
                            ),
                            flush=True,
                        )
                        last_report = now_ts
                        last_bytes = total_bytes
                    cursor += len(data)
                    remaining -= len(data)

        send_json(sock, {
            "type": "done",
            "chunks": total_chunks,
            "bytes": total_bytes,
            "wire_bytes": total_wire_bytes,
        })
        ack_state.mark_done_requested()
        ack = ack_state.wait_final(timeout=300)
        reader.join(timeout=1)
        if ack.get("status") != "ok":
            raise RuntimeError("receiver final error: {}".format(ack))

    print("sent chunks={} bytes={} wire_bytes={}".format(
        total_chunks, total_bytes, total_wire_bytes
    ))


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

            receiver_compressions = supported_compressions(args.decompression)
            requested_compression = hello.get("compression", COMPRESSION_NONE)
            if requested_compression != COMPRESSION_NONE and COMPRESSION_ZLIB not in receiver_compressions:
                send_json(conn, {
                    "status": "error",
                    "error": "receiver decompression disabled",
                    "compression": receiver_compressions,
                })
                return 2

            flags = os.O_CREAT | os.O_RDWR
            if args.direct:
                flags |= getattr(os, "O_DIRECT", 0)
            fd = os.open(args.target, flags, 0o600)
            try:
                if hello.get("truncate"):
                    os.ftruncate(fd, int(hello["source_size"]))

                send_json(conn, {
                    "status": "ok",
                    "compression": receiver_compressions,
                    "decompression": args.decompression,
                })
                ack_every_chunks = int(hello.get("ack_every_chunks") or 1)
                if ack_every_chunks <= 0:
                    ack_every_chunks = 1
                chunks = 0
                total = 0
                total_wire = 0

                while True:
                    msg = recv_json(conn)
                    msg_type = msg.get("type")
                    if msg_type == "done":
                        send_json(conn, {
                            "status": "ok",
                            "type": "done",
                            "chunks": chunks,
                            "bytes": total,
                            "wire_bytes": total_wire,
                        })
                        print(
                            "received chunks={} bytes={} wire_bytes={}".format(
                                chunks, total, total_wire
                            ),
                            flush=True,
                        )
                        return 0

                    if msg_type != "chunk":
                        send_json(conn, {"status": "error", "error": "bad message"})
                        return 2

                    offset = int(msg["offset"])
                    length = int(msg["length"])
                    compression = msg.get("compression", COMPRESSION_NONE)
                    if compression != COMPRESSION_NONE and args.decompression == "none":
                        send_json(conn, {
                            "status": "error",
                            "error": "compressed chunk rejected by receiver",
                            "chunks": chunks,
                            "bytes": total,
                            "wire_bytes": total_wire,
                        })
                        return 2
                    wire_length = int(msg.get("wire_length", length))
                    payload = read_exact(conn, wire_length)
                    total_wire += wire_length
                    try:
                        data = decode_chunk_payload(payload, compression, length)
                    except Exception as exc:
                        send_json(conn, {
                            "status": "error",
                            "error": str(exc),
                            "chunks": chunks,
                            "bytes": total,
                            "wire_bytes": total_wire,
                        })
                        return 2
                    digest = hashlib.sha256(data).hexdigest()
                    if digest != msg["sha256"]:
                        send_json(conn, {
                            "status": "error",
                            "error": "sha256 mismatch",
                            "chunks": chunks,
                            "bytes": total,
                            "wire_bytes": total_wire,
                        })
                        return 2

                    written = os.pwrite(fd, data, offset)
                    if written != length:
                        send_json(conn, {
                            "status": "error",
                            "error": "short write",
                            "chunks": chunks,
                            "bytes": total,
                            "wire_bytes": total_wire,
                        })
                        return 2
                    chunks += 1
                    total += length
                    if chunks % ack_every_chunks == 0:
                        send_json(conn, {
                            "status": "ok",
                            "type": "ack",
                            "chunks": chunks,
                            "bytes": total,
                            "wire_bytes": total_wire,
                        })
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
    recv.add_argument("--decompression", choices=("auto", "none"), default="auto")

    full = sub.add_parser("send-full")
    full.add_argument("--host", required=True)
    full.add_argument("--port", type=int, required=True)
    full.add_argument("--source", required=True)
    full.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK)
    full.add_argument("--connect-timeout", type=float, default=10.0)
    full.add_argument("--progress-interval", type=float, default=1.0)
    full.add_argument("--ack-every-chunks", type=int, default=DEFAULT_ACK_EVERY_CHUNKS)
    full.add_argument("--compression", choices=COMPRESSION_CHOICES, default=COMPRESSION_NONE)
    full.add_argument("--compression-level", type=int, choices=range(1, 10), default=1)
    full.add_argument("--truncate", action="store_true")

    ranges = sub.add_parser("send-ranges")
    ranges.add_argument("--host", required=True)
    ranges.add_argument("--port", type=int, required=True)
    ranges.add_argument("--source", required=True)
    ranges.add_argument("--ranges", required=True)
    ranges.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK)
    ranges.add_argument("--connect-timeout", type=float, default=10.0)
    ranges.add_argument("--progress-interval", type=float, default=1.0)
    ranges.add_argument("--ack-every-chunks", type=int, default=DEFAULT_ACK_EVERY_CHUNKS)
    ranges.add_argument("--compression", choices=COMPRESSION_CHOICES, default=COMPRESSION_NONE)
    ranges.add_argument("--compression-level", type=int, choices=range(1, 10), default=1)
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
