#!/usr/bin/env python3
"""
prusa_recorder: records everything a Prusa Buddy printer (MK4, MK3.9, MINI, XL, CORE One)
can tell us about itself, to plain files, for post-mortem debugging.

Sources:
  1. Buddy metrics over UDP (syslog framing + InfluxDB line protocol), default port 8514.
     Format taken from Prusa-Firmware-Buddy src/common/metric_handlers.cpp:
       <14>1 - <MAC> buddy - - - msg=<id>,tm=<abs_us>,v=<ver> <point>\n<point>\n...
       point = <name>[,<tag>=<val>...] <field>=<val>[,...] <ts_offset_from_tm>
  2. Buddy logs over UDP (syslog), default port 8515. Stored raw.
  3. PrusaLink REST API (/api/v1/status, /api/v1/job, /api/v1/info), polled every N seconds.
     Works over Wi-Fi too. Coarse, but a good independent cross-check.

Stdlib only. Python 3.9+.

Output (one directory per session):
  metrics.jsonl      one parsed data point per line
  metrics_raw.log    every metrics packet as received (base64-free, utf-8 with replacement)
  logs_raw.log       every log packet as received
  prusalink.jsonl    one PrusaLink poll per line
  events.log         recorder's own notes: start/stop, packet loss, parse errors, value alarms
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ----------------------------------------------------------------------------
# Line protocol parsing
# ----------------------------------------------------------------------------

HEADER_RE = re.compile(
    r"^<(?P<pri>\d+)>(?P<ver>\d+) (?P<ts>\S+) (?P<host>\S+) (?P<app>\S+) (?P<procid>\S+) (?P<msgid>\S+) (?P<sd>\S+) ?(?P<rest>.*)$",
    re.DOTALL,
)
METRICS_HDR_RE = re.compile(r"^msg=(?P<msg>\d+),tm=(?P<tm>-?\d+),v=(?P<v>\d+) ?(?P<body>.*)$", re.DOTALL)


def split_unquoted(s: str, sep: str) -> list[str]:
    """Split on `sep` outside double quotes, honouring backslash escapes."""
    out, cur, in_q, esc = [], [], False, False
    for ch in s:
        if esc:
            cur.append(ch)
            esc = False
        elif ch == "\\":
            cur.append(ch)
            esc = True
        elif ch == '"':
            cur.append(ch)
            in_q = not in_q
        elif ch == sep and not in_q:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    out.append("".join(cur))
    return out


def parse_value(raw: str):
    if raw.startswith('"') and raw.endswith('"') and len(raw) >= 2:
        return raw[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    if raw.endswith("i"):
        try:
            return int(raw[:-1])
        except ValueError:
            pass
    if raw in ("T", "t", "true", "True"):
        return True
    if raw in ("F", "f", "false", "False"):
        return False
    try:
        return float(raw)
    except ValueError:
        return raw


def parse_point(line: str):
    """Parse one line-protocol point. Returns (name, tags, fields, offset) or raises ValueError."""
    parts = [p for p in split_unquoted(line.strip(), " ") if p != ""]
    if len(parts) < 3:
        raise ValueError(f"too few parts: {line!r}")
    head, offset = parts[0], parts[-1]
    fields_raw = " ".join(parts[1:-1])  # string fields may contain spaces
    name_and_tags = split_unquoted(head, ",")
    name = name_and_tags[0]
    tags = {}
    for t in name_and_tags[1:]:
        k, _, v = t.partition("=")
        tags[k] = v
    fields = {}
    for f in split_unquoted(fields_raw, ","):
        k, _, v = f.partition("=")
        fields[k] = parse_value(v)
    return name, tags, fields, int(offset)


# ----------------------------------------------------------------------------
# Writer
# ----------------------------------------------------------------------------


class Writer:
    def __init__(self, out_dir: Path):
        self.dir = out_dir
        self.dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._files = {
            name: open(self.dir / name, "a", encoding="utf-8", buffering=1)  # line buffered
            for name in ("metrics.jsonl", "metrics_raw.log", "logs_raw.log", "prusalink.jsonl", "events.log")
        }

    def write(self, name: str, line: str):
        with self._lock:
            self._files[name].write(line.rstrip("\n") + "\n")

    def event(self, msg: str):
        line = f"{now_iso()} {msg}"
        self.write("events.log", line)
        print(line, flush=True)

    def flush(self):
        with self._lock:
            for f in self._files.values():
                f.flush()
                os.fsync(f.fileno())

    def close(self):
        with self._lock:
            for f in self._files.values():
                f.close()


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds")


# ----------------------------------------------------------------------------
# UDP listeners
# ----------------------------------------------------------------------------


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.packets = 0
        self.points = 0
        self.parse_errors = 0
        self.lost_packets = 0
        self.log_packets = 0
        self.metric_names: dict[str, int] = {}
        self.last_values: dict[str, object] = {}


class MetricsListener(threading.Thread):
    def __init__(self, bind: str, port: int, w: Writer, st: Stats, watch: dict[str, tuple[float, float]]):
        super().__init__(daemon=True, name="metrics")
        self.w, self.st, self.watch = w, st, watch
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        self.sock.bind((bind, port))
        self.last_msg_id: dict[str, int] = {}
        self.error_counts: dict[tuple[str, str], int] = {}

    def run(self):
        while True:
            data, addr = self.sock.recvfrom(65535)
            recv = time.time()
            text = data.decode("utf-8", errors="replace")
            self.w.write("metrics_raw.log", f"{recv:.6f} {addr[0]} {text.rstrip()!r}")
            with self.st.lock:
                self.st.packets += 1
            try:
                self.handle(text, recv, addr[0])
            except Exception as e:  # never die on a bad packet
                with self.st.lock:
                    self.st.parse_errors += 1
                self.w.event(f"PARSE_ERROR packet from {addr[0]}: {e}")

    def handle(self, text: str, recv: float, src: str):
        m = HEADER_RE.match(text)
        if not m:
            raise ValueError(f"not a syslog packet: {text[:80]!r}")
        host = m["host"]
        mh = METRICS_HDR_RE.match(m["rest"])
        if not mh:
            raise ValueError(f"no metrics header: {m['rest'][:80]!r}")
        msg_id, tm, ver = int(mh["msg"]), int(mh["tm"]), int(mh["v"])

        prev = self.last_msg_id.get(host)
        if prev is not None:
            gap = msg_id - prev - 1
            if gap > 0:
                with self.st.lock:
                    self.st.lost_packets += gap
                self.w.event(f"PACKET_LOSS host={host} lost={gap} (msg {prev} -> {msg_id})")
            elif msg_id <= prev:
                self.w.event(f"MSG_ID_RESET host={host} {prev} -> {msg_id} (printer rebooted?)")
        self.last_msg_id[host] = msg_id

        for line in mh["body"].split("\n"):
            if not line.strip():
                continue
            try:
                name, tags, fields, offset = parse_point(line)
            except Exception as e:
                with self.st.lock:
                    self.st.parse_errors += 1
                self.w.event(f"POINT_PARSE_ERROR {e} line={line!r}")
                continue
            rec = {
                "recv": round(recv, 6),
                "host": host,
                "msg": msg_id,
                "proto": ver,
                "tm": tm,
                "off": offset,
                "t_printer": tm + offset,  # printer uptime units (us on v4)
                "metric": name,
            }
            if tags:
                rec["tags"] = tags
            rec.update({f"f_{k}" if k in rec else k: v for k, v in fields.items()})
            self.w.write("metrics.jsonl", json.dumps(rec, separators=(",", ":")))
            with self.st.lock:
                self.st.points += 1
                self.st.metric_names[name] = self.st.metric_names.get(name, 0) + 1
                if "v" in fields:
                    self.st.last_values[name] = fields["v"]
            if "error" in fields:
                # Firmware reports e.g. "value too long" for every oversized point; log the
                # first one and then every 1000th so events.log stays readable.
                key = (name, str(fields["error"]))
                n = self.error_counts.get(key, 0) + 1
                self.error_counts[key] = n
                if n == 1 or n % 1000 == 0:
                    self.w.event(f"METRIC_ERROR {name}: {fields['error']} (count={n})")
            self.check_watch(name, fields)

    def check_watch(self, name: str, fields: dict):
        rng = self.watch.get(name)
        v = fields.get("v")
        if rng is None or not isinstance(v, (int, float)) or isinstance(v, bool):
            return
        lo, hi = rng
        if v < lo or v > hi:
            self.w.event(f"OUT_OF_RANGE {name}={v} (expected {lo}..{hi})")


class LogListener(threading.Thread):
    def __init__(self, bind: str, port: int, w: Writer, st: Stats, alarm_words: list[str]):
        super().__init__(daemon=True, name="logs")
        self.w, self.st, self.alarm_words = w, st, [a.lower() for a in alarm_words]
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((bind, port))

    def run(self):
        while True:
            data, addr = self.sock.recvfrom(65535)
            recv = time.time()
            text = data.decode("utf-8", errors="replace").rstrip()
            self.w.write("logs_raw.log", f"{recv:.6f} {addr[0]} {text}")
            with self.st.lock:
                self.st.log_packets += 1
            low = text.lower()
            for a in self.alarm_words:
                if a in low:
                    self.w.event(f"LOG_ALARM [{a}] {text[:300]}")
                    break


# ----------------------------------------------------------------------------
# PrusaLink poller
# ----------------------------------------------------------------------------


class PrusaLinkPoller(threading.Thread):
    ENDPOINTS = ("/api/v1/status", "/api/v1/job")

    def __init__(self, host: str, user: str, password: str | None, api_key: str | None,
                 interval: float, w: Writer):
        super().__init__(daemon=True, name="prusalink")
        self.base = host if host.startswith("http") else f"http://{host}"
        self.interval, self.w = interval, w
        self.headers = {"Accept": "application/json"}
        handlers = []
        if api_key:
            self.headers["X-Api-Key"] = api_key
        if password:
            mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
            mgr.add_password(None, self.base, user, password)
            handlers.append(urllib.request.HTTPDigestAuthHandler(mgr))
        self.opener = urllib.request.build_opener(*handlers)
        self.fail_streak = 0

    def get(self, path: str):
        req = urllib.request.Request(self.base + path, headers=self.headers)
        with self.opener.open(req, timeout=3) as r:
            body = r.read()
            return r.status, (json.loads(body) if body else None)

    def run(self):
        try:
            _, info = self.get("/api/v1/info")
            self.w.write("prusalink.jsonl", json.dumps({"recv": time.time(), "info": info}))
        except Exception as e:
            self.w.event(f"PRUSALINK info failed: {e}")
        last_state = None
        while True:
            t0 = time.time()
            rec = {"recv": round(t0, 3)}
            ok = True
            for p in self.ENDPOINTS:
                try:
                    status, body = self.get(p)
                    rec[p.rsplit("/", 1)[-1]] = body if status == 200 else {"http": status}
                except urllib.error.HTTPError as e:
                    rec[p.rsplit("/", 1)[-1]] = {"http": e.code}
                    if e.code == 401:
                        ok = False
                except Exception as e:
                    rec[p.rsplit("/", 1)[-1]] = {"error": str(e)}
                    ok = False
            self.w.write("prusalink.jsonl", json.dumps(rec, separators=(",", ":")))

            st = rec.get("status") or {}
            state = (st.get("printer") or {}).get("state") if isinstance(st, dict) else None
            if state and state != last_state:
                self.w.event(f"PRINTER_STATE {last_state} -> {state}")
                last_state = state

            if ok:
                if self.fail_streak:
                    self.w.event(f"PRUSALINK back after {self.fail_streak} failures")
                self.fail_streak = 0
            else:
                self.fail_streak += 1
                if self.fail_streak in (1, 10, 100):
                    self.w.event(f"PRUSALINK failing (streak {self.fail_streak}): {rec}")
            time.sleep(max(0.0, self.interval - (time.time() - t0)))


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------


def parse_watch(items: list[str]) -> dict[str, tuple[float, float]]:
    out = {}
    for it in items:
        name, _, rng = it.partition("=")
        lo, _, hi = rng.partition(":")
        out[name] = (float(lo), float(hi))
    return out


def main():
    ap = argparse.ArgumentParser(description="Record all Prusa Buddy metrics, logs and PrusaLink status to files.")
    ap.add_argument("--out", default="sessions", help="base output directory (default: sessions)")
    ap.add_argument("--bind", default="0.0.0.0")
    ap.add_argument("--metrics-port", type=int, default=8514)
    ap.add_argument("--log-port", type=int, default=8515, help="0 disables the log listener")
    ap.add_argument("--printer", help="printer IP/host for PrusaLink polling (optional)")
    ap.add_argument("--user", default="maker", help="PrusaLink user (default: maker)")
    ap.add_argument("--password", default=os.environ.get("PRUSALINK_PASSWORD"),
                    help="PrusaLink password (or env PRUSALINK_PASSWORD)")
    ap.add_argument("--api-key", default=os.environ.get("PRUSALINK_API_KEY"),
                    help="PrusaLink API key, if your firmware uses one (or env PRUSALINK_API_KEY)")
    ap.add_argument("--poll", type=float, default=1.0, help="PrusaLink poll interval in seconds")
    ap.add_argument("--watch", action="append", default=[],
                    help="flag values outside a range, e.g. --watch temp_bed=15:130 (repeatable)")
    ap.add_argument("--status-every", type=float, default=30.0, help="console summary interval, seconds")
    args = ap.parse_args()

    session = Path(args.out) / datetime.now().strftime("%Y%m%d-%H%M%S")
    w = Writer(session)
    st = Stats()
    w.event(f"START session={session} args={ {k: v for k, v in vars(args).items() if k not in ('password', 'api_key')} }")

    MetricsListener(args.bind, args.metrics_port, w, st, parse_watch(args.watch)).start()
    if args.log_port:
        LogListener(args.bind, args.log_port, w, st,
                    ["mintemp", "maxtemp", "thermal", "runaway", "error", "bsod", "fault"]).start()
    if args.printer:
        PrusaLinkPoller(args.printer, args.user, args.password, args.api_key, args.poll, w).start()

    stop = threading.Event()

    def on_signal(signum, _frame):
        stop.set()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    while not stop.wait(args.status_every):
        with st.lock:
            top = ", ".join(f"{k}={st.last_values.get(k)}" for k in sorted(st.metric_names)[:12])
            print(f"{now_iso()} packets={st.packets} points={st.points} lost={st.lost_packets} "
                  f"parse_err={st.parse_errors} logs={st.log_packets} metrics={len(st.metric_names)} | {top}",
                  flush=True)
        w.flush()

    with st.lock:
        summary = {"packets": st.packets, "points": st.points, "lost_packets": st.lost_packets,
                   "parse_errors": st.parse_errors, "log_packets": st.log_packets,
                   "metric_counts": dict(sorted(st.metric_names.items()))}
    (session / "summary.json").write_text(json.dumps(summary, indent=2))
    w.event(f"STOP {json.dumps(summary)}")
    w.flush()
    w.close()


if __name__ == "__main__":
    main()
