#!/usr/bin/env python3
"""
analyze.py: quick post-mortem of a prusa_recorder session.

  python3 analyze.py sessions/20261011-120000
  python3 analyze.py sessions/20261011-120000 --metric temp_bed --tail 60 --csv bed.csv

Prints: metrics seen, per-metric min/max, the biggest sample-to-sample jumps for
temperature-like metrics, and the last N seconds of data before the stream ended.
"""

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


def load(path: Path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("session")
    ap.add_argument("--metric", action="append", default=[], help="focus metric(s); default: names containing 'temp' or 'bed'")
    ap.add_argument("--tail", type=float, default=30.0, help="seconds before end of stream to print")
    ap.add_argument("--jumps", type=int, default=10, help="how many largest jumps to show per metric")
    ap.add_argument("--csv", help="export focus metrics to CSV (recv, t_printer, metric, v)")
    args = ap.parse_args()

    s = Path(args.session)
    series = defaultdict(list)  # name -> [(recv, t_printer, v)]
    for r in load(s / "metrics.jsonl"):
        v = r.get("v")
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            continue
        series[r["metric"]].append((r["recv"], r["t_printer"], v))

    if not series:
        print("No numeric metrics in this session. Check printer Metrics & Log settings and firewall.")
        return

    focus = args.metric or [n for n in series if "temp" in n or "bed" in n]
    print("== metrics seen ==")
    for n in sorted(series):
        vals = [x[2] for x in series[n]]
        mark = " *" if n in focus else ""
        print(f"  {n:28s} n={len(vals):7d} min={min(vals):10.3f} max={max(vals):10.3f}{mark}")

    print("\n== largest jumps between consecutive samples (focus metrics) ==")
    for n in focus:
        pts = sorted(series.get(n, []), key=lambda x: x[1])
        jumps = []
        for a, b in zip(pts, pts[1:]):
            jumps.append((abs(b[2] - a[2]), a, b))
        jumps.sort(key=lambda j: -j[0])
        print(f"  {n}:")
        for d, a, b in jumps[: args.jumps]:
            dt_ms = (b[1] - a[1]) / 1000.0
            print(f"    Δ={d:9.3f}  {a[2]:9.3f} -> {b[2]:9.3f}  over {dt_ms:8.1f} ms  (recv {b[0]:.3f})")

    end = max(x[0] for pts in series.values() for x in pts)
    print(f"\n== last {args.tail:.0f}s before stream end (recv={end:.3f}) ==")
    for n in focus:
        tail = [x for x in series.get(n, []) if x[0] >= end - args.tail]
        if tail:
            vals = ", ".join(f"{v:.2f}" for _, _, v in tail[-40:])
            print(f"  {n}: {vals}")

    ev = s / "events.log"
    if ev.exists():
        lines = ev.read_text(encoding="utf-8").splitlines()
        interesting = [l for l in lines if any(k in l for k in ("OUT_OF_RANGE", "LOG_ALARM", "PACKET_LOSS", "STATE", "METRIC_ERROR", "RESET"))]
        print(f"\n== events ({len(interesting)} interesting of {len(lines)}) ==")
        for l in interesting[-40:]:
            print("  " + l)

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            wr = csv.writer(f)
            wr.writerow(["recv", "t_printer", "metric", "v"])
            for n in focus:
                for recv, tp, v in series.get(n, []):
                    wr.writerow([recv, tp, n, v])
        print(f"\nCSV written: {args.csv}")


if __name__ == "__main__":
    main()
