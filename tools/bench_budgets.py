import argparse
import json
import os
import platform
import resource
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import synthetic_configs

BUDGETS = {
    ("tpch", 1.0, "tbl"): (3.0, 1536),
    ("tpch", 1.0, "parquet"): (4.0, 1536),
    ("tpch", 10.0, "tbl"): (30.0, 2048),
    ("tpch", 10.0, "parquet"): (40.0, 2048),
    ("tpcds", 1.0, "tbl"): (3.0, 1024),
    ("tpcds", 1.0, "parquet"): (4.0, 1024),
    ("tpcds", 10.0, "tbl"): (30.0, 2048),
    ("tpcds", 10.0, "parquet"): (40.0, 2048),
}
for name in synthetic_configs.names():
    for (scale, fmt), budget in synthetic_configs.load(name).budgets.items():
        BUDGETS[(name, scale, fmt)] = budget

CHILD = """
import json, resource, sys, time
import benchbox_datagen
benchmark, scale, fmt, out, threads = sys.argv[1], float(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5]
start = time.monotonic()
stats = benchbox_datagen.write_tables(benchmark, scale, out, format=fmt, threads=None if threads == "default" else int(threads))
wall = time.monotonic() - start
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
if sys.platform != "darwin":
    peak *= 1024
print(json.dumps({"wall_s": wall, "peak_rss_mib": peak / (1 << 20), "rows": sum(s.rows for s in stats), "bytes": sum(s.bytes for s in stats)}))
"""


def measure_driver(driver: str, scale: float, fmt: str, threads: str, workdir: Path) -> dict[str, float]:
    out = Path(tempfile.mkdtemp(prefix="bench-", dir=workdir))
    try:
        started = time.monotonic()
        process = subprocess.Popen([driver, str(scale), "42", fmt, str(out), threads])
        _, status, usage = os.wait4(process.pid, 0)
        wall = time.monotonic() - started
        if status != 0:
            raise subprocess.CalledProcessError(status, driver)
        peak = usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024
        size = sum(path.stat().st_size for path in out.iterdir())
        return {"wall_s": wall, "peak_rss_mib": peak / (1 << 20), "bytes": size}
    finally:
        shutil.rmtree(out, ignore_errors=True)


def measure(benchmark: str, scale: float, fmt: str, threads: str, workdir: Path) -> dict[str, float]:
    out = Path(tempfile.mkdtemp(prefix="bench-", dir=workdir))
    try:
        completed = subprocess.run(
            [sys.executable, "-c", CHILD, benchmark, str(scale), fmt, str(out), threads],
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(completed.stdout)
    finally:
        shutil.rmtree(out, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", choices=["tpch", "tpcds", *synthetic_configs.names()], default="tpch")
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--scales", type=float, nargs="+", default=[1.0, 10.0])
    parser.add_argument("--formats", nargs="+", default=["tbl", "parquet"])
    parser.add_argument("--threads", default="default")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--driver")
    args = parser.parse_args()
    report: dict[str, object] = {
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "cpus": os.cpu_count(),
            "loadavg_start": os.getloadavg(),
        },
        "benchmark": args.benchmark,
        "threads": args.threads,
        "results": [],
    }
    ok = True
    for scale in args.scales:
        for fmt in args.formats:
            load_before = os.getloadavg()
            if args.driver:
                runs = [measure_driver(args.driver, scale, fmt, args.threads, Path(args.workdir)) for _ in range(args.runs)]
            else:
                runs = [measure(args.benchmark, scale, fmt, args.threads, Path(args.workdir)) for _ in range(args.runs)]
            wall_budget, rss_budget = BUDGETS[(args.benchmark, scale, fmt)]
            wall = statistics.median(run["wall_s"] for run in runs)
            rss = statistics.median(run["peak_rss_mib"] for run in runs)
            met = wall <= wall_budget and max(run["peak_rss_mib"] for run in runs) <= rss_budget
            ok &= met
            report["results"].append(
                {
                    "scale": scale,
                    "format": fmt,
                    "runs_wall_s": [round(run["wall_s"], 3) for run in runs],
                    "runs_peak_rss_mib": [round(run["peak_rss_mib"], 1) for run in runs],
                    "median_wall_s": round(wall, 3),
                    "median_peak_rss_mib": round(rss, 1),
                    "budget_wall_s": wall_budget,
                    "budget_peak_rss_mib": rss_budget,
                    "output_bytes": runs[0]["bytes"],
                    "loadavg_before": load_before,
                    "loadavg_after": os.getloadavg(),
                    "met": met,
                }
            )
    report["ok"] = ok
    print(json.dumps(report, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
