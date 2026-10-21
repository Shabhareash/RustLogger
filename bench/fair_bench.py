#!/usr/bin/env python3
"""Fair Kafka benchmark for RustLogger and Vector.

Both engines:
  - consume the same 1M-line ISO syslog file from Kafka (3 partitions)
  - start from a preloaded topic, so the producer is not in the hot path
  - emit JSON through their file/stdout sink
  - are timed from process start until the transform counter reaches 1M
  - are measured with /proc CPU and RSS samples, not lifetime ps averages

RustLogger uses its live /metrics endpoint. Vector uses the cumulative
counter printed by the blackhole sink, with linear interpolation between
the last two one-second reports, so the 1s print interval does not add a
1s bias. A file sink is attached to Vector so the comparison includes
JSON serialization and output writing, like RustLogger's stdout sink.

Usage:
    python3 bench/fair_bench.py
    python3 bench/fair_bench.py --runs 3 --events 1000000
"""

import argparse
import csv
import datetime as dt
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORK = Path("/tmp/rustlogger_bench")
INPUT = WORK / "auth_1m.log"
TOPIC = "raw-logs"
KAFKA_CONTAINER = "kafka-bench"
KAFKA_BIN = "/opt/bitnami/kafka/bin"
VECTOR_DEFAULT = Path("/tmp/vector/vector-x86_64-unknown-linux-musl/bin/vector")
RUST_BIN = REPO / "target/release/kafka_pipeline"
PRODUCER = REPO / "target/release/kafka_producer"
AUTH_YAML = REPO / "auth.yaml"
VECTOR_CFG = REPO / "vector.yaml"
METRICS_BASE = 29093

BLACKHOLE_RE = re.compile(
    r"^(\S+)\s+INFO.*Collected events\. events=(\d+)"
)


def log(msg):
    print(f"[bench] {msg}", flush=True)


def shell(cmd, timeout=300, check=True):
    proc = subprocess.run(
        cmd, shell=isinstance(cmd, str), capture_output=True, text=True,
        timeout=timeout,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"command failed ({proc.returncode}): {cmd}\n"
            f"stdout: {proc.stdout}\nstderr: {proc.stderr}"
        )
    return proc


def kafka_script(name, *args, timeout=120, check=True):
    return shell(
        ["docker", "exec", KAFKA_CONTAINER, f"{KAFKA_BIN}/{name}", *args],
        timeout=timeout, check=check,
    )


def ensure_kafka():
    proc = shell(["docker", "ps", "--format", "{{.Names}}"], check=False)
    if KAFKA_CONTAINER not in proc.stdout.split():
        log("starting Kafka container")
        shell(["docker", "compose", "up", "-d", "kafka"], cwd=REPO, timeout=300)
        for _ in range(60):
            time.sleep(1)
            if KAFKA_CONTAINER in shell(
                ["docker", "ps", "--format", "{{.Names}}"], check=False
            ).stdout.split():
                break
        else:
            raise RuntimeError("Kafka container did not start")
    for _ in range(60):
        proc = kafka_script("kafka-topics.sh", "--bootstrap-server",
                            "localhost:9092", "--list", check=False)
        if proc.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError("Kafka is not ready")


def recreate_topic():
    kafka_script("kafka-topics.sh", "--bootstrap-server", "localhost:9092",
                 "--delete", "--topic", TOPIC, check=False)
    time.sleep(0.5)
    for _ in range(30):
        proc = kafka_script(
            "kafka-topics.sh", "--bootstrap-server", "localhost:9092",
            "--create", "--topic", TOPIC, "--partitions", "3",
            "--replication-factor", "1", check=False,
        )
        if proc.returncode == 0:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError(f"could not create topic {TOPIC}")
    time.sleep(0.3)


def topic_end_offsets():
    proc = kafka_script(
        "kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092",
        "--topic", TOPIC, check=False,
    )
    total = 0
    for line in proc.stdout.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[2].isdigit():
            total += int(parts[2])
    return total


def preload(events, vector_bin):
    log(f"producing {events:,} events into {TOPIC}")
    input_lines = max(count_lines([INPUT]), 1)
    multiplier = max(events // input_lines, 1)
    proc = shell(
        [str(PRODUCER), str(INPUT), TOPIC, str(multiplier), "localhost:9092"],
        timeout=600,
    )
    producer_eps = 0
    match = re.search(r"~([\d,]+) EPS", proc.stdout)
    if match:
        producer_eps = int(match.group(1).replace(",", ""))
    for _ in range(300):
        if topic_end_offsets() >= events:
            return producer_eps
        time.sleep(0.2)
    raise RuntimeError("topic did not reach the expected offset")


class Sampler(threading.Thread):
    def __init__(self, pids, interval=0.1):
        super().__init__(daemon=True)
        self.pids = list(pids)
        self.interval = interval
        self.stop_event = threading.Event()
        self.samples = []
        self.t0 = None
        self.cpu0 = None

    @staticmethod
    def cpu_seconds(pid):
        try:
            fields = Path(f"/proc/{pid}/stat").read_text().split()
            return (int(fields[13]) + int(fields[14])) / os.sysconf("SC_CLK_TCK")
        except (FileNotFoundError, ProcessLookupError, IndexError, ValueError):
            return 0.0

    @staticmethod
    def rss_kb(pid):
        try:
            for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
        except (FileNotFoundError, ProcessLookupError, ValueError):
            pass
        return 0

    def total_cpu(self):
        return sum(self.cpu_seconds(p) for p in self.pids)

    def total_rss(self):
        return sum(self.rss_kb(p) for p in self.pids)

    def run(self):
        self.t0 = time.monotonic()
        self.cpu0 = self.total_cpu()
        while not self.stop_event.is_set():
            now = time.monotonic()
            self.samples.append((now - self.t0, self.total_cpu(), self.total_rss()))
            self.stop_event.wait(self.interval)

    def stop(self):
        self.stop_event.set()
        self.join(timeout=2)

    def summary(self):
        if not self.samples:
            return {"duration_s": 0, "avg_cpu_pct": 0, "avg_rss_mb": 0,
                    "peak_rss_mb": 0, "cpu_seconds": 0}
        end_t, end_cpu, _ = self.samples[-1]
        duration = max(end_t, 1e-9)
        cpu_seconds = max(end_cpu - self.cpu0, 0.0)
        rss = [s[2] / 1024.0 for s in self.samples]
        return {
            "duration_s": round(duration, 3),
            "cpu_seconds": round(cpu_seconds, 3),
            "avg_cpu_pct": round(100.0 * cpu_seconds / duration, 1),
            "avg_rss_mb": round(sum(rss) / len(rss), 1),
            "peak_rss_mb": round(max(rss), 1),
        }


def grep_count(pattern, files):
    total = 0
    for path in files:
        if not path.exists():
            continue
        proc = subprocess.run(["grep", "-c", pattern, str(path)],
                              capture_output=True, text=True)
        if proc.stdout.strip():
            total += int(proc.stdout.strip())
    return total


def count_lines(files):
    total = 0
    for path in files:
        if not path.exists():
            continue
        proc = subprocess.run(["wc", "-l", str(path)], capture_output=True, text=True)
        if proc.stdout.strip():
            total += int(proc.stdout.split()[0])
    return total


def wait_lines(files, target, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        total = count_lines(files)
        if total >= target:
            return total
        time.sleep(0.5)
    return count_lines(files)


def kill_all(procs, grace=3.0):
    for proc in procs:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
    deadline = time.time() + grace
    for proc in procs:
        while proc.poll() is None and time.time() < deadline:
            time.sleep(0.05)
        if proc.poll() is None:
            proc.kill()
    for proc in procs:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def prepare(args):
    WORK.mkdir(parents=True, exist_ok=True)
    existing_lines = count_lines([INPUT]) if INPUT.exists() else 0
    if existing_lines != args.events:
        log(f"building {args.events:,}-line ISO input from OpenSSH_200k.log")
        shell([sys.executable, str(REPO / "bench" / "prepare_input.py"),
               str(REPO / "OpenSSH_200k.log"), str(INPUT),
               "--lines", str(args.events)], timeout=300)
    if not args.skip_build and (not RUST_BIN.exists() or not PRODUCER.exists()):
        log("building RustLogger")
        cargo = shutil.which("cargo")
        if cargo is None:
            candidate = Path.home() / ".cargo" / "bin" / "cargo"
            cargo = str(candidate) if candidate.exists() else None
        if cargo is None:
            raise SystemExit("cargo not found; build the binaries first")
        shell([cargo, "build", "--release", "--bin", "kafka_pipeline",
               "--bin", "kafka_producer"], cwd=REPO, timeout=900)
    if not args.vector.exists():
        raise SystemExit(f"Vector binary not found: {args.vector}")
    ensure_kafka()


def prepare_topic(events, vector_bin):
    recreate_topic()
    return preload(events, vector_bin)


def run_rust(events, run_id):
    base = WORK / f"rust_{run_id}"
    outputs = [Path(f"{base}_{p}.ndjson") for p in range(3)]
    logs = [Path(f"{base}_{p}.log") for p in range(3)]
    for path in outputs + logs:
        path.unlink(missing_ok=True)

    group = f"rust-fair-{run_id}-{int(time.time())}"
    env_base = os.environ.copy()
    env_base.update({
        "KAFKA_BROKER": "localhost:9092",
        "KAFKA_TOPIC": TOPIC,
        "ES_HOST": "http://127.0.0.1:9",
        "WRITE_TO_STDOUT": "1",
        "DAEMON": "1",
        "RAYON_NUM_THREADS": "4",
        "TOKIO_WORKER_THREADS": "2",
    })

    log(f"rust run {run_id}: starting 3 consumers")
    t0 = time.time()
    procs = []
    files = []
    for part in range(3):
        env = env_base.copy()
        env["KAFKA_PARTITION"] = str(part)
        env["KAFKA_GROUP"] = f"{group}-{part}"
        env["METRICS_PORT"] = str(METRICS_BASE + part)
        out = outputs[part].open("wb")
        err = logs[part].open("w")
        files.extend([out, err])
        procs.append(subprocess.Popen(
            [str(RUST_BIN), str(AUTH_YAML)],
            stdout=out, stderr=err, env=env,
        ))

    sampler = Sampler([p.pid for p in procs])
    sampler.start()

    done_at = None
    deadline = time.time() + 300
    while time.time() < deadline:
        total = 0
        ok = True
        for part in range(3):
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{METRICS_BASE + part}/metrics",
                    timeout=0.3,
                ) as resp:
                    total += int(json.loads(resp.read())["total_consumed"])
            except Exception:
                ok = False
                break
        if ok and total >= events:
            done_at = time.time()
            break
        if all(p.poll() is not None for p in procs):
            break
        time.sleep(0.05)

    if done_at is None:
        done_at = time.time()
    sampler.stop()
    kill_all(procs)
    for fh in files:
        fh.close()

    elapsed = max(done_at - t0, 1e-9)
    action_lines = grep_count('^{"index":{"_index"', outputs)
    matched = grep_count('"matched":true', outputs)
    specialized = grep_count('"_parse_stage":"specialized"', outputs)
    total_lines = count_lines(outputs)
    doc_lines = max(total_lines - action_lines, 0)
    output_mb = sum(p.stat().st_size for p in outputs if p.exists()) / 1e6

    stats = sampler.summary()
    for path in outputs:
        path.unlink(missing_ok=True)
    return {
        "engine": "rustlogger",
        "run": run_id,
        "events": action_lines,
        "elapsed_s": round(elapsed, 4),
        "eps": round(action_lines / elapsed) if action_lines else 0,
        "cpu_pct": stats["avg_cpu_pct"],
        "cpu_seconds": stats["cpu_seconds"],
        "avg_rss_mb": stats["avg_rss_mb"],
        "peak_rss_mb": stats["peak_rss_mb"],
        "matched_pct": round(100.0 * matched / action_lines, 1) if action_lines else 0.0,
        "specialized_pct": round(100.0 * specialized / action_lines, 1) if action_lines else 0.0,
        "output_mb": round(output_mb, 1),
        "doc_lines": doc_lines,
        "note": "",
    }


def vector_elapsed_from_log(log_path, events, t0):
    entries = []
    with log_path.open("r", errors="replace") as fh:
        for line in fh:
            match = BLACKHOLE_RE.search(line)
            if not match:
                continue
            ts = dt.datetime.fromisoformat(
                match.group(1).replace("Z", "+00:00")
            ).timestamp()
            entries.append((ts, int(match.group(2))))

    for i, (ts, count) in enumerate(entries):
        if count < events:
            continue
        if i == 0:
            return ts - t0, count
        prev_ts, prev_count = entries[i - 1]
        if count > prev_count:
            frac = (events - prev_count) / (count - prev_count)
            done = prev_ts + frac * (ts - prev_ts)
        else:
            done = ts
        return done - t0, count
    return None, entries[-1][1] if entries else 0


def run_vector(events, run_id, vector_bin):
    cfg = WORK / f"vector_{run_id}.yaml"
    text = VECTOR_CFG.read_text()
    text = re.sub(r'group_id: ".*"', f'group_id: "vector-fair-{run_id}-{int(time.time())}"', text)
    text = text.replace('/tmp/rustlogger_bench/vector_out.ndjson', str(WORK / 'vector_out.ndjson'))
    text = text.replace('/tmp/rustlogger_bench/vector_data', str(WORK / 'vector_data'))
    cfg.write_text(text)

    out = WORK / "vector_out.ndjson"
    out.unlink(missing_ok=True)
    log_file = WORK / f"vector_{run_id}.log"
    log_file.unlink(missing_ok=True)
    shutil.rmtree(WORK / "vector_data", ignore_errors=True)

    env = os.environ.copy()
    env["VECTOR_LOG"] = "info"
    env["VECTOR_THREADS"] = "12"

    log(f"vector run {run_id}: starting Vector")
    t0 = time.time()
    proc = subprocess.Popen(
        [str(vector_bin), "--config", str(cfg), "--threads", "12"],
        stdout=subprocess.DEVNULL, stderr=log_file.open("w"), env=env,
    )
    sampler = Sampler([proc.pid])
    sampler.start()

    elapsed = None
    last_count = 0
    deadline = time.time() + 300
    while time.time() < deadline:
        if proc.poll() is not None:
            break
        elapsed, last_count = vector_elapsed_from_log(log_file, events, t0)
        if elapsed is not None:
            break
        time.sleep(0.2)

    if elapsed is None:
        elapsed, last_count = vector_elapsed_from_log(log_file, events, t0)
    if elapsed is None:
        elapsed = time.time() - t0

    sampler.stop()
    kill_all([proc])

    # The file sink has a 5000-event buffer. Give it a moment to flush,
    # then verify the output. Timing is based on the transform counter.
    wait_lines([out], events, timeout=15)
    file_lines = count_lines([out])
    matched = grep_count('"matched":true', [out]) if out.exists() else 0
    specialized = grep_count('"_parse_stage":"specialized"', [out]) if out.exists() else 0
    output_mb = out.stat().st_size / 1e6 if out.exists() else 0.0
    stats = sampler.summary()
    out.unlink(missing_ok=True)

    return {
        "engine": "vector",
        "run": run_id,
        "events": last_count,
        "elapsed_s": round(max(elapsed, 1e-9), 4),
        "eps": round(events / max(elapsed, 1e-9), 0),
        "cpu_pct": stats["avg_cpu_pct"],
        "cpu_seconds": stats["cpu_seconds"],
        "avg_rss_mb": stats["avg_rss_mb"],
        "peak_rss_mb": stats["peak_rss_mb"],
        "matched_pct": round(100.0 * matched / file_lines, 1) if file_lines else 0.0,
        "specialized_pct": round(100.0 * specialized / file_lines, 1) if file_lines else 0.0,
        "output_mb": round(output_mb, 1),
        "doc_lines": file_lines,
        "note": "" if file_lines >= events else f"file short: {file_lines}/{events}",
    }


def fmt_results(results):
    header = (
        "| engine | run | events | wall s | EPS | CPU % | RSS MB | peak RSS | "
        "matched % | specialized % | output MB |\n"
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    )
    lines = [header]
    for r in results:
        lines.append(
            f"| {r['engine']} | {r['run']} | {r['events']:,} | "
            f"{r['elapsed_s']:.3f} | {r['eps']:,} | {r['cpu_pct']:.1f} | "
            f"{r['avg_rss_mb']:.1f} | {r['peak_rss_mb']:.1f} | "
            f"{r['matched_pct']:.1f} | {r['specialized_pct']:.1f} | {r['output_mb']:.0f} |"
        )
    return "\n".join(lines)


def summarize(results):
    lines = []
    for engine in ("rustlogger", "vector"):
        rows = [r for r in results if r["engine"] == engine]
        if not rows:
            continue
        eps = sorted(r["eps"] for r in rows)
        med = eps[len(eps) // 2]
        lines.append(
            f"- **{engine}**: median {med:,} EPS, "
            f"range {eps[0]:,} to {eps[-1]:,} EPS, "
            f"median CPU {sorted(r['cpu_pct'] for r in rows)[len(rows)//2]:.1f}%, "
            f"median RSS {sorted(r['avg_rss_mb'] for r in rows)[len(rows)//2]:.1f} MB"
        )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--events", type=int, default=1_000_000)
    parser.add_argument("--vector", type=Path, default=VECTOR_DEFAULT)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--keep-topic", action="store_true")
    parser.add_argument("--work-dir", type=Path, default=Path("/tmp/rustlogger_bench"))
    args = parser.parse_args()

    global WORK, INPUT
    WORK = args.work_dir
    INPUT = WORK / "auth_1m.log"
    WORK.mkdir(parents=True, exist_ok=True)

    prepare(args)
    results = []
    try:
        for run in range(1, args.runs + 1):
            order = ["rust", "vector"] if run % 2 == 1 else ["vector", "rust"]
            for engine in order:
                producer_eps = prepare_topic(args.events, args.vector)
                log(f"run {run} {engine}: producer {producer_eps:,} EPS")
                if engine == "rust":
                    result = run_rust(args.events, run)
                else:
                    result = run_vector(args.events, run, args.vector)
                result["producer_eps"] = producer_eps
                results.append(result)
                log(json.dumps(result))
    finally:
        if not args.keep_topic:
            pass

    table = fmt_results(results)
    summary = summarize(results)
    print()
    print(table)
    print()
    print(summary)

    out_csv = WORK / "results.csv"
    with out_csv.open("w") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)

    out_md = WORK / "RESULTS.md"
    out_md.write_text(
        "# Fair Kafka benchmark\n\n"
        f"Input: `{INPUT}` ({args.events:,} events)\n\n"
        f"Hardware: {os.cpu_count()} logical CPUs, "
        f"{os.uname().sysname} {os.uname().release}\n\n"
        f"Vector: {args.vector}\n\n"
        "Both engines consume the same preloaded 3-partition topic, parse the "
        "same ISO syslog input, serialize JSON to a file/stdout sink, and are "
        "timed until their transform counter reaches the expected event count. "
        "CPU and RSS are sampled from /proc every 100 ms.\n\n"
        + table + "\n\n" + summary + "\n"
    )
    log(f"wrote {out_csv} and {out_md}")


if __name__ == "__main__":
    main()
