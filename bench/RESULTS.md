# Fair Kafka benchmark results

Date: 2026-10-21  
RustLogger commit: `520a787`  
Vector: `0.60.0` (`x86_64-unknown-linux-musl`)  
Hardware: Intel Core Ultra 5 235U, 12 cores / 14 threads, 15 GB RAM, Linux 7.2.9

## What was fixed

The committed `vector.yaml` executed 16 `parse_regex` calls on every event.
Each call was a separate top-level VRL statement, so the `if e == null`
blocks only guarded field assignment and never skipped the later regexes.
The benchmark document claimed a "cascade through 9+ if/else" block, but
there was no `else if` cascade.

The corrected config:

- parses the syslog header once,
- uses one cheap `contains` classifier check per event,
- runs at most one message regex for the matched event type,
- writes the same ECS-style fields as before.

Both engines now write JSON. RustLogger writes NDJSON with Elasticsearch
bulk action lines to stdout. Vector writes JSON through a file sink and
uses a second blackhole sink only as a cumulative completion counter.
The output files are under `/tmp`, which is tmpfs on this host, so this
measures parsing, serialization, and memory-backed output, not physical
disk I/O.

## Method

- Input: 1,000,000 ISO syslog auth lines generated from
  `OpenSSH_200k.log` by `bench/prepare_input.py`.
- Kafka: 3 partitions, replication factor 1. The topic is recreated and
  preloaded before every engine run, so the producer is not in the hot
  path.
- RustLogger: 3 consumers, one pinned to each partition, 4 Rayon threads
  and 2 Tokio workers each.
- Vector: 1 daemon, `--threads 12`.
- Timing: from process start until the transform counter reaches
  1,000,000 events. RustLogger uses its live `/metrics` endpoint. Vector
  uses the cumulative blackhole counter, with linear interpolation
  between the last two one-second reports.
- CPU and RSS: sampled from `/proc` every 100 ms, not `ps` lifetime
  averages.
- Runs: 3, with the engine order alternated.

## Raw runs

| engine | run | events | wall s | EPS | CPU % | RSS MB | peak RSS | matched % | specialized % | output MB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| rustlogger | 1 | 1,000,000 | 2.816 | 355,114 | 218.9 | 700.8 | 948.5 | 100.0 | 74.2 | 765 |
| vector | 1 | 1,000,000 | 17.622 | 56,746 | 304.5 | 357.1 | 448.5 | 82.3 | 82.3 | 641 |
| vector | 2 | 1,000,000 | 17.684 | 56,548 | 308.7 | 359.4 | 446.1 | 82.3 | 82.3 | 641 |
| rustlogger | 2 | 1,000,000 | 2.820 | 354,586 | 215.7 | 734.5 | 1,050.3 | 100.0 | 74.2 | 765 |
| rustlogger | 3 | 1,000,000 | 2.874 | 347,935 | 215.2 | 727.0 | 995.8 | 100.0 | 74.2 | 765 |
| vector | 3 | 1,000,000 | 17.609 | 56,790 | 315.3 | 364.6 | 455.7 | 82.3 | 82.3 | 641 |

## Medians

| metric | RustLogger | Vector | ratio |
|---|---:|---:|---:|
| Sustained EPS | 354,586 | 56,746 | 6.25x |
| CPU used | 215.7% | 308.7% | 0.70x |
| EPS per used core | 164,389 | 18,382 | 8.9x |
| Total RSS | 727.0 MB | 359.4 MB | 2.02x more |
| Peak RSS | 995.8 MB | 448.5 MB | 2.22x more |
| Output written | 765 MB | 641 MB | 1.19x more |
| Specialized parse rate | 74.2% | 82.3% | lower |

RustLogger is faster and uses fewer cores, but it uses more total memory
because it runs 3 consumer processes. Per process, RustLogger is about
242 MB versus Vector's 359 MB.

## Interpretation

- The fair end-to-end ratio is about **6.25x EPS**, not the 14.9x,
  26x, or 45x figures in the earlier write-up.
- The earlier RustLogger number used a Vector config that ran all 15
  message regexes on every event, and compared that to RustLogger
  writing to `/dev/null`. That is not an engine-to-engine comparison.
- The 1,349,344 EPS figure is not reproduced. It was a parse plus
  serialize-to-`/dev/null` measurement. With JSON output, RustLogger
  sustains about 355,000 EPS on this host.
- The memory claim in the old README is not supported. Total and peak
  RSS are about 2x and 2.2x higher for RustLogger. The old "1.2x less
  memory" row is wrong for the 3-consumer deployment.
- The "matched" number for RustLogger is 100% only because the generic
  syslog regex fallback matches almost every line. The specialized rate
  is 74.2%. Vector classifies 82.3% of this input. The generic fallback
  also sets `process.name` to the whole header prefix, so a high
  "matched" percentage is not an accuracy metric.

## Reproduce

```bash
python3 bench/fair_bench.py --runs 3
```

The script builds the input if needed, starts Kafka with
`docker compose up -d kafka` if it is not running, preloads each topic,
runs both engines, samples CPU and RSS, and writes `results.csv` and
`RESULTS.md` under `/tmp/rustlogger_bench/`.
