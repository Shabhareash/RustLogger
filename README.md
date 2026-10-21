# RustLogger

A high-performance log ingestion engine written in Rust. Built as a specialized alternative to Logstash/Vector edge agents in high-throughput SIEM pipelines.

Built from scratch by a single developer. The fair end-to-end benchmark in this repository is **~355,000 events/sec** on a 1M-event Kafka workload with JSON output, about **6.25x** Vector with a corrected Vector config.

## Performance

### Fair Head-to-Head: RustLogger vs Vector

Both engines consumed the same preloaded 1M-event, 3-partition Kafka topic, parsed the same ISO syslog input, and wrote JSON. Vector's config was corrected to use a cheap classifier check and at most one message regex per event, instead of executing all 16 `parse_regex` calls on every event.

| Metric | RustLogger | Vector | Ratio |
| :--- | ---: | ---: | ---: |
| **Sustained EPS (median, 3 runs)** | **354,586** | **56,746** | **6.25x** |
| **CPU Efficiency** | **164,389 EPS/core** | **18,382 EPS/core** | **8.9x** |
| **Total RSS (median)** | **727 MB** | **359 MB** | **Vector uses 2.0x less** |
| **Peak RSS** | **~996 MB** | **~449 MB** | **Vector uses 2.2x less** |
| **Specialized parse rate** | **74.2%** | **82.3%** | **Vector classifies more** |

> **Hardware:** Intel Core Ultra 5 235U (12C/14T), 15 GB RAM, Linux 7.2.9.
> **Workload:** 1,000,000 ISO syslog auth lines derived from `OpenSSH_200k.log`, preloaded into a 3-partition Kafka topic.
> **Topology:** RustLogger runs 3 consumers (one per partition, 4 Rayon threads each). Vector runs 1 daemon with 12 threads.
> **Sinks:** RustLogger writes NDJSON with ES bulk action lines to stdout; Vector writes JSON through a file sink and a blackhole counter. Both write to tmpfs (`/tmp`), so this measures parsing, serialization, and memory-backed output, not physical disk I/O.
> **Reproduce:** `python3 bench/fair_bench.py --runs 3`. Raw table and methodology: `bench/RESULTS.md`.

The earlier table (1,349,344 vs ~90,000, 14.9x) is not a fair comparison. It used a Vector config that ran all 16 `parse_regex` calls on every event because the `if` blocks did not short-circuit, and it compared Vector's blackhole sink against RustLogger writing to `/dev/null`. The corrected rerun above is the number to use. The 1.35M figure is a parse/serialize-to-`/dev/null` measurement, not an end-to-end result.

The RustLogger Kafka binary currently emits NDJSON to stdout. There is no live Elasticsearch bulk sender in this repository, so the benchmark uses the stdout/file path.

### Benchmark Architecture

```
┌─────────────────┐     ┌───────────────┐     ┌───────────────────────┐
│  Native Rust    │────▶│ Kafka Broker  │───▶ │  3× RustLogger        │
│  Producer       │     │ (3 partitions)│     │  Daemon Consumers     │
│  (rdkafka)      │     └───────────────┘     │                       │
└─────────────────┘                           │  ┌──────────────────┐ │
                                              │  │ RawBatch (zero-  │ │
                                              │  │ copy byte buffer)│ │
                                              │  └────────┬─────────┘ │
                                              │           │           │
                                              │  ┌────────▼─────────┐ │
                                              │  │ Rayon par_iter   │ │
                                              │  │ (parse+serialize)│ │
                                              │  └────────┬─────────┘ │
                                              │           │           │
                                              │  ┌────────▼─────────┐ │
                                              │  │ JSON output      │ │
                                              │  │ File / stdout    │ │
                                              │  └──────────────────┘ │
                                              └───────────────────────┘
```

## Architecture

RustLogger uses a four-stage, YAML-driven parsing pipeline with zero regex on the hot path:

### Stage 1: SIMD Classification (Aho-Corasick)
Every incoming log line is classified into an event type using a compiled [Aho-Corasick](https://en.wikipedia.org/wiki/Aho%E2%80%93Corasick_algorithm) automaton built from YAML-defined signature needles. This runs at memory bandwidth speed via SIMD vectorization — no regex, no backtracking.

### Stage 2: State-Machine & Dissect Parsing (Zero Regex)
Classified events are parsed using one of two strategies:
- **State-Machine Parser:** Byte-level token extraction using `memchr` SIMD search with word-boundary awareness. Handles quoted strings, bracketed values, and key-value pairs.
- **Dissect Parser:** Logstash-compatible `%{field:type}` patterns compiled to typed token captures. Supports `notspace`, `word`, `int`, `ip`, and `data` types.

Both strategies produce structured [ECS (Elastic Common Schema)](https://www.elastic.co/guide/en/ecs/current/index.html) documents with zero regex overhead.

### Stage 3: Regex Fallback
Unclassified lines fall through to a configurable regex engine (PCRE2 via JIT). This is the slow path and is only hit for unknown log formats.

### Stage 4: Raw Ingestion (Never Drop)
Every event — matched or unmatched — is guaranteed to be ingested with baseline ECS fields (`event.original`, `message`, `host.name`, `process.name`, `@timestamp`). No silent drops.

## Zero-Copy Ingestion Pipeline

The Kafka consumer uses a custom `RawBatch` contiguous byte buffer that eliminates per-message heap allocation in the ingestion loop:

```
Kafka poll() → raw bytes memcpy into RawBatch (no UTF-8 validation)
                    │
                    ▼
            std::thread::spawn (dedicated OS thread, not Tokio)
                    │
                    ▼
            Rayon par_iter across all cores:
              ├─ UTF-8 validation (distributed)
              ├─ Pipeline parse (SIMD classify → state-machine extract)
              ├─ ECS enrichment (static field insertion via Cow<'static, str>)
              └─ serde_json serialization (parallel, per-worker buffer)
                    │
                    ▼
            Concatenate NDJSON fragments → Sink (file / stdout)
```

### Key Design Decisions
- **`FieldVec` over `BTreeMap`/`HashMap`:** Documents use a flat `Vec<(Cow<'static, str>, FieldValue)>` instead of tree/hash structures. For 15-25 field documents, linear scan beats hash lookup due to cache locality and zero node allocations.
- **`FieldValue` over `serde_json::Value`:** A compact enum (`StaticStr | Str | Int | Float | Bool | Null | Array`) that avoids the heap allocation overhead of `serde_json::Value`'s internal `Box<str>`.
- **`Cow<'static, str>` keys:** 95%+ of ECS field names are compile-time constants. `Cow::Borrowed` makes them zero-cost. Only dynamic YAML keys allocate.
- **`mimalloc` allocator:** Replaces the system allocator for optimized multi-threaded allocation patterns.

## Usage

### File Mode (Parse + Write to JSON)
```bash
cargo build --release
./target/release/my_scanner auth.log auth.yaml
```

### Kafka Pipeline (Daemon Mode)
```bash
cargo build --release

# Single consumer
KAFKA_BROKER="localhost:9092" \
KAFKA_TOPIC="raw-logs" \
KAFKA_GROUP="rustlogger" \
  ./target/release/kafka_pipeline auth.yaml

# Daemon mode with HTTP metrics
DAEMON=1 METRICS_PORT=9093 \
KAFKA_BROKER="localhost:9092" \
KAFKA_TOPIC="raw-logs" \
KAFKA_PARTITION=0 \
  ./target/release/kafka_pipeline auth.yaml
```

### Native Kafka Producer (Benchmarking)
```bash
./target/release/kafka_producer auth_1m.log raw-logs 10 localhost:9092
```

### Benchmarks
```bash
# Peak throughput (parse-only, 3 consumers)
./peak_bench.sh 5

# Sustained steady-state (continuous producer, SSD sink, 30s window)
./rustlogger.sh 30
```

## Environment Variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KAFKA_BROKER` | `localhost:9092` | Kafka bootstrap server |
| `KAFKA_TOPIC` | `raw-logs` | Topic to consume |
| `KAFKA_GROUP` | `rustlogger` | Consumer group ID |
| `KAFKA_PARTITION` | *(auto)* | Pin to specific partition |
| `ES_HOST` | `http://127.0.0.1:9200` | Elasticsearch endpoint |
| `ES_INDEX` | `logs-ecs` | Target index |
| `BULK_SIZE` | `5000` | Documents per bulk request |
| `DAEMON` | *(unset)* | Set to `1` for continuous mode |
| `METRICS_PORT` | `9090` | HTTP metrics endpoint (daemon mode) |
| `WRITE_TO_STDOUT` | *(unset)* | Set to output parsed JSON to stdout |
| `RAYON_NUM_THREADS` | *(auto)* | Worker threads for parallel parsing |
| `TOKIO_WORKER_THREADS` | *(auto)* | Async I/O threads |

## YAML Pipeline Configuration

Pipeline behavior is fully driven by YAML configs (e.g., `auth.yaml`, `ssh.yaml`):

```yaml
version: 1
log_family: auth

pipeline:
  classifier:
    strategy: aho-corasick
    fallback: generic

  signatures:
    - id: ssh_failed_password
      contains: ["Failed password"]
    - id: ssh_accepted
      contains: ["Accepted password", "Accepted publickey"]

  specialized_parsers:
    ssh_failed_password:
      strategy: dissect
      patterns:
        - "Failed %{?method} for %{?prefix}%{user.name:notspace} from %{source.ip:ip} port %{source.port:int}"
      ecs:
        static:
          event.category: authentication
          event.action: ssh_login
          event.outcome: failure
```

## Build

```bash
# Requires: Rust toolchain, librdkafka (auto-built via cmake)
cargo build --release
```

## Project Structure

```
src/
├── lib.rs             # FieldVec, FieldValue — core zero-alloc document types
├── pipeline.rs        # Four-stage parsing pipeline (SIMD → state-machine → regex → raw)
├── dissect.rs         # Logstash-compatible dissect pattern compiler & executor
├── yaml_config.rs     # YAML configuration deserializer
├── extraction.rs      # Legacy grok field extraction + ES bulk sender
├── processors.rs      # Post-processing transforms (timestamp normalization, defaults)
├── log_types.rs       # Auto-detection of log formats (syslog, apache, auth, etc.)
├── main.rs            # File-mode entry point
└── bin/
    ├── kafka_pipeline.rs  # Kafka daemon consumer (zero-copy RawBatch + Rayon)
    └── kafka_producer.rs  # Native rdkafka producer for benchmarking
```

## License

MIT
