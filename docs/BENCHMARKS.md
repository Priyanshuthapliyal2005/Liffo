# Direct-backend benchmark

The [November 2026 reference report](../reports/reference-cpu-gpu-2026-11-06.md)
records CPU and NVIDIA L4 inference from the benchmark client directly to two
`llama-server` processes. Use the [gateway smoke run](REAL_INFERENCE.md) to
exercise Liffo's HTTP path.

## Workload and metrics

The [reference config](../configs/bench-reference.yaml) contains the three
synthetic prompts whose hashes match all retained request rows. Each cell cycles
through them for 32 measured streaming requests, 11 short, 11 explanatory and
10 code requests. At concurrency 1, 2, 4 and 8, the warmup counts are 3, 2, 4 and
8, respectively. The historical config specifies a 64-token output limit and
temperature zero. The retained rows agree with that limit but do not record the
effective request parameters.

- **TTFT** measures time from client request start to the first nonempty
  streamed content delta. Latency measures the full request.
- **Per-request decode rate** is `(output tokens − 1) / (latency − TTFT)` when
  the token count and timings permit it, with a fallback to tokens divided by
  latency. Time in these denominators is in seconds.
- **Wall output rate** is the sum of successful output tokens divided by the
  measured cell's elapsed wall time, excluding warmup.
- **Sampled maxima** reflect the resource samples taken during the run. Check
  the server log for layer placement.

## Repeat the setup

Build Liffo and use llama.cpp b12177 from
[commit `6c01da627`](https://github.com/ggml-org/llama.cpp/commit/6c01da6278ab6c69ed67d9124806b50d2b299ba7).
Use the
[Qwen Q4_K_M GGUF](https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct-GGUF/blob/6dd44a1fb35d11b5d1b28902876ce3cc9e882d0e/qwen2.5-0.5b-instruct-q4_k_m.gguf)
and check its SHA-256 against the report. Record the hardware, runtime, driver,
effective launch arguments and file digest with new results.

Run each mode separately. In a worker terminal, set `MODEL` to the GGUF path
and start the CPU build or, for the GPU run, the CUDA build:

```sh
# CPU on port 18080
llama-server -m "$MODEL" --alias qwen --host 127.0.0.1 --port 18080 \
  -t 8 -c 8192 -np 8 -ngl 0 --metrics -v

# GPU on port 18081, using the CUDA build
llama-server -m "$MODEL" --alias qwen --host 127.0.0.1 --port 18081 \
  -t 8 -c 8192 -np 8 -ngl 999 --metrics -v
```

The commands reconstruct the recorded settings. The original full command
lines and benchmark executable digest were not retained. Stop the CPU process
before starting the GPU run. For the GPU run confirm `offloaded 25/25 layers to GPU`
in the server log.

From the Liffo checkout, set `mode=gpu port=18081` for the GPU matrix:

```sh
go build -trimpath -o bin/liffo ./cmd/liffo
mode=cpu port=18080
base="http://127.0.0.1:$port"
curl -fsS "$base/health" || exit 1
run_root="$(mktemp -d)"
for concurrency in 1 2 4 8; do
  cell="$run_root/$mode-c$concurrency"
  mkdir -p "$cell"
  ./bin/liffo bench --config configs/bench-reference.yaml --base-url "$base" \
    --mode "$mode" --concurrency "$concurrency" --warmup 1 \
    --total-requests 32 --out "$cell/raw" > "$cell/bench.log" || exit 1
  cat "$cell/bench.log"
  grep -q 'n=32 errors=0' "$cell/bench.log" || exit 1
  ./bin/liffo report --raw "$cell/raw" --out "$cell/report" \
    --target "direct llama-server, $mode" || exit 1
done
```

`--mode` labels the run and controls GPU sampling. The worker launch controls
offload. Pass `--server-pid` to sample the worker's memory. The report command
reads request rows and does not have the cell's elapsed time. To estimate the
wall output rate for a new run, sum successful `tokens_out` from JSONL and
divide by the rounded `wall` value from `bench` stdout in seconds. The curated
reference uses the historical elapsed time at its original precision.
The historical InferMesh client also sampled resources across the measured
phase. Resource maxima from the current Liffo client may differ in coverage.
