# Direct-backend benchmark

The [November 2026 reference run](../reports/reference-cpu-gpu-2026-11-06.md)
used the CPU and CUDA `llama-server` endpoints directly. The
[gateway quickstart](../README.md#quickstart) uses `liffo bench` with a gateway endpoint to exercise request forwarding. Its timings
include that additional hop.

## Workload and metrics

The [reference config](../configs/bench-reference.yaml) contains the three
synthetic prompts used in the reference run, verified against the retained
request hashes. Each cell has 32 measured streaming requests: 11 short,
11 explanatory, and 10 code requests. At concurrency 1, 2, 4 and 8, the run
warmed up with 3, 2, 4 and 8 requests, respectively.

- **TTFT** measures time from client request start to the first nonempty
  streamed content delta. Latency measures the full request.
- **Per-request decode rate** is `(output tokens − 1) / (latency − TTFT)` when
  the token count and timings permit it, with a fallback to tokens divided by
  latency. Time in these denominators is in seconds.
- **Aggregate output throughput** is the total number of tokens from successful
  requests divided by the cell’s elapsed wall time, excluding warmup.
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

Stop the CPU process before starting the GPU run. Confirm
`offloaded 25/25 layers to GPU` in the GPU server log.

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

`--mode` labels the run and controls GPU sampling. Use the worker launch flags
to set offload. Pass `--server-pid` to sample worker memory.

## Reproduction limits

The recipe reconstructs the retained settings. The original full command lines
and client executable digest are unavailable. The historical config specified
64 output tokens at temperature zero. Retained rows agree with the token cap
and omit the effective request parameters.

Estimate aggregate output throughput for a new run by summing `tokens_out` in
successful JSONL rows and dividing by the rounded `wall` seconds printed by `bench`. The report uses request rows
only. The reference aggregate uses the historical cell durations at full
precision. InferMesh also sampled resources across the entire measured phase,
so its maxima may differ from those sampled by the current Liffo client.
