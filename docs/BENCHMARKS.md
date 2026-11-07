# Reference benchmark

These aggregate results are included to demonstrate Liffo's benchmark output,
not to claim universal model or hardware performance.

## Environment

- Linux x86_64
- AMD EPYC 7R13, 8 logical CPUs, 30 GiB RAM
- NVIDIA L4, 23,034 MiB, driver 580.178.04
- llama.cpp b12177 (`6c01da627`), CUDA 12.8 for the GPU run
- Qwen2.5-0.5B-Instruct Q4_K_M, context 2048
- CPU: 8 threads, 8 server slots, `-ngl 0`
- GPU: full 25/25-layer offload, 8 server slots
- 32 measured requests per concurrency cell; identical prompts and token limits
- CPU and GPU matrices ran sequentially on the same allocation
- 128/128 successful requests in each matrix; zero SSE parse errors

The GPU server log recorded `offloaded 25/25 layers to GPU`. Peak sampled GPU
memory was 734 MiB and peak sampled utilization was 88%.

## Results

| Mode | Concurrency | TTFT p50 | Latency p50 | Per-request decode p50 | Wall output rate |
|---|---:|---:|---:|---:|---:|
| CPU | 1 | 21.2 ms | 1,031.8 ms | 55.1 tok/s | 49.4 tok/s |
| CPU | 2 | 36.3 ms | 1,652.7 ms | 33.7 tok/s | 58.9 tok/s |
| CPU | 4 | 117.3 ms | 3,505.9 ms | 16.5 tok/s | 59.7 tok/s |
| CPU | 8 | 293.5 ms | 9,130.8 ms | 5.4 tok/s | 40.4 tok/s |
| GPU | 1 | 5.5 ms | 179.6 ms | 360.3 tok/s | 220.5 tok/s |
| GPU | 2 | 6.7 ms | 201.5 ms | 317.5 tok/s | 396.2 tok/s |
| GPU | 4 | 11.1 ms | 299.3 ms | 207.5 tok/s | 565.2 tok/s |
| GPU | 8 | 16.9 ms | 455.5 ms | 124.8 tok/s | 673.3 tok/s |

Per-request decode rate uses the decode window after TTFT. Wall output rate is
successful output tokens divided by elapsed matrix time. They answer different
questions and should not be substituted for one another.

## Reproducing on another machine

Use the supplied benchmark config and record the actual hardware, llama.cpp
revision, model hash, quantization, context, offload, concurrency, and token
limit with every report:

```sh
./bin/liffo bench --config configs/bench.yaml --mode cpu --concurrency 1 --total-requests 32
./bin/liffo report --raw results/raw --target "actual hardware and software manifest"
```

Repeat with a clean output directory for each concurrency and mode. GPU results
require a retained server-log line proving actual layer offload. Numbers from
different environments are not matched before/after comparisons.
