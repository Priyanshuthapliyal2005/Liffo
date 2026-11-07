# Run real inference

> Connect Liffo to a real `llama-server`, produce a privacy-safe measurement,
> and finish with no orphaned inference process.

[Documentation](README.md) · [Architecture](ARCHITECTURE.md) · [Reference results](BENCHMARKS.md)

---

## What a successful run proves

By the end of this guide you should have:

- a healthy OpenAI-compatible inference worker;
- a Liffo gateway forwarding streamed completions;
- JSONL containing prompt hashes rather than prompt or completion bodies;
- a report labeled with the actual hardware and software manifest;
- GPU log evidence of layer offload, if testing GPU execution; and
- no `llama-server` process left behind after cleanup.

## Before you begin

| Requirement | Why it matters |
|---|---|
| Recent `llama-server` | Provides the reference OpenAI-compatible worker API |
| GGUF model | Liffo does not download or bundle model weights |
| Go 1.27.3 | Matches the release-candidate toolchain |
| `nvidia-smi` for NVIDIA runs | Enables VRAM and utilization sampling |

Record the CPU, RAM, OS, GPU, driver, llama.cpp revision, model filename and
hash, quantization, context size, `-ngl`, concurrency, and output-token limit.
Without that manifest, the number is not reproducible.

## Path A — external worker

This is the shortest and most transparent setup. Use three terminals.

### 1. Start the inference worker

CPU:

```sh
llama-server \
  -m /path/to/model.gguf \
  --host 127.0.0.1 \
  --port 8080 \
  -c 2048 \
  -ngl 0
```

GPU:

```sh
llama-server \
  -m /path/to/model.gguf \
  --host 127.0.0.1 \
  --port 8080 \
  -c 2048 \
  -ngl 999 \
  --verbose 2>&1 | tee llama-server-gpu.log
```

`-ngl 999` asks llama.cpp to offload as many layers as the device supports. A
smaller value may be appropriate for limited VRAM.

### 2. Start Liffo

```sh
go build -trimpath -o bin/liffo ./cmd/liffo
./bin/liffo gateway --config configs/gateway.yaml
```

### 3. Verify the mesh

```sh
./bin/liffo status --gateway http://127.0.0.1:8088
curl -fsS http://127.0.0.1:8088/readyz
```

Do not begin a timed run until both the worker and gateway report ready.

### 4. Measure

```sh
./bin/liffo bench \
  --config configs/bench.yaml \
  --mode cpu \
  --concurrency 1 \
  --total-requests 10

./bin/liffo report \
  --raw results/raw \
  --target "CPU, RAM, GPU, OS, driver, llama.cpp revision, model hash"
```

For a GPU run, change `--mode cpu` to `--mode gpu`. Use a fresh output
directory when changing modes or concurrency so samples cannot be mixed.

### 5. Prove GPU placement

```sh
grep -E "offloaded [0-9]+/[0-9]+ layers|CUDA[0-9]+ model buffer" llama-server-gpu.log
```

Expected evidence resembles:

```text
offloaded 25/25 layers to GPU
CUDA0 model buffer size = 373.73 MiB
```

GPU utilization is supporting evidence, not proof that model layers were
offloaded.

### 6. Clean up

Stop the gateway and worker with `Ctrl-C`, then verify that no worker remains:

```sh
pgrep -fl 'llama-server|liffo' || true
```

On Windows PowerShell, use:

```powershell
Get-Process llama-server,liffo -ErrorAction SilentlyContinue
```

## Path B — managed lifecycle

Managed mode lets Liffo start, health-check, drain, and stop a local
`llama-server` process.

```mermaid
flowchart LR
    O[Start gateway] --> S[Spawn allowlisted model]
    S --> H[Wait for health]
    H --> R[Ready]
    R --> T[Serve streaming traffic]
    T --> D[Stop new leases and drain]
    D --> X[Terminate process]
```

Copy `configs/gateway-managed.example.yaml`, replace its paths, and map a
logical model ID to a GGUF beneath `model_root`.

```sh
export LIFFO_ADMIN_TOKEN='generate-a-long-random-token'
./bin/liffo gateway --config /path/to/gateway-managed.yaml
```

Never commit the token or a machine-specific managed configuration. On normal
shutdown, Liffo terminates the process it started.

## Troubleshooting

| Symptom | Check first |
|---|---|
| Gateway is not ready | Worker URL, `/health`, port conflicts, and model load time |
| No streamed tokens | Worker supports `/v1/chat/completions` with `stream: true` |
| GPU memory stays at zero | CUDA build, driver compatibility, `-ngl`, and offload log lines |
| Requests fail under load | Admission limits, worker slots, timeout, and context size |
| Report mixes experiments | Use a new raw-results directory per mode and matrix cell |
| Process remains after managed shutdown | Inspect lifecycle logs and terminate it before another run |

Next: compare your output schema and interpretation with the
[reference benchmark](BENCHMARKS.md).
