# Reproducible real-inference demonstration

Record the exact CPU, RAM, operating system, GPU, driver, llama.cpp revision,
model filename/hash, quantization, context size, `-ngl`, concurrency, and
token limit before interpreting results.

## External server mode

1. Start one `llama-server` on loopback. CPU example:

   ```sh
   llama-server -m /models/model.gguf --host 127.0.0.1 --port 8080 -c 2048 -ngl 0
   ```

2. In terminal two, start Liffo:

   ```sh
   go run ./cmd/liffo gateway --config configs/gateway.yaml
   ```

3. In terminal three, verify and benchmark:

   ```sh
   go run ./cmd/liffo status
   go run ./cmd/liffo bench --config configs/bench.yaml --mode cpu --total-requests 10
   go run ./cmd/liffo report --raw results/raw --target "CPU/RAM/GPU/OS details"
   ```

4. Stop Liffo and `llama-server`. Confirm no inference process remains.

For GPU mode, choose an `-ngl` supported by the model and VRAM, label the run
`--mode gpu`, and retain the server log line proving the number of layers
offloaded. A GPU-utilization sample alone is not proof of model offload.

## Managed mode

Copy `configs/gateway-managed.example.yaml`, replace paths, and use a logical
model ID mapped to a GGUF beneath `model_root`.

```sh
export LIFFO_ADMIN_TOKEN='generate-a-long-random-token'
go run ./cmd/liffo gateway --config /path/to/gateway-managed.yaml
```

The gateway starts the configured model, waits for `/health`, drains in-flight
requests before replacement or eviction, and terminates its managed process on
shutdown. Never commit the token or local absolute-path config.
