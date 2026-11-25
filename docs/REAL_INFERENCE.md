# Run real inference

The [quickstart](../README.md#quickstart) runs a CPU worker and sends requests
through Liffo. For the CPU/L4 direct-backend matrix, follow the
[benchmark guide](BENCHMARKS.md).

## External GPU worker

Use the same quickstart with a GPU-enabled build of the worker:

```sh
llama-server -m /path/to/model.gguf --host 127.0.0.1 --port 8080 \
  -c 2048 -ngl 999 --verbose 2>&1 | tee llama-server-gpu.log
```

Lower `-ngl` if device memory is limited. Check the worker log for the number
of layers offloaded and the GPU model buffer. Use `--mode gpu` with the
quickstart bench command to label the samples and enable NVIDIA sampling
through `nvidia-smi` when available.

## Managed worker

Copy [the managed configuration](../configs/gateway-managed.example.yaml),
replace the runtime and model paths, and map a logical model ID to a GGUF
beneath `model_root`. Set `LIFFO_ADMIN_TOKEN` through your environment before
using the lifecycle mutation endpoints.

```sh
./bin/liffo gateway --config /path/to/gateway-managed.yaml
curl -fsS http://127.0.0.1:8088/ready
```

A configured bootstrap model is loaded at startup. Liffo drains and stops the
worker it launched during normal shutdown. If the gateway is unready, check
worker logs, port conflicts and model load time. See the
[deployment policy](../SECURITY.md) before moving off loopback.
