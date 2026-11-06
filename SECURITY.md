# Security policy

This release candidate is intended for loopback or trusted private networks.
Report vulnerabilities privately to the repository owner once a public contact
channel exists; do not include model data, prompts, credentials, or exploit
details in a public issue.

Lifecycle mutation endpoints require `LIFFO_ADMIN_TOKEN`. Completion
requests are not authenticated. Use TLS and authentication at a reverse proxy
before any non-loopback exposure. Model IDs are allowlisted and model paths are
confined to `model_root`, including symlink resolution. Generated results omit
prompt and completion bodies by default.

Never commit `.env` files, bearer tokens, GGUF models, raw traces, or local
configs containing absolute paths.
