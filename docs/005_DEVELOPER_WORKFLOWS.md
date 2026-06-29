# Developer Workflows

All development commands go through `just`. Run `just` with no arguments to list everything.

---

## Setup (once per clone)

| Command | What it does |
|---------|-------------|
| `just bootstrap` | Installs `cargo-nextest`, `sqlx-cli`, `cargo-deny`, and git hooks |
| `just doctor` | Verifies all tools are present; auto-installs missing cargo tools |
| `just hook` | Installs / refreshes `pre-commit` and `commit-msg` hooks (safe to re-run) |

---

## Daily development loop

| Command | What it does |
|---------|-------------|
| `just dev` | Start Postgres + zradar with hot reload (Docker Compose) |
| `just dev-logs` | Same as `just dev` and follows logs immediately |
| `just stop` | Stop all services |
| `just restart` | Stop then start |
| `just status` | Show container status + live health check |
| `just health` | Hit `/health` and `/health/ready` |
| `just logs` | Tail all container logs |
| `just logs-server` | Tail zradar server logs only |
| `just shell` | Open a shell in the running zradar container |
| `just db-shell` | Open a psql shell against the running Postgres |

---

## Code quality

Run these before opening a PR (the `pre-commit` hook runs them automatically on every commit):

| Command | What it does |
|---------|-------------|
| `just fmt` | `cargo fmt` — auto-format all Rust code |
| `just check` | `cargo check --all-targets` — fast compile check |
| `just lint` | `cargo clippy --all-targets -- -D warnings` — zero warnings required |
| `just fix` | Apply `cargo fix` + `clippy --fix` suggestions automatically |

---

## Testing

| Command | What it does |
|---------|-------------|
| `just test` | Unit tests (`cargo test`) |
| `just functional-tests` | Full E2E against a fresh Docker environment (tears down after) |
| `just functional-tests-fast` | E2E reusing the running dev stack — faster for iteration |
| `just functional-tests-fast <name>` | Run a single named functional test |
| `just test-all` | `just test` + `just functional-tests` |

---

## Framework examples (requires `just dev` running)

| Command | What it does |
|---------|-------------|
| `just example <fw>` | Run a single framework example against the local stack |
| `just example-test <fw>` | Run example + validate spans arrived in zradar |
| `just example-test-all` | Run all 10 frameworks end-to-end |
| `just example-update-snapshot <fw>` | Regenerate `tests/expected_spans.json` after intentional format changes |
| `just sdk-check` | Check all example SDKs against PyPI/npm for available updates |

Available frameworks: `langchain` · `openai-agents` · `openai` · `pydantic-ai` · `crewai` · `llamaindex` · `anthropic` · `google-adk` · `vercel-ai-sdk` · `mastra`

All examples use mock LLMs — no API key needed. Set `ZRADAR_API_KEY` to override the default dev key.

---

## Database and SQLx

| Command | What it does |
|---------|-------------|
| `just migrate` | Run pending SQL migrations |
| `just sqlx-prepare` | Regenerate `.sqlx/` offline query cache (commit this after schema changes) |
| `just clean-sqlx` | Delete and regenerate `.sqlx/` cache |

When to run `just sqlx-prepare`: after any change to a `sqlx::query!` macro or a migration file. Commit `.sqlx/` to keep CI builds offline-capable.

---

## Build and release

| Command | What it does |
|---------|-------------|
| `just build-release` | Build optimised binary (`target/release/zradar`) |
| `just run` | Run binary locally (requires external DB via `DATABASE_URL`) |
| `just build-prod` | Build production Docker images |
| `just prod` | Run production-like stack locally |
| `just show-version` | Print current version from `VERSION` file |
| `just version-bump [patch\|minor\|major]` | Bump `VERSION` + `Cargo.toml` (does not commit) |
| `just release-publish [patch\|minor\|major]` | Bump → commit → tag → push (triggers CI binary build) |

---

## Cleanup

| Command | What it does |
|---------|-------------|
| `just clean` | Stop containers and remove volumes (preserves `./data/`) |
| `just clean-all` | `just clean` + delete `./data/` — **destructive, prompts for confirmation** |

---

## Fast builds (optional)

Install `mold` (linker) and `sccache` (compiler cache), then prefix any command:

```bash
ZRADAR_FAST_BUILD=1 just test
ZRADAR_FAST_BUILD=1 just check
```

Meaningfully faster on repeated builds. Not required; the default path needs no extra tools.
