# Contributing to zradar

Thank you for your interest in zradar! This document is the hub for everything you need to contribute — whether you're fixing a bug, adding a framework integration, writing documentation, or proposing a new feature.

---

## Ways to contribute

You don't need to write Rust to contribute meaningfully:

- **Report a bug** — open a [bug report](https://github.com/zvectorlabs/zradar/issues/new?template=bug_report.yml)
- **Request a feature** — open a [feature request](https://github.com/zvectorlabs/zradar/issues/new?template=feature_request.yml)
- **Propose a design** — open an [RFC](https://github.com/zvectorlabs/zradar/issues/new?template=rfc.yml) for larger changes before implementing
- **Write docs** — improve READMEs, architecture docs, or example code
- **Add a framework integration** — instrument a new AI framework (see `examples/` for reference)
- **Fix a bug or implement a feature** — look for issues labelled [`good first issue`](https://github.com/zvectorlabs/zradar/issues?q=is%3Aopen+label%3A%22good+first+issue%22) or [`help wanted`](https://github.com/zvectorlabs/zradar/issues?q=is%3Aopen+label%3A%22help+wanted%22)

---

## Development setup

### Prerequisites (install once manually)

- **Rust 1.93.0** — `rustup` (https://rustup.rs)
- **Docker** — for local Postgres in dev/test
- **Python 3** — for functional test scripts
- **`just`** — `cargo install just`

### First-time setup

```bash
git clone https://github.com/zvectorlabs/zradar.git
cd zradar
just bootstrap   # installs cargo tools + git hooks, checks Rust version
just dev         # starts local Postgres and dev environment
just test        # confirm everything works
```

`just bootstrap` handles: `cargo-nextest`, `sqlx-cli`, git hooks (`pre-commit` + `commit-msg`), and optional fast-build tools (`mold`/`sccache`).

See the [README](README.md) for full quickstart and configuration details.

---

## Making a change

1. **Open or find an issue first** for anything beyond a trivial fix. This avoids duplicate work and lets maintainers flag design concerns before you invest time.
2. **Fork the repo** and create a branch from `main`:
   ```bash
   git checkout -b feat/your-feature-name
   ```
3. **Make your changes.** Follow [CODING_GUIDELINES.md](CODING_GUIDELINES.md) for Rust style.
4. **Validate locally** before opening a PR:
   ```bash
   just fmt      # format
   just check    # compile check
   just lint     # clippy (zero warnings)
   just test     # unit tests
   ```
5. **Open a pull request** against `main`. Fill in the PR template.

---

## Commit style

zradar uses [Conventional Commits](https://www.conventionalcommits.org/). The `commit-msg` hook enforces the format on every commit.

```
<type>(<scope>): <subject>        ← max 70 characters

- what changed and why            ← bullet points only
- second point if needed          ← max 5 bullets
```

**Types:** `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`, `build`, `ci`

**Example:**
```
feat(ingest): add MCP span type detection

- maps mcp.tool.name and mcp.server.name to span fields
- registers McpConvention in the dispatch pipeline
```

Hooks are installed by `just bootstrap`. Refresh anytime with `just hook`.

---

## RFC process

For changes that affect the public API, data model, or storage format, open an [RFC issue](https://github.com/zvectorlabs/zradar/issues/new?template=rfc.yml) before writing code. This gives the community a chance to weigh in on the design. We'll tag the issue `rfc: accepted` or `rfc: declined` with rationale.

---

## Code of conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating you agree to uphold it.

---

## Getting help

- Open a [GitHub Discussion](https://github.com/zvectorlabs/zradar/discussions) for questions, ideas, or anything that doesn't fit an issue
- For bugs or concrete feature requests, use [GitHub Issues](https://github.com/zvectorlabs/zradar/issues)
