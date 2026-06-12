.PHONY: build
build:
	cargo build --all-targets

.PHONY: check
check:
	cargo clippy --all-targets -- --no-deps -D warnings
	cargo test --doc

# Mirror the test split CI uses (`cargo test --lib` then `cargo test --tests`).
# Run separately so a slow integration test doesn't hide a fast-suite regression.
.PHONY: test
test:
	cargo test --lib
	cargo test --tests

# `cargo audit` checks our locked dependencies against RustSec advisories;
# matches the `audit-check` job in CI. Installs the binary on first run.
.PHONY: audit
audit:
	@command -v cargo-audit >/dev/null || cargo install --locked cargo-audit
	cargo audit

# `cargo deny` enforces the policy in `deny.toml` (license allowlist,
# duplicate-version checks, banned crates). Matches the `cargo-deny-action`
# step in CI.
.PHONY: deny
deny:
	@command -v cargo-deny >/dev/null || cargo install --locked cargo-deny
	cargo deny check

# Run every check CI runs, in CI order, so `make ci` locally surfaces the
# same failures the workflow would.
.PHONY: ci
ci: fmt check test audit deny

.PHONY: fmt
fmt:
	cargo fmt --all

.PHONY: clean
clean:
	cargo clean

.PHONY: outdated
outdated:
	cargo install --locked cargo-outdated
	cargo outdated -R
