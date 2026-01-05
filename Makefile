.PHONY: build
build:
	cargo build --all-targets

.PHONY: check
check:
	cargo clippy --all-targets -- --no-deps -D warnings
	cargo test --doc

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
