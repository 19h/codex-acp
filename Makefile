SHELL := /bin/bash
.PHONY: build release release-linux-x64 release-linux-arm64 release-macos-x64 release-windows-x64 run run-mock fmt clippy check test lint all clean

CARGO := cargo
XWIN_CACHE_DIR ?= $(CURDIR)/.cache/cargo-xwin

build:
	$(CARGO) build

release:
	$(CARGO) build --release

release-linux-x64:
	cross build --release --target x86_64-unknown-linux-gnu

release-linux-arm64:
	cross build --release --target aarch64-unknown-linux-gnu

release-macos-x64:
	$(CARGO) build --release --target x86_64-apple-darwin

release-windows-x64:
	XWIN_ACCEPT_LICENSE=1 XWIN_CACHE_DIR="$(XWIN_CACHE_DIR)" cargo xwin build --release --target x86_64-pc-windows-msvc

run:
	RUST_LOG?=info
	RUST_LOG=$(RUST_LOG) $(CARGO) run --quiet

fmt:
	$(CARGO) fmt --all

clippy:
	$(CARGO) clippy -- -D warnings

check:
	$(CARGO) check

test:
	$(CARGO) test

lint: fmt clippy

clean:
	$(CARGO) clean

all: fmt clippy build
