# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

.PHONY: all build test clean install-deps fmt lint examples docs release

# Default target
all: build test

# Build the Rust library and Go bindings
build:
	@echo "Building Rust library..."
	cargo build --release
	@echo "Generating C headers..."
	mkdir -p target/generated/include
	cbindgen --config cbindgen.toml --crate lancedb-go --output target/generated/include/lancedb.h
	@echo "Copying library files..."
	mkdir -p target/generated/lib
	cp ../target/release/liblancedb_go.a target/generated/lib/
	@echo "Building Go module..."
	go build ./...

# Run tests
test: build
	@echo "Running Go tests..."
	go test -v ./...

# Run benchmarks
bench: build
	@echo "Running benchmarks..."
	go test -bench=. -benchmem ./...

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	go clean ./...
	rm -rf target/

# Install development dependencies
install-deps:
	@echo "Installing Rust dependencies..."
	rustup update
	cargo install cbindgen
	@echo "Installing Go dependencies..."
	go mod download
	go mod tidy

# Format code
fmt:
	@echo "Formatting Rust code..."
	cargo fmt
	@echo "Formatting Go code..."
	go fmt ./...

# Lint code
lint:
	@echo "Linting Rust code..."
	cargo clippy -- -D warnings
	@echo "Linting Go code..."
	golangci-lint run

# Build examples
examples: build
	@echo "Building examples..."
	@mkdir -p target
	@for dir in examples/*/; do \
		if [ -d "$$dir" ] && [ "$$(basename "$$dir")" != "README.md" ]; then \
			example_name=$$(basename "$$dir"); \
			echo "Building $$example_name..."; \
			if [ -f "$$dir/main.go" ]; then \
				cd "$$dir" && go build -o "../../target/$${example_name}_example" . && cd ../..; \
			elif [ -f "$$dir/$${example_name}.go" ]; then \
				cd "$$dir" && go build -o "../../target/$${example_name}_example" "$${example_name}.go" && cd ../..; \
			else \
				echo "Warning: No main.go or $${example_name}.go found in $$dir"; \
			fi; \
		fi; \
	done
	@echo "Examples built in target/ directory"

# Run examples
run-examples: examples
	@echo "Running all examples..."
	@for example in target/*_example; do \
		if [ -x "$$example" ]; then \
			example_name=$$(basename "$$example" _example); \
			echo ""; \
			echo "=== Running $$example_name example ==="; \
			"$$example" || echo "Warning: $$example_name example failed with exit code $$?"; \
		fi; \
	done
	@echo ""
	@echo "All examples completed."

# Generate documentation
docs:
	@echo "Generating Rust documentation..."
	cargo doc --no-deps
	@echo "Generating Go documentation..."
	go doc -all ./...

# Check code formatting
check-fmt:
	@echo "Checking Rust code formatting..."
	cargo fmt -- --check
	@echo "Checking Go code formatting..."
	test -z "$$(gofmt -l .)"

# Create a release build
release: clean
	@echo "Creating release build..."
	cargo build --release --features remote
	cbindgen --config cbindgen.toml --crate lancedb-go --output target/generated/include/lancedb.h
	@echo "Copying library files..."
	mkdir -p target/generated/lib
	cp ../target/release/liblancedb_go.a target/generated/lib/
	@echo "Release build complete"

# Install pre-commit hooks
install-hooks:
	@echo "Installing pre-commit hooks..."
	@echo "#!/bin/sh" > .git/hooks/pre-commit
	@echo "make check-fmt && make lint" >> .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Pre-commit hooks installed"

# Development setup
dev-setup: install-deps install-hooks
	@echo "Development environment setup complete"

# Check if required tools are installed
check-tools:
	@command -v cargo >/dev/null 2>&1 || { echo "Rust/Cargo is required but not installed. Please install from https://rustup.rs/"; exit 1; }
	@command -v go >/dev/null 2>&1 || { echo "Go is required but not installed. Please install from https://golang.org/"; exit 1; }
	@command -v cbindgen >/dev/null 2>&1 || { echo "cbindgen is required. Install with: cargo install cbindgen"; exit 1; }
	@echo "All required tools are installed"

# Show help
help:
	@echo "Available targets:"
	@echo "  all          - Build and test"
	@echo "  build        - Build Rust library and Go bindings"
	@echo "  test         - Run tests"
	@echo "  bench        - Run benchmarks"
	@echo "  clean        - Clean build artifacts"
	@echo "  install-deps - Install development dependencies"
	@echo "  fmt          - Format code"
	@echo "  lint         - Lint code"
	@echo "  examples     - Build all examples"
	@echo "  run-examples - Run all examples"
	@echo "  docs         - Generate documentation"
	@echo "  check-fmt    - Check code formatting"
	@echo "  release      - Create release build"
	@echo "  dev-setup    - Setup development environment"
	@echo "  check-tools  - Check if required tools are installed"
	@echo "  help         - Show this help"
