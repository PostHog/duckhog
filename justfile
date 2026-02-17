# DuckHog DuckDB Extension

# Default recipe: list all available recipes
default:
    @just --list

# === Build ===

# Build the extension (release). First build is slow — vcpkg compiles ~85 deps from source.
[group('build')]
build: _require-vcpkg submodules
    GEN=ninja make release
    ln -sf build/release/compile_commands.json .

# Build the extension (debug)
[group('build')]
build-debug: _require-vcpkg submodules
    GEN=ninja make debug

# Watch for changes and rebuild
[group('build')]
watch:
    watchexec -e cpp,hpp,cmake -- GEN=ninja make release

# Clean build artifacts
[group('build')]
clean:
    make clean

# Deep clean including duckdb build artifacts
[group('build')]
deep-clean:
    rm -rf build
    rm -rf duckdb/build

# === Test ===

# Run unit tests (no server required)
[group('test')]
test-unit: build
    ./build/release/test/unittest "[duckhog],test/sql/*" "~test/sql/queries/*" "~test/sql/roadmap/*"

# Run integration tests (requires running test servers)
[group('test')]
test-integration:
    ./build/release/test/unittest "test/sql/queries/*"

# Run full local suite (unit + integration with automatic setup/teardown).
# Expects duckgres at ../duckgres by default (override with DUCKGRES_ROOT).
[group('test')]
test-all: build _require-duckgres
    ./test/run_full_suite.sh "test/*" "~test/sql/roadmap/*"

# Run default extension-ci-tools test target
[group('test')]
test: build
    make test

# Run roadmap test suite (non-gating)
[group('test')]
test-roadmap:
    ./test/run_sql_roadmap.sh

# Run roadmap test suite (strict — failures are fatal)
[group('test')]
test-roadmap-strict:
    ./test/run_sql_roadmap.sh --strict

# Auto-format source files
[group('dev')]
format:
    PATH="$HOME/.local/bin:$PATH" python3 duckdb/scripts/format.py --all --fix --noconfirm --directories src test

# Check formatting without modifying files
[group('test')]
format-check:
    PATH="$HOME/.local/bin:$PATH" python3 duckdb/scripts/format.py --all --check --directories src test

# Run clang-tidy static analysis
[group('test')]
tidy-check:
    GEN=ninja make tidy-check

# === Servers ===

# Start test servers (Duckgres + DuckLake infra) in background.
# Expects duckgres at ../duckgres by default (override with DUCKGRES_ROOT).
[group('servers')]
start-servers: _require-duckgres
    ./scripts/test-servers.sh start --background --seed

# Stop test servers
[group('servers')]
stop-servers:
    ./scripts/test-servers.sh stop

# Show test server status
[group('servers')]
server-status:
    ./scripts/test-servers.sh status

# Print env vars for test servers (use: eval "$(just server-env)")
[group('servers')]
server-env:
    ./scripts/test-servers.sh env

# === Dev ===

# Load extension in interactive DuckDB shell
[group('dev')]
shell: build
    ./build/release/duckdb -cmd "LOAD 'build/release/extension/duckhog/duckhog.duckdb_extension';"

# Verify extension loads and is registered
[group('dev')]
verify: build
    ./build/release/duckdb -c "SELECT extension_name, loaded, extension_version FROM duckdb_extensions() WHERE extension_name = 'duckhog';"

# === Issues ===

# Claim a GitHub issue: assign to yourself and label status:in-progress
[group('dev')]
claim issue:
    gh issue edit {{issue}} --add-assignee @me --add-label status:in-progress

# === Setup ===

# One-time setup: install deps, vcpkg, and submodules
[group('setup')]
setup: setup-brew-deps setup-format-tools install-vcpkg submodules
    @echo ""
    @echo "Setup complete. Next steps:"
    @echo "  1. Add the VCPKG_TOOLCHAIN_PATH export shown above to ~/.zshenv"
    @echo "  2. source ~/.zshenv"
    @echo "  3. just build"

# Install required brew packages (macOS)
[group('setup')]
setup-brew-deps:
    brew install cmake ninja pkg-config bison
    @echo ""
    @echo "NOTE: If you hit bison errors during build, ensure Homebrew bison is first in PATH:"
    @echo "  export PATH=\"/opt/homebrew/opt/bison/bin:\$PATH\""

# Install formatting tools (black, clang-format, cmake-format) via uv
[group('setup')]
setup-format-tools:
    uv tool install "black>=24"
    uv tool install "clang-format==11.0.1"
    uv tool install cmakelang

# Install vcpkg and set VCPKG_TOOLCHAIN_PATH
[group('setup')]
install-vcpkg vcpkg_dir="~/.vcpkg":
    #!/usr/bin/env bash
    set -euo pipefail
    dir="{{vcpkg_dir}}"
    dir="${dir/#\~/$HOME}"
    if [ -f "$dir/scripts/buildsystems/vcpkg.cmake" ]; then
        echo "vcpkg already installed at $dir"
    else
        git clone https://github.com/Microsoft/vcpkg.git "$dir"
        "$dir/bootstrap-vcpkg.sh" -disableMetrics
    fi
    echo ""
    echo "Add to ~/.zshenv (or equivalent):"
    echo "  export VCPKG_TOOLCHAIN_PATH=\"$dir/scripts/buildsystems/vcpkg.cmake\""

# Initialize and update git submodules
[group('setup')]
submodules:
    git submodule update --init --recursive

# --- internal ---

[private]
_require-vcpkg:
    #!/usr/bin/env bash
    if [ -z "${VCPKG_TOOLCHAIN_PATH:-}" ]; then
        echo "ERROR: VCPKG_TOOLCHAIN_PATH is not set." >&2
        echo "Run 'just setup' or see docs/DEVELOPMENT.md." >&2
        exit 1
    fi
    if [ ! -f "$VCPKG_TOOLCHAIN_PATH" ]; then
        echo "ERROR: VCPKG_TOOLCHAIN_PATH does not exist: $VCPKG_TOOLCHAIN_PATH" >&2
        exit 1
    fi

[private]
_require-duckgres:
    #!/usr/bin/env bash
    set -euo pipefail
    root="${DUCKGRES_ROOT:-../duckgres}"
    root="${root/#\~/$HOME}"
    if [ ! -d "$root" ]; then
        echo "ERROR: duckgres repo not found at: $root" >&2
        echo "Clone duckgres to ../duckgres or set DUCKGRES_ROOT." >&2
        exit 1
    fi
