# Quick Start Guide

This guide provides the essential commands to get started quickly.

## One-Time Setup

### 1. Install Prerequisites (macOS)

```bash
brew install cmake ninja pkg-config bison
export PATH="/opt/homebrew/opt/bison/bin:$PATH"
```

### 2. Set Up vcpkg

```bash
cd ~/projects
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg
git checkout ce613c41372b23b1f51333815feb3edd87ef8a8b
./bootstrap-vcpkg.sh -disableMetrics
```

### 3. Configure Environment

Add to your `~/.zshrc` or `~/.bashrc`:

```bash
export VCPKG_TOOLCHAIN_PATH=~/projects/vcpkg/scripts/buildsystems/vcpkg.cmake
export PATH="/opt/homebrew/opt/bison/bin:$PATH"
```

Reload your shell:

```bash
source ~/.zshrc
```

## Daily Development

### Build

```bash
# Release build (recommended)
GEN=ninja make release

# Debug build
GEN=ninja make debug

# Clean build
rm -rf build/release && GEN=ninja make release
```

### Test

```bash
# Run all tests
make test

# Test extension loading
./build/release/duckdb -cmd "LOAD 'build/release/extension/posthog/posthog.duckdb_extension';"

# Interactive shell with extension
./build/release/duckdb
```

### Verify Extension

```sql
-- In DuckDB shell
SELECT * FROM duckdb_extensions() WHERE extension_name = 'posthog';

-- Test attach (will fail without server, but verifies protocol registration)
ATTACH 'hog:testdb?token=test123' AS remote;
```

## Common Issues

| Issue | Solution |
|-------|----------|
| `bison` errors | `brew install bison && export PATH="/opt/homebrew/opt/bison/bin:$PATH"` |
| `pkg-config not found` | `brew install pkg-config` |
| `VCPKG_TOOLCHAIN_PATH not set` | `export VCPKG_TOOLCHAIN_PATH=~/projects/vcpkg/scripts/buildsystems/vcpkg.cmake` |
| Slow first build | Normal - vcpkg compiles dependencies. Subsequent builds are fast. |

## File Locations

| What | Where |
|------|-------|
| Built extension | `build/release/extension/posthog/posthog.duckdb_extension` |
| DuckDB binary | `build/release/duckdb` |
| Test binary | `build/release/test/unittest` |
| vcpkg packages | `build/release/vcpkg_installed/` |

## Useful Commands

```bash
# Check extension is registered
echo "SELECT * FROM duckdb_extensions() WHERE extension_name = 'posthog';" | ./build/release/duckdb

# View extension info
echo "LOAD 'build/release/extension/posthog/posthog.duckdb_extension'; SELECT extension_name, extension_version FROM duckdb_extensions() WHERE loaded;" | ./build/release/duckdb

# Clean everything
rm -rf build/ && make clean
```
