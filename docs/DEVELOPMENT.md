# Development Environment Setup

This guide covers setting up the development environment for the PostHog DuckDB Extension.

## TL;DR (Quickstart)

**macOS (one-time setup):**

```bash
brew install cmake ninja pkg-config bison
export PATH="/opt/homebrew/opt/bison/bin:$PATH"

cd ~/projects
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg
git checkout ce613c41372b23b1f51333815feb3edd87ef8a8b
./bootstrap-vcpkg.sh -disableMetrics
export VCPKG_TOOLCHAIN_PATH=~/projects/vcpkg/scripts/buildsystems/vcpkg.cmake
```

**Build + test:**

```bash
GEN=ninja make release
make test
```

For full test instructions (unit + integration), see `test/README.md`.

## Prerequisites

- **macOS** (Apple Silicon or Intel) or **Linux**
- **CMake** 3.10 or later
- **Ninja** build system (recommended)
- **C++17 compatible compiler** (Clang 10+ or GCC 9+)
- **Git**

### macOS

Install the required tools via Homebrew:

```bash
brew install cmake ninja pkg-config bison
```

**Note:** The system bison on macOS is outdated. If you encounter build errors related to bison, ensure the Homebrew version is in your PATH:

```bash
export PATH="/opt/homebrew/opt/bison/bin:$PATH"
```

### Linux (Ubuntu/Debian)

```bash
sudo apt-get update
sudo apt-get install -y cmake ninja-build pkg-config bison flex \
    build-essential git
```

## Setting Up vcpkg

The extension uses [vcpkg](https://github.com/microsoft/vcpkg) for dependency management. Follow these steps to set it up:

### Step 1: Clone vcpkg

Clone vcpkg to a directory **outside** the extension repository:

```bash
cd ~/projects  # or your preferred location
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg
```

### Step 2: Checkout the Pinned Version

We use a specific vcpkg commit for reproducible builds:

```bash
git checkout 23dc124705fcac41cf35c33dd9541f5094a9c19f
```

This repository also pins the vcpkg baseline in `vcpkg-configuration.json` so that
dependency resolution is reproducible even if your vcpkg checkout is at a different commit.
To upgrade dependencies, update the `baseline` commit in `vcpkg-configuration.json` and
rebuild vcpkg-installed packages.

### Bumping the vcpkg baseline safely

1. Update your local vcpkg checkout to the desired commit.
2. Replace the `baseline` hash in `vcpkg-configuration.json`.
3. Remove installed packages for this build (e.g. `rm -rf build/release/vcpkg_installed/`).
4. Rebuild and run the relevant tests.

### Step 3: Bootstrap vcpkg

```bash
./bootstrap-vcpkg.sh -disableMetrics
```

### Step 4: Set Environment Variable

Set the `VCPKG_TOOLCHAIN_PATH` environment variable:

```bash
export VCPKG_TOOLCHAIN_PATH=$(pwd)/scripts/buildsystems/vcpkg.cmake
```

Add this to your shell profile (`~/.bashrc`, `~/.zshrc`, etc.) for persistence:

```bash
echo 'export VCPKG_TOOLCHAIN_PATH=~/projects/vcpkg/scripts/buildsystems/vcpkg.cmake' >> ~/.zshrc
```

## Building the Extension

### Clone the Repository

```bash
git clone --recurse-submodules https://github.com/PostHog/posthog-duckdb-extension.git
cd posthog-duckdb-extension
```

If you already cloned without submodules:

```bash
git submodule update --init --recursive
```

### Build Commands

**Release build (recommended):**

```bash
GEN=ninja make release
```

**Debug build:**

```bash
GEN=ninja make debug
```

`GEN=ninja` is optional, it just speeds up the build.

### First Build Notes

The first build will take longer as vcpkg downloads and compiles all dependencies:
- Arrow (with Flight and FlightSQL)
- gRPC
- Protobuf
- OpenSSL
- Boost libraries
- And other transitive dependencies

Subsequent builds will be much faster as dependencies are cached in `~/.cache/vcpkg/archives`.

## Verifying the Build

After a successful build, verify the extension loads correctly:

```bash
./build/release/duckdb -cmd "LOAD 'build/release/extension/posthog/posthog.duckdb_extension';"
```

Check that the extension is registered:

```bash
echo "SELECT * FROM duckdb_extensions() WHERE extension_name = 'posthog';" | ./build/release/duckdb
```

Expected output:
```
┌────────────────┬─────────┬───────────┬──────────────┬─────────────┬───────────┬───────────────────┬───────────────────┬────────────────┐
│ extension_name │ loaded  │ installed │ install_path │ description │  aliases  │ extension_version │   install_mode    │ installed_from │
│    varchar     │ boolean │  boolean  │   varchar    │   varchar   │ varchar[] │      varchar      │      varchar      │    varchar     │
├────────────────┼─────────┼───────────┼──────────────┼─────────────┼───────────┼───────────────────┼───────────────────┼────────────────┤
│ posthog        │ true    │ true      │ (BUILT-IN)   │             │ []        │ ...               │ STATICALLY_LINKED │                │
└────────────────┴─────────┴───────────┴──────────────┴─────────────┴───────────┴───────────────────┴───────────────────┴────────────────┘
```

## Project Structure

```
posthog-duckdb-extension/
├── src/
│   ├── posthog_extension.cpp      # Extension entry point
│   ├── catalog/
│   │   ├── posthog_catalog.cpp    # Catalog implementation
│   │   └── posthog_catalog.hpp
│   ├── flight/
│   │   ├── flight_client.cpp      # Arrow Flight SQL client
│   │   ├── flight_client.hpp
│   │   ├── arrow_stream.cpp       # Arrow C stream bridge for DuckDB scan
│   │   └── arrow_stream.hpp
│   ├── storage/
│   │   ├── posthog_storage.cpp    # Storage extension (hog: protocol)
│   │   ├── posthog_storage.hpp
│   │   ├── posthog_transaction_manager.cpp
│   │   └── posthog_transaction_manager.hpp
│   └── utils/
│       ├── connection_string.cpp  # Connection string parser
│       └── connection_string.hpp
├── duckdb/                        # DuckDB submodule
├── extension-ci-tools/            # CI tooling submodule
├── CMakeLists.txt                 # Build configuration
├── vcpkg.json                     # Dependency manifest
└── docs/
    └── DEVELOPMENT.md             # This file
```

## Dependencies

The extension depends on the following libraries (managed via vcpkg):

| Dependency | Purpose |
|------------|---------|
| Arrow | Core Arrow library for columnar data |
| Arrow Flight | gRPC-based data transport |
| Arrow Flight SQL | SQL query execution over Flight |
| gRPC | Remote procedure call framework |
| Protobuf | Serialization for gRPC |
| OpenSSL | TLS/SSL support |

## Troubleshooting

### Build fails with "bison" errors

The system bison may be too old. Install a newer version:

```bash
brew install bison
export PATH="/opt/homebrew/opt/bison/bin:$PATH"
```

### CMake can't find vcpkg packages

Ensure `VCPKG_TOOLCHAIN_PATH` is set correctly:

```bash
echo $VCPKG_TOOLCHAIN_PATH
# Should output: /path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build is slow

The first build compiles many dependencies. Use multiple CPU cores:

```bash
# vcpkg respects this for parallel builds
export VCPKG_MAX_CONCURRENCY=$(nproc)
```

### "pkg-config not found" error

Install pkg-config:

```bash
# macOS
brew install pkg-config

# Linux
sudo apt-get install pkg-config
```

### Cleaning the build

```bash
# Clean build artifacts
make clean

# Full clean (removes build directory)
rm -rf build/

# Clean vcpkg installed packages for this project
rm -rf build/release/vcpkg_installed/
```

## Testing

See `test/README.md` for unit/integration test flows and Flight server setup.

## IDE Setup

### VS Code

Recommended extensions:
- C/C++ (Microsoft)
- CMake Tools
- clangd (for better code intelligence)

Create `.vscode/settings.json`:

```json
{
    "cmake.configureSettings": {
        "CMAKE_TOOLCHAIN_FILE": "${env:VCPKG_TOOLCHAIN_PATH}"
    },
    "C_Cpp.default.configurationProvider": "ms-vscode.cmake-tools"
}
```

### CLion

1. Open the project
2. Go to Settings > Build, Execution, Deployment > CMake
3. Add to CMake options: `-DCMAKE_TOOLCHAIN_FILE=$VCPKG_TOOLCHAIN_PATH`
