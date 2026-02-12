# Codex Cloud CI Simulation (Tidy Check)

This documents a direct simulation of the `tidy-check` job from `.github/workflows/MainDistributionPipeline.yml` in Codex Cloud.

## Environment
- `uname -s`: `Linux`
- `uname -m`: `x86_64`
- `id -u`: `0`

## Commands run

```bash
git submodule update --init --recursive
apt-get update -y -qq
apt-get install -y -qq ninja-build clang-tidy python3-pip
pip3 install pybind11[global] --break-system-packages
```

```bash
BASELINE="$(python3 - <<'PY'
import json
with open('vcpkg-configuration.json','r',encoding='utf-8') as f:
    print(json.load(f)['default-registry']['baseline'])
PY
)"
rm -rf vcpkg
git clone https://github.com/microsoft/vcpkg.git vcpkg
cd vcpkg
git checkout "$BASELINE"
./bootstrap-vcpkg.sh -disableMetrics
```

```bash
CC=gcc CXX=g++ GEN=ninja TIDY_THREADS=4 \
VCPKG_TOOLCHAIN_PATH="$PWD/vcpkg/scripts/buildsystems/vcpkg.cmake" \
VCPKG_TARGET_TRIPLET=x64-linux-release \
VCPKG_HOST_TRIPLET=x64-linux-release \
VCPKG_OVERLAY_PORTS="$PWD/extension-ci-tools/vcpkg_ports" \
VCPKG_OVERLAY_TRIPLETS="$PWD/extension-ci-tools/toolchains" \
VCPKG_BINARY_SOURCES='clear;http,https://vcpkg-cache.duckdb.org,read' \
make tidy-check
```

## Result in this run
- The job setup completed successfully (submodules, tooling install, vcpkg bootstrap).
- `make tidy-check` started successfully and entered `vcpkg install` for `x64-linux-release`.
- The run progressed into dependency compilation (`protobuf`, `openssl`, `grpc`) but did not complete within this iteration.

## Notes
- Warnings were observed when trying to restore from `https://vcpkg-cache.duckdb.org` (`CONNECT tunnel failed, response 403`), so dependencies were built from source.
- This significantly increases runtime compared with CI cache hits.
