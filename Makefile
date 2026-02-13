PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=duckhog
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
# Use release-branch versioning so v1.4.x tags report as v1.4.x at runtime.
EXT_FLAGS += -DMAIN_BRANCH_VERSIONING=0

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: test-roadmap
test-roadmap:
	./test/run_sql_roadmap.sh

# Override tidy-check to include vcpkg manifest flags so FlightSQL features
# from this extension's vcpkg.json are visible during clang-tidy configuration.
.PHONY: tidy-check
tidy-check: ${EXTENSION_CONFIG_STEP}
	mkdir -p ./build/tidy
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_DEBUG_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DVCPKG_BUILD_TYPE=release -DDISABLE_UNITY=1 -DCLANG_TIDY=1 -S $(DUCKDB_SRCDIR) -B build/tidy
	cp duckdb/.clang-tidy build/tidy/.clang-tidy
	cd build/tidy && python3 ../../duckdb/scripts/run-clang-tidy.py '$(PROJ_DIR)src/.*/' -header-filter '$(PROJ_DIR)src/.*/' -quiet ${TIDY_THREAD_PARAMETER} ${TIDY_BINARY_PARAMETER} ${TIDY_PERFORM_CHECKS}

# Exclude roadmap SQLLogicTests from normal `make test` targets.
# The extension-ci-tools recipe invokes:
#   ./build/<cfg>/test/unittest "$(TESTS_BASE_DIRECTORY)*"
# We inject both include and exclude patterns via TESTS_BASE_DIRECTORY.
TESTS_BASE_DIRECTORY = test/*" "~test/sql/roadmap/

# Ensure `make test` also prepares integration dependencies for Flight tests.
# This overrides extension-ci-tools' `test_release_internal` recipe while
# preserving its higher-level target wiring (`test` -> `test_release`).
.PHONY: test_release_internal
test_release_internal:
	@set -eu; \
	./scripts/test-servers.sh start --background --seed; \
	trap './scripts/test-servers.sh stop' EXIT INT TERM; \
	eval "$$(./scripts/test-servers.sh env)"; \
	./build/release/$(TEST_PATH) "$(TESTS_BASE_DIRECTORY)*"
