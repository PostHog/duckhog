PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=duckhog
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: test-roadmap
test-roadmap:
	./test/run_sql_roadmap.sh

# Exclude roadmap SQLLogicTests from normal `make test` targets.
# The extension-ci-tools recipe invokes:
#   ./build/<cfg>/test/unittest "$(TESTS_BASE_DIRECTORY)*"
# We inject both include and exclude patterns via TESTS_BASE_DIRECTORY.
TESTS_BASE_DIRECTORY = test/*" "~test/sql/roadmap/
