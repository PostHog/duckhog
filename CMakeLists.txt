cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME posthog)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package.
find_package(OpenSSL REQUIRED)

# Arrow Flight SQL dependencies (for Milestone 2+)
# Uncomment these when implementing Flight SQL client:
# find_package(Arrow REQUIRED)
# find_package(ArrowFlight REQUIRED)
# find_package(ArrowFlightSql REQUIRED)
# find_package(gRPC CONFIG REQUIRED)
# find_package(Protobuf REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# Milestone 1: Basic extension that loads and prints version
set(EXTENSION_SOURCES
    src/posthog_extension.cpp
)

# Milestone 2+ sources (uncomment when implementing):
# list(APPEND EXTENSION_SOURCES
#     src/storage/posthog_storage.cpp
#     src/catalog/posthog_catalog.cpp
#     src/catalog/posthog_schema.cpp
#     src/catalog/posthog_table.cpp
#     src/catalog/remote_scan.cpp
#     src/flight/flight_client.cpp
#     src/flight/arrow_conversion.cpp
#     src/utils/connection_string.cpp
# )

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

# Arrow Flight SQL dependencies (for Milestone 2+)
# Uncomment when implementing Flight SQL client:
# target_link_libraries(${EXTENSION_NAME}
#     Arrow::arrow_shared
#     ArrowFlight::arrow_flight_shared
#     ArrowFlightSql::arrow_flight_sql_shared
#     gRPC::grpc++
#     protobuf::libprotobuf
# )
# target_link_libraries(${LOADABLE_EXTENSION_NAME}
#     Arrow::arrow_shared
#     ArrowFlight::arrow_flight_shared
#     ArrowFlightSql::arrow_flight_sql_shared
#     gRPC::grpc++
#     protobuf::libprotobuf
# )

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
