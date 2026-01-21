cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME posthog)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package.
find_package(OpenSSL REQUIRED)

# Arrow Flight SQL dependencies (Milestone 3)
find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)
find_package(ArrowFlightSql REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include src)

# Extension sources
set(EXTENSION_SOURCES
    src/posthog_extension.cpp
    # Milestone 2: Storage extension with hog: protocol registration
    src/storage/posthog_storage.cpp
    src/storage/posthog_transaction_manager.cpp
    src/catalog/posthog_catalog.cpp
    src/utils/connection_string.cpp
    # Milestone 3: Arrow Flight SQL client integration
    src/flight/flight_client.cpp
    src/flight/arrow_conversion.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

# Arrow Flight SQL dependencies (Milestone 3) - using static libraries from vcpkg
target_link_libraries(${EXTENSION_NAME}
    Arrow::arrow_static
    ArrowFlight::arrow_flight_static
    ArrowFlightSql::arrow_flight_sql_static
    gRPC::grpc++
    protobuf::libprotobuf
)
target_link_libraries(${LOADABLE_EXTENSION_NAME}
    Arrow::arrow_static
    ArrowFlight::arrow_flight_static
    ArrowFlightSql::arrow_flight_sql_static
    gRPC::grpc++
    protobuf::libprotobuf
)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
