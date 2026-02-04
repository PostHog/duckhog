#!/usr/bin/env python3
"""
PostHog DuckDB Extension - Test Flight SQL Server

A Flight SQL server for integration testing. Uses DuckDB as the backend
to provide a fully-featured SQL engine with Flight SQL protocol support.

Usage:
    python flight_server.py [--host HOST] [--port PORT]

Requirements:
    pip install pyarrow duckdb
"""

import argparse
import re
import struct
from typing import Generator

import duckdb
import pyarrow as pa
import pyarrow.flight as flight


class FlightSQLServer(flight.FlightServerBase):
    """
    Flight SQL server backed by DuckDB.

    Implements the Flight SQL protocol for metadata and query execution.
    """

    # Flight SQL command type URLs
    CMD_GET_CATALOGS = b"type.googleapis.com/arrow.flight.protocol.sql.CommandGetCatalogs"
    CMD_GET_DB_SCHEMAS = b"type.googleapis.com/arrow.flight.protocol.sql.CommandGetDbSchemas"
    CMD_GET_TABLES = b"type.googleapis.com/arrow.flight.protocol.sql.CommandGetTables"
    CMD_GET_TABLE_TYPES = b"type.googleapis.com/arrow.flight.protocol.sql.CommandGetTableTypes"
    CMD_STATEMENT_QUERY = b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
    CMD_PREPARED_STATEMENT = b"type.googleapis.com/arrow.flight.protocol.sql.CommandPreparedStatementQuery"

    def __init__(self, host: str = "127.0.0.1", port: int = 8815, **kwargs):
        location = f"grpc://{host}:{port}"
        super().__init__(location, **kwargs)
        self._host = host
        self._port = port
        self._location_uri = location

        # Create in-memory DuckDB connection
        self.conn = duckdb.connect(":memory:")

        # Create test tables
        self._setup_test_data()
        self._setup_arrow_test_tables()

        print(f"[FlightSQL] Server started on {location}")

    def _setup_test_data(self):
        """Create test tables for integration tests."""
        self.conn.execute("CREATE TABLE test_data AS SELECT 1 AS value")
        self.conn.execute(
            "CREATE TABLE numbers AS SELECT i AS id, 'item_' || i AS name "
            "FROM range(1, 11) t(i)"
        )
        self.conn.execute(
            "CREATE TABLE types_test AS SELECT "
            "1 AS int_col, "
            "CAST(1.5 AS DOUBLE) AS float_col, "
            "'hello' AS str_col, "
            "TRUE AS bool_col, "
            "DATE '2024-01-15' AS date_col"
        )
        self.conn.execute(
            "CREATE TABLE decimal_test AS SELECT "
            "CAST(1234 AS DECIMAL(4,0)) AS dec_p4, "
            "CAST(123456789 AS DECIMAL(9,0)) AS dec_p9, "
            "CAST(123456789012345678 AS DECIMAL(18,0)) AS dec_p18, "
            "CAST('12345678901234567890123456789012345678' AS DECIMAL(38,0)) AS dec_p38, "
            "CAST(123.45 AS DECIMAL(10,2)) AS dec_p10s2"
        )
        self.conn.execute(
            "CREATE TABLE nested_test AS SELECT "
            "[1, 2, NULL]::INTEGER[] AS int_list, "
            "[]::INTEGER[] AS empty_list, "
            "struct_pack(a := 1, b := 'hi') AS simple_struct, "
            "struct_pack(a := NULL, b := 'bye') AS struct_with_null, "
            "map(['a','b'], [1,2]) AS str_int_map, "
            "map(['x'], [NULL::INTEGER]) AS map_with_null"
        )
        print("[FlightSQL] Test tables created: test_data, numbers, types_test, decimal_test, nested_test, "
              "dictionary_test, run_end_test")

    def _setup_arrow_test_tables(self):
        """Create Arrow tables for encoding-specific tests."""
        self.arrow_tables = {}

        dict_values = pa.array(["alpha", "beta", "gamma"])
        dict_indices = pa.array([0, 1, 0, 2, 1], type=pa.int8())
        dict_array = pa.DictionaryArray.from_arrays(dict_indices, dict_values)
        self.arrow_tables["dictionary_test"] = pa.table({"dict_col": dict_array})

        run_end_array = self._build_run_end_encoded_array()
        self.arrow_tables["run_end_test"] = pa.table({"ree_col": run_end_array})

        self.conn.execute("CREATE TABLE dictionary_test AS SELECT 'alpha'::VARCHAR AS dict_col LIMIT 0")
        self.conn.execute("CREATE TABLE run_end_test AS SELECT 1::BIGINT AS ree_col LIMIT 0")

    def _build_run_end_encoded_array(self):
        run_ends = pa.array([2, 4, 6], type=pa.int32())
        values = pa.array([1, 2, 3], type=pa.int64())
        if hasattr(pa, "run_end_encoded_array"):
            return pa.run_end_encoded_array(run_ends, values)
        if hasattr(pa, "RunEndEncodedArray"):
            return pa.RunEndEncodedArray.from_arrays(run_ends, values)
        return pa.array([1, 1, 2, 2, 3, 3], type=pa.int64())

    def _arrow_table_for_query(self, query: str):
        match = re.search(r"FROM\s+([^\s;]+)", query, flags=re.IGNORECASE)
        if not match:
            return None
        table_ref = match.group(1).strip().strip(";").replace('"', "")
        table_name = table_ref.split(".")[-1]
        return self.arrow_tables.get(table_name)

    def _rewrite_query(self, query: str) -> str:
        """Rewrite query to strip catalog names since DuckDB uses default catalog.

        Converts 3-part qualified names (catalog.schema.table) to 2-part (schema.table).
        E.g., "test"."main"."numbers" -> "main"."numbers"
        """
        # Pattern to match 3-part qualified table references: "catalog"."schema"."table" or catalog.schema.table
        # This handles both quoted and unquoted identifiers
        pattern = r'(?:FROM|JOIN)\s+("?\w+"?)\.("?\w+"?)\.("?\w+"?)'

        def replace_match(m):
            # Keep only schema.table (drop catalog)
            return f'FROM {m.group(2)}.{m.group(3)}'

        return re.sub(pattern, replace_match, query, flags=re.IGNORECASE)

    def _is_flight_sql_command(self, command: bytes) -> bool:
        """Check if the command is a Flight SQL protocol buffer."""
        # Just check if the type URL is present anywhere in the command
        # The protobuf encoding varies, so we can't rely on a specific start byte
        return b"type.googleapis.com/arrow.flight.protocol.sql" in command

    def _get_command_type(self, command: bytes) -> bytes:
        """Extract the command type URL from a Flight SQL command."""
        # Flight SQL commands are wrapped in an Any protobuf
        # Format: \n<length><type_url>\x12<length><payload>
        # Find the type URL
        for cmd_type in [
            self.CMD_GET_CATALOGS,
            self.CMD_GET_DB_SCHEMAS,
            self.CMD_GET_TABLES,
            self.CMD_GET_TABLE_TYPES,
            self.CMD_STATEMENT_QUERY,
            self.CMD_PREPARED_STATEMENT,
        ]:
            if cmd_type in command:
                return cmd_type
        return b""

    def _read_varint(self, data: bytes, idx: int) -> tuple:
        """Read a varint from bytes at index, return (value, new_idx)."""
        value = 0
        shift = 0
        while idx < len(data):
            byte = data[idx]
            idx += 1
            value |= (byte & 0x7F) << shift
            if (byte & 0x80) == 0:
                break
            shift += 7
        return value, idx

    def _extract_inner_value(self, command: bytes) -> bytes:
        """Extract the inner value (field 2) from an Any protobuf."""
        try:
            idx = 0
            while idx < len(command):
                if idx >= len(command):
                    break
                tag = command[idx]
                idx += 1

                field_num = tag >> 3
                wire_type = tag & 0x07

                if wire_type == 2:  # Length-delimited
                    length, idx = self._read_varint(command, idx)
                    field_value = command[idx:idx+length]
                    idx += length

                    if field_num == 2:  # This is the 'value' field of Any
                        return field_value
                elif wire_type == 0:  # Varint
                    _, idx = self._read_varint(command, idx)
                else:
                    break
        except Exception as e:
            print(f"[FlightSQL] Error extracting inner value: {e}")
        return b""

    def _extract_string_field(self, data: bytes, target_field_num: int) -> str:
        """Extract a string field from a protobuf message."""
        try:
            idx = 0
            while idx < len(data):
                if idx >= len(data):
                    break
                tag = data[idx]
                idx += 1

                field_num = tag >> 3
                wire_type = tag & 0x07

                if wire_type == 2:  # Length-delimited
                    length, idx = self._read_varint(data, idx)
                    field_value = data[idx:idx+length]
                    idx += length

                    if field_num == target_field_num:
                        return field_value.decode("utf-8")
                elif wire_type == 0:  # Varint
                    _, idx = self._read_varint(data, idx)
                else:
                    break
        except Exception as e:
            print(f"[FlightSQL] Error extracting string field: {e}")
        return ""

    def _extract_query_from_statement_command(self, command: bytes) -> str:
        """Extract the SQL query from a CommandStatementQuery protobuf.

        The command is an Any proto with:
        - Field 1 (type_url): string
        - Field 2 (value): bytes containing CommandStatementQuery

        CommandStatementQuery has:
        - Field 1 (query): string
        """
        inner_value = self._extract_inner_value(command)
        if not inner_value:
            return ""
        return self._extract_string_field(inner_value, 1)  # Field 1 is query

    def _extract_catalog_filter_from_command(self, command: bytes) -> str:
        """Extract the catalog filter from CommandGetDbSchemas or CommandGetTables.

        CommandGetDbSchemas has:
        - Field 1 (catalog): optional string

        CommandGetTables has:
        - Field 1 (catalog): optional string
        - Field 2 (db_schema_filter_pattern): optional string
        - Field 3 (table_name_filter_pattern): optional string
        - Field 4 (table_types): repeated string
        - Field 5 (include_schema): bool
        """
        inner_value = self._extract_inner_value(command)
        if not inner_value:
            return ""
        return self._extract_string_field(inner_value, 1)  # Field 1 is catalog

    def get_flight_info(self, context, descriptor):
        """Handle GetFlightInfo requests."""
        command = descriptor.command

        if self._is_flight_sql_command(command):
            cmd_type = self._get_command_type(command)
            print(f"[FlightSQL] GetFlightInfo for command type: {cmd_type.split(b'.')[-1].decode()}")

            if cmd_type == self.CMD_GET_DB_SCHEMAS:
                catalog_filter = self._extract_catalog_filter_from_command(command)
                return self._get_db_schemas_info(descriptor, catalog_filter)
            elif cmd_type == self.CMD_GET_TABLES:
                catalog_filter = self._extract_catalog_filter_from_command(command)
                return self._get_tables_info(descriptor, catalog_filter)
            elif cmd_type == self.CMD_GET_CATALOGS:
                return self._get_catalogs_info(descriptor)
            elif cmd_type == self.CMD_GET_TABLE_TYPES:
                return self._get_table_types_info(descriptor)
            elif cmd_type == self.CMD_STATEMENT_QUERY:
                query = self._extract_query_from_statement_command(command)
                if query:
                    return self._get_query_info(query, descriptor)

        # Try to decode as plain SQL query
        try:
            query = command.decode("utf-8")
            return self._get_query_info(query, descriptor)
        except UnicodeDecodeError:
            raise flight.FlightServerError(f"Unknown command format")

    def _get_query_info(self, query: str, descriptor) -> flight.FlightInfo:
        """Get FlightInfo for a SQL query."""
        query = self._rewrite_query(query)
        print(f"[FlightSQL] Preparing query: {query}")

        try:
            # Check if this query references a special Arrow table (e.g., dictionary_test)
            # If so, use that table's schema to preserve encoding information
            arrow_table = self._arrow_table_for_query(query)
            if arrow_table is not None:
                schema = arrow_table.schema
                total_records = arrow_table.num_rows
            else:
                result = self.conn.execute(query).fetch_arrow_table()
                schema = result.schema
                total_records = result.num_rows

            ticket = flight.Ticket(f"QUERY:{query}".encode("utf-8"))
            endpoint = flight.FlightEndpoint(ticket, [self._location_uri])

            return flight.FlightInfo(
                schema=schema,
                descriptor=descriptor,
                endpoints=[endpoint],
                total_records=total_records,
                total_bytes=-1,
            )
        except Exception as e:
            raise flight.FlightServerError(f"Failed to prepare query: {e}")

    def _get_db_schemas_info(self, descriptor, catalog_filter: str = "") -> flight.FlightInfo:
        """Return FlightInfo for GetDbSchemas command."""
        schema = pa.schema([
            ("catalog_name", pa.string()),
            ("db_schema_name", pa.string()),
        ])

        # Encode catalog filter in ticket
        ticket_data = f"CMD:GET_DB_SCHEMAS:{catalog_filter}"
        ticket = flight.Ticket(ticket_data.encode("utf-8"))
        endpoint = flight.FlightEndpoint(ticket, [self._location_uri])

        return flight.FlightInfo(
            schema=schema,
            descriptor=descriptor,
            endpoints=[endpoint],
            total_records=-1,
            total_bytes=-1,
        )

    def _get_tables_info(self, descriptor, catalog_filter: str = "") -> flight.FlightInfo:
        """Return FlightInfo for GetTables command."""
        schema = pa.schema([
            ("catalog_name", pa.string()),
            ("db_schema_name", pa.string()),
            ("table_name", pa.string()),
            ("table_type", pa.string()),
            ("table_schema", pa.binary()),  # Serialized Arrow schema
        ])

        # Encode catalog filter in ticket
        ticket_data = f"CMD:GET_TABLES:{catalog_filter}"
        ticket = flight.Ticket(ticket_data.encode("utf-8"))
        endpoint = flight.FlightEndpoint(ticket, [self._location_uri])

        return flight.FlightInfo(
            schema=schema,
            descriptor=descriptor,
            endpoints=[endpoint],
            total_records=-1,
            total_bytes=-1,
        )

    def _get_catalogs_info(self, descriptor) -> flight.FlightInfo:
        """Return FlightInfo for GetCatalogs command."""
        schema = pa.schema([("catalog_name", pa.string())])

        ticket = flight.Ticket(b"CMD:GET_CATALOGS")
        endpoint = flight.FlightEndpoint(ticket, [self._location_uri])

        return flight.FlightInfo(
            schema=schema,
            descriptor=descriptor,
            endpoints=[endpoint],
            total_records=-1,
            total_bytes=-1,
        )

    def _get_table_types_info(self, descriptor) -> flight.FlightInfo:
        """Return FlightInfo for GetTableTypes command."""
        schema = pa.schema([("table_type", pa.string())])

        ticket = flight.Ticket(b"CMD:GET_TABLE_TYPES")
        endpoint = flight.FlightEndpoint(ticket, [self._location_uri])

        return flight.FlightInfo(
            schema=schema,
            descriptor=descriptor,
            endpoints=[endpoint],
            total_records=-1,
            total_bytes=-1,
        )

    def do_get(self, context, ticket):
        """Handle DoGet requests."""
        ticket_bytes = ticket.ticket

        if ticket_bytes.startswith(b"QUERY:"):
            query = ticket_bytes[6:].decode("utf-8")
            query = self._rewrite_query(query)
            print(f"[FlightSQL] Executing query: {query}")
            arrow_table = self._arrow_table_for_query(query)
            if arrow_table is not None:
                return flight.RecordBatchStream(arrow_table)
            result = self.conn.execute(query).fetch_arrow_table()
            return flight.RecordBatchStream(result)

        elif ticket_bytes.startswith(b"CMD:GET_DB_SCHEMAS:"):
            catalog_filter = ticket_bytes[len(b"CMD:GET_DB_SCHEMAS:"):].decode("utf-8")
            print(f"[FlightSQL] Returning schemas (catalog_filter='{catalog_filter}')")

            # All catalogs have a 'main' schema
            all_catalogs = ["test", "alt"]

            if catalog_filter:
                # Filter to specific catalog
                catalogs = [c for c in all_catalogs if c == catalog_filter]
            else:
                # Return all catalogs
                catalogs = all_catalogs

            table = pa.table({
                "catalog_name": catalogs,
                "db_schema_name": ["main"] * len(catalogs),
            })
            return flight.RecordBatchStream(table)

        elif ticket_bytes.startswith(b"CMD:GET_TABLES:"):
            catalog_filter = ticket_bytes[len(b"CMD:GET_TABLES:"):].decode("utf-8")
            print(f"[FlightSQL] Returning tables (catalog_filter='{catalog_filter}')")

            # Get all tables from DuckDB
            tables = self.conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
            ).fetchall()

            table_names = [t[0] for t in tables]
            table_schemas = []

            for table_name in table_names:
                # Get the schema for each table (preserve Arrow encoding for special tables)
                arrow_table = self.arrow_tables.get(table_name)
                if arrow_table is not None:
                    schema = arrow_table.schema
                else:
                    result = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 0").fetch_arrow_table()
                    schema = result.schema
                # Serialize the schema using IPC
                sink = pa.BufferOutputStream()
                writer = pa.ipc.new_stream(sink, schema)
                writer.close()
                schema_bytes = sink.getvalue().to_pybytes()
                table_schemas.append(schema_bytes)

            # Tables exist in BOTH 'test' and 'alt' catalogs for backward compatibility
            # This allows queries to either remote.main.table or remote_test.main.table
            all_catalogs = ["test", "alt"]

            if catalog_filter:
                if catalog_filter in all_catalogs:
                    catalogs_for_tables = [catalog_filter] * len(table_names)
                else:
                    # No tables in this catalog
                    table_names = []
                    table_schemas = []
                    catalogs_for_tables = []
            else:
                # Return tables for all catalogs - duplicate tables for each catalog
                original_table_names = table_names[:]
                original_table_schemas = table_schemas[:]
                table_names = []
                table_schemas = []
                catalogs_for_tables = []
                for cat in all_catalogs:
                    table_names.extend(original_table_names)
                    table_schemas.extend(original_table_schemas)
                    catalogs_for_tables.extend([cat] * len(original_table_names))

            if table_names:
                table = pa.table({
                    "catalog_name": catalogs_for_tables,
                    "db_schema_name": ["main"] * len(table_names),
                    "table_name": table_names,
                    "table_type": ["TABLE"] * len(table_names),
                    "table_schema": table_schemas,
                })
            else:
                # Return empty table with correct schema
                table = pa.table({
                    "catalog_name": pa.array([], type=pa.string()),
                    "db_schema_name": pa.array([], type=pa.string()),
                    "table_name": pa.array([], type=pa.string()),
                    "table_type": pa.array([], type=pa.string()),
                    "table_schema": pa.array([], type=pa.binary()),
                })
            return flight.RecordBatchStream(table)

        elif ticket_bytes == b"CMD:GET_CATALOGS":
            print("[FlightSQL] Returning catalogs")
            table = pa.table({"catalog_name": [""]})
            return flight.RecordBatchStream(table)

        elif ticket_bytes == b"CMD:GET_TABLE_TYPES":
            print("[FlightSQL] Returning table types")
            table = pa.table({"table_type": ["TABLE", "VIEW"]})
            return flight.RecordBatchStream(table)

        else:
            # Try as raw query
            try:
                query = ticket_bytes.decode("utf-8")
                print(f"[FlightSQL] Executing raw query: {query}")
                arrow_table = self._arrow_table_for_query(query)
                if arrow_table is not None:
                    return flight.RecordBatchStream(arrow_table)
                result = self.conn.execute(query).fetch_arrow_table()
                return flight.RecordBatchStream(result)
            except Exception as e:
                raise flight.FlightServerError(f"Unknown ticket: {e}")


def main():
    parser = argparse.ArgumentParser(description="Flight SQL Test Server")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8815, help="Port to bind to")
    args = parser.parse_args()

    server = FlightSQLServer(host=args.host, port=args.port)

    print(f"[FlightSQL] Serving on grpc://{args.host}:{args.port}")
    print("[FlightSQL] Press Ctrl+C to stop")

    try:
        server.serve()
    except KeyboardInterrupt:
        print("\n[FlightSQL] Shutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
