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
        match = re.search(r"FROM\\s+([^\\s;]+)", query, flags=re.IGNORECASE)
        if not match:
            return None
        table_ref = match.group(1).strip().strip(";").replace('"', "")
        table_name = table_ref.split(".")[-1]
        return self.arrow_tables.get(table_name)

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

    def _extract_query_from_statement_command(self, command: bytes) -> str:
        """Extract the SQL query from a CommandStatementQuery protobuf.

        The command is an Any proto with:
        - Field 1 (type_url): string
        - Field 2 (value): bytes containing CommandStatementQuery

        CommandStatementQuery has:
        - Field 1 (query): string
        """
        try:
            idx = 0
            inner_value = None

            # Parse the Any proto
            while idx < len(command):
                # Read field tag
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
                        inner_value = field_value
                        break
                elif wire_type == 0:  # Varint
                    _, idx = self._read_varint(command, idx)
                else:
                    # Skip unknown wire types
                    break

            if inner_value is None:
                return ""

            # Now parse CommandStatementQuery from inner_value
            idx = 0
            while idx < len(inner_value):
                if idx >= len(inner_value):
                    break
                tag = inner_value[idx]
                idx += 1

                field_num = tag >> 3
                wire_type = tag & 0x07

                if wire_type == 2:  # Length-delimited
                    length, idx = self._read_varint(inner_value, idx)
                    field_value = inner_value[idx:idx+length]
                    idx += length

                    if field_num == 1:  # This is the 'query' field
                        return field_value.decode("utf-8")
                elif wire_type == 0:  # Varint
                    _, idx = self._read_varint(inner_value, idx)
                else:
                    break

        except Exception as e:
            print(f"[FlightSQL] Error extracting query: {e}")
            import traceback
            traceback.print_exc()
        return ""

    def get_flight_info(self, context, descriptor):
        """Handle GetFlightInfo requests."""
        command = descriptor.command

        if self._is_flight_sql_command(command):
            cmd_type = self._get_command_type(command)
            print(f"[FlightSQL] GetFlightInfo for command type: {cmd_type.split(b'.')[-1].decode()}")

            if cmd_type == self.CMD_GET_DB_SCHEMAS:
                return self._get_db_schemas_info(descriptor)
            elif cmd_type == self.CMD_GET_TABLES:
                return self._get_tables_info(descriptor)
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
        print(f"[FlightSQL] Preparing query: {query}")

        try:
            result = self.conn.execute(query).fetch_arrow_table()
            schema = result.schema

            ticket = flight.Ticket(f"QUERY:{query}".encode("utf-8"))
            endpoint = flight.FlightEndpoint(ticket, [self._location_uri])

            return flight.FlightInfo(
                schema=schema,
                descriptor=descriptor,
                endpoints=[endpoint],
                total_records=result.num_rows,
                total_bytes=-1,
            )
        except Exception as e:
            raise flight.FlightServerError(f"Failed to prepare query: {e}")

    def _get_db_schemas_info(self, descriptor) -> flight.FlightInfo:
        """Return FlightInfo for GetDbSchemas command."""
        schema = pa.schema([
            ("catalog_name", pa.string()),
            ("db_schema_name", pa.string()),
        ])

        ticket = flight.Ticket(b"CMD:GET_DB_SCHEMAS")
        endpoint = flight.FlightEndpoint(ticket, [self._location_uri])

        return flight.FlightInfo(
            schema=schema,
            descriptor=descriptor,
            endpoints=[endpoint],
            total_records=-1,
            total_bytes=-1,
        )

    def _get_tables_info(self, descriptor) -> flight.FlightInfo:
        """Return FlightInfo for GetTables command."""
        schema = pa.schema([
            ("catalog_name", pa.string()),
            ("db_schema_name", pa.string()),
            ("table_name", pa.string()),
            ("table_type", pa.string()),
            ("table_schema", pa.binary()),  # Serialized Arrow schema
        ])

        ticket = flight.Ticket(b"CMD:GET_TABLES")
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
            print(f"[FlightSQL] Executing query: {query}")
            arrow_table = self._arrow_table_for_query(query)
            if arrow_table is not None:
                return flight.RecordBatchStream(arrow_table)
            result = self.conn.execute(query).fetch_arrow_table()
            return flight.RecordBatchStream(result)

        elif ticket_bytes == b"CMD:GET_DB_SCHEMAS":
            print("[FlightSQL] Returning schemas")
            table = pa.table({
                "catalog_name": [""],
                "db_schema_name": ["main"],
            })
            return flight.RecordBatchStream(table)

        elif ticket_bytes == b"CMD:GET_TABLES":
            print("[FlightSQL] Returning tables")
            # Get all tables from DuckDB
            tables = self.conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
            ).fetchall()

            table_names = [t[0] for t in tables]
            table_schemas = []

            for table_name in table_names:
                # Get the schema for each table
                result = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 0").fetch_arrow_table()
                # Serialize the schema using IPC
                sink = pa.BufferOutputStream()
                writer = pa.ipc.new_stream(sink, result.schema)
                writer.close()
                schema_bytes = sink.getvalue().to_pybytes()
                table_schemas.append(schema_bytes)

            table = pa.table({
                "catalog_name": [""] * len(table_names),
                "db_schema_name": ["main"] * len(table_names),
                "table_name": table_names,
                "table_type": ["TABLE"] * len(table_names),
                "table_schema": table_schemas,
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
