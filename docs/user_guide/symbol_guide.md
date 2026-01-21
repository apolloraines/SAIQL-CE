# SAIQL Symbol Guide: Community Edition Reference

This guide provides documentation for all SAIQL CE symbols and operators, their meanings, and usage examples.

## Table of Contents
- [Core Query Operators](#core-query-operators)
- [Limit and Selection](#limit-and-selection)
- [Column Specification](#column-specification)
- [Filtering and Conditions](#filtering-and-conditions)
- [Output Formatting](#output-formatting)
- [Aggregation Functions](#aggregation-functions)
- [Advanced Operators](#advanced-operators)
- [Text Search (QIPI-Lite)](#text-search-qipi-lite)

## Core Query Operators

### `*` - Query Prefix
**Purpose**: Initiates a SAIQL query
**Usage**: `*[table_name]` or `*LIMIT[table_name]`
**Examples**:
```sql
*[users]              -- Query users table
*5[products]          -- Query products table with limit 5
*COUNT[orders]        -- Count query on orders table
```

### `[table_name]` - Table Specification
**Purpose**: Specifies the target table or data source
**Usage**: `*[table_name]::columns`
**Examples**:
```sql
*[users]::name,email
*[products]::*
*[orders]::order_id,amount
```

### `::` - Column Separator
**Purpose**: Separates table specification from column list
**Usage**: `*[table]::column1,column2`
**Examples**:
```sql
*[users]::name,email,created_at
*[products]::name,price
*[orders]::*                    -- All columns
```

### `>>` - Query Pipeline Operator
**Purpose**: Pipes query results to output format
**Usage**: `query>>output`
**Examples**:
```sql
*5[users]::name,email>>oQ
*[products]::*>>oJ
*[users]::*|status='active'>>oQ
```

## Limit and Selection

### Numeric Limits
**Purpose**: Limit the number of results returned
**Usage**: `*LIMIT[table]`
**Examples**:
```sql
*1[users]::*          -- Return 1 user
*10[products]::name   -- Return 10 products
*100[orders]::*       -- Return 100 orders
*1000[events]::*      -- Return 1000 events
```

### Special Limits
```sql
*ALL[table]::*        -- Return all records (use with caution)
*FIRST[table]::*      -- Return first record only
*LAST[table]::*       -- Return last record only
*RANDOM[table]::*     -- Return random record
```

## Column Specification

### `*` - All Columns
**Purpose**: Select all columns from the table
**Usage**: `*[table]::*`
**Examples**:
```sql
*5[users]::*          -- All columns from users table
*[products]::*        -- All columns from products table
```

### Column Lists
**Purpose**: Select specific columns
**Usage**: `*[table]::col1,col2,col3`
**Examples**:
```sql
*[users]::name,email,created_at
*[products]::id,name,price,category
*[orders]::order_id,user_id,amount,status
```

### Column Aliases
**Purpose**: Rename columns in output
**Usage**: `*[table]::column AS alias`
**Examples**:
```sql
*[users]::name AS full_name,email AS email_address
*[products]::price AS cost,category AS type
```

## Filtering and Conditions

### `|` - Filter Operator
**Purpose**: Apply filters to query results
**Usage**: `*[table]::columns|condition`
**Examples**:
```sql
*[users]::*|status='active'
*[products]::*|price>100
*[orders]::*|created_at>'2024-01-01'
```

### Comparison Operators
```sql
=                     -- Equal to
!=                    -- Not equal to
>                     -- Greater than
<                     -- Less than
>=                    -- Greater than or equal
<=                    -- Less than or equal
```

### Logical Operators
```sql
AND                   -- Logical AND
OR                    -- Logical OR
NOT                   -- Logical NOT
IN(value1,value2)     -- Value in list
BETWEEN(val1,val2)    -- Value between range
```

### Pattern Matching
```sql
LIKE('%pattern%')     -- SQL-style pattern matching
CONTAINS('text')      -- Text contains substring
STARTSWITH('prefix')  -- Text starts with prefix
ENDSWITH('suffix')    -- Text ends with suffix
```

## Output Formatting

### `oQ` - Standard Output
**Purpose**: Standard query output format
**Usage**: `query>>oQ`
**Examples**:
```sql
*5[users]::name,email>>oQ
*[products]::*>>oQ
```

### Output Format Modifiers
```sql
>>oQ                  -- Standard query output
>>oJ                  -- JSON output format
>>oT                  -- Table/tabular output
>>oC                  -- CSV output format
```

## Aggregation Functions

### Basic Aggregations
```sql
*COUNT[table]::*                   -- Count all records
*SUM[table]::amount                -- Sum of amount column
*AVG[table]::price                 -- Average of price column
*MIN[table]::created_at            -- Minimum value
*MAX[table]::updated_at            -- Maximum value
```

### Advanced Aggregations
```sql
*DISTINCT[table]::category         -- Distinct values
*GROUP_COUNT[table]::status        -- Count grouped by status
```

## Advanced Operators

### Temporal Operators
```sql
RECENT(timespan)                   -- Recent time period
RECENT(7d)                         -- Last 7 days
RECENT(1h)                         -- Last 1 hour
RECENT(30m)                        -- Last 30 minutes

OLDER_THAN(timespan)               -- Older than timespan
BETWEEN_DATES(start, end)          -- Date range filter
```

### String Operations
```sql
CONCAT(field1, field2)             -- Concatenate strings
SUBSTRING(field, start, length)    -- Extract substring
UPPER(field)                       -- Convert to uppercase
LOWER(field)                       -- Convert to lowercase
TRIM(field)                        -- Remove whitespace
```

### Mathematical Operations
```sql
ROUND(field, decimals)             -- Round number
FLOOR(field)                       -- Round down
CEIL(field)                        -- Round up
ABS(field)                         -- Absolute value
```

## Text Search (QIPI-Lite)

CE includes QIPI-Lite for full-text search using SQLite FTS5.

### Basic Text Search
```sql
*[documents]::*|SEARCH('keyword')>>oQ
*[products]::name,description|SEARCH('wireless')>>oQ
```

### Text Search with Ranking
```sql
*10[articles]::title,body|SEARCH('machine learning')>>oQ
```

## Symbol Precedence and Parsing Rules

### Operator Precedence (High to Low)
1. `[]` - Table specification
2. `::` - Column separation
3. `|` - Filtering
4. `>>` - Pipeline operations
5. Logical operators (`AND`, `OR`, `NOT`)
6. Comparison operators (`=`, `>`, `<`, etc.)

### Parsing Rules
- Queries must start with `*`
- Table names must be enclosed in `[]`
- Column specifications follow `::`
- Filters follow `|`
- Pipeline operations use `>>`
- Whitespace is generally ignored
- Quotes preserve literal values
- Parentheses override precedence

## Error Handling and Debugging

### Common Syntax Errors
```sql
-- Missing query prefix
[users]::name              -- ERROR: Must start with *

-- Missing table brackets
*users::name               -- ERROR: Table must be in []

-- Missing column separator
*[users]name               -- ERROR: Missing ::

-- Invalid operator placement
*[users]::name|>oQ         -- ERROR: Invalid pipeline
```

### Debug Symbols
```sql
>>DEBUG                    -- Enable debug output
>>EXPLAIN                  -- Show query execution plan
>>VALIDATE                 -- Validate query syntax only
```

## Query Examples

### Multi-condition Filtering
```sql
*50[users]::name,email,status|
  status='active' AND
  created_at>'2024-01-01' AND
  email LIKE('%@company.com')>>oQ
```

### Aggregation with Filter
```sql
*COUNT[orders]::*|status='completed'>>oQ
*SUM[orders]::amount|customer_id=123>>oQ
```

### Text Search
```sql
*10[products]::name,price|SEARCH('bluetooth speaker')>>oQ
```

## Best Practices

### Performance
1. **Use limits** for large tables: `*100[table]` vs `*[table]`
2. **Specific columns** vs `*`: `::name,email` vs `::*`
3. **Effective filtering**: Filter early to reduce result sets

### Query Readability
1. **Consistent spacing** around operators
2. **Logical grouping** with parentheses
3. **Meaningful aliases** for complex expressions

---

This symbol guide covers SAIQL Community Edition operators. For advanced features like semantic search, vector similarity, and AI/ML operators, see the Full Edition documentation.
