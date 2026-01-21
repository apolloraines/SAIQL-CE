-- Phase 07 Integration Test Fixture
-- SAP HANA test schema with comprehensive type coverage

-- Drop tables if they exist (reverse FK order)
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS type_test;

-- 1. Customers Table (Primary test table with various types)
CREATE COLUMN TABLE customers (
    customer_id INTEGER NOT NULL PRIMARY KEY,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    phone CHAR(15),
    is_active BOOLEAN DEFAULT TRUE,
    credit_score SMALLINT,
    account_balance DECIMAL(15,2),
    created_at TIMESTAMP NOT NULL,
    updated_at SECONDDATE,
    profile_data CLOB,
    notes VARCHAR(500)
);

-- 2. Products Table (Test PK/FK relationships)
CREATE COLUMN TABLE products (
    product_id INTEGER NOT NULL PRIMARY KEY,
    product_name NVARCHAR(100) NOT NULL,
    description NCLOB,
    price DECIMAL(10,2) NOT NULL,
    quantity_in_stock INTEGER DEFAULT 0,
    weight_kg DOUBLE,
    is_available BOOLEAN DEFAULT TRUE,
    sku VARCHAR(50) UNIQUE,
    created_at TIMESTAMP
);

-- 3. Orders Table (FK to customers)
CREATE COLUMN TABLE orders (
    order_id INTEGER NOT NULL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    ship_date DATE,
    order_time TIME,
    total_amount DECIMAL(12,2),
    status VARCHAR(20),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- 4. Order Items Table (Composite FK, many-to-many)
CREATE COLUMN TABLE order_items (
    order_item_id INTEGER NOT NULL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_percent DECIMAL(5,2) DEFAULT 0.00,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- 5. Type Test Table (Comprehensive type coverage for Phase 07)
CREATE COLUMN TABLE type_test (
    test_id INTEGER NOT NULL PRIMARY KEY,

    -- Integer types
    col_tinyint TINYINT,
    col_smallint SMALLINT,
    col_integer INTEGER,
    col_bigint BIGINT,

    -- Floating point
    col_real REAL,
    col_double DOUBLE,

    -- Decimal types
    col_decimal DECIMAL(20,4),
    col_smalldecimal SMALLDECIMAL,

    -- String types
    col_char CHAR(10),
    col_nchar NCHAR(10),
    col_varchar VARCHAR(100),
    col_nvarchar NVARCHAR(100),
    col_clob CLOB,
    col_nclob NCLOB,

    -- Date/Time types
    col_date DATE,
    col_time TIME,
    col_timestamp TIMESTAMP,
    col_seconddate SECONDDATE,

    -- Binary types
    col_binary BINARY(16),
    col_varbinary VARBINARY(100),
    col_blob BLOB,

    -- Boolean
    col_boolean BOOLEAN,

    -- Nullable test
    col_nullable_int INTEGER
);

-- Insert test data
INSERT INTO customers VALUES (
    1,
    'Alice',
    'Johnson',
    'alice@example.com',
    '+1-555-0001',
    TRUE,
    750,
    1250.50,
    '2024-01-15 10:30:00',
    '2024-06-20 14:22:33',
    'Customer profile with detailed purchase history and preferences',
    'VIP customer since 2020'
);

INSERT INTO customers VALUES (
    2,
    'Bob',
    'Smith',
    'bob@example.com',
    '+1-555-0002',
    TRUE,
    680,
    -150.25,
    '2024-03-22 09:15:00',
    '2024-07-10 11:05:12',
    'Frequent buyer of electronics',
    'Requested phone support'
);

INSERT INTO customers VALUES (
    3,
    'Charlie',
    'Brown',
    'charlie@example.com',
    '+1-555-0003',
    FALSE,
    NULL,
    0.00,
    '2023-11-05 16:45:00',
    '2023-11-05 16:45:00',
    NULL,
    'Account suspended - payment issues'
);

INSERT INTO products VALUES (
    101,
    'Laptop Pro 15',
    'High-performance laptop with 16GB RAM and 512GB SSD storage. Perfect for professionals and developers.',
    1299.99,
    25,
    1.8,
    TRUE,
    'LAP-PRO-15-001',
    '2024-01-10 08:00:00'
);

INSERT INTO products VALUES (
    102,
    'Wireless Mouse',
    'Ergonomic wireless mouse with 3-button design and long battery life.',
    29.99,
    150,
    0.12,
    TRUE,
    'MOU-WIR-001',
    '2024-01-10 08:00:00'
);

INSERT INTO products VALUES (
    103,
    'USB-C Hub',
    'Multi-port USB-C hub with HDMI, USB 3.0, and ethernet connectivity.',
    49.99,
    0,
    0.25,
    FALSE,
    'HUB-USC-001',
    '2024-02-15 10:30:00'
);

INSERT INTO orders VALUES (
    1001,
    1,
    '2024-07-01',
    '2024-07-03',
    '14:30:00',
    1329.98,
    'SHIPPED'
);

INSERT INTO orders VALUES (
    1002,
    2,
    '2024-07-15',
    NULL,
    '09:15:00',
    79.98,
    'PENDING'
);

INSERT INTO order_items VALUES (1, 1001, 101, 1, 1299.99, 0.00);
INSERT INTO order_items VALUES (2, 1001, 102, 1, 29.99, 0.00);
INSERT INTO order_items VALUES (3, 1002, 102, 2, 29.99, 5.00);
INSERT INTO order_items VALUES (4, 1002, 103, 1, 49.99, 10.00);

-- Insert comprehensive type test data
INSERT INTO type_test VALUES (
    1,
    -- Integer types
    255,                    -- TINYINT (max)
    32767,                  -- SMALLINT (max)
    2147483647,            -- INTEGER (max)
    9223372036854775807,   -- BIGINT (max)

    -- Floating point
    3.14159,               -- REAL
    2.718281828459045,     -- DOUBLE

    -- Decimal types
    12345.6789,            -- DECIMAL(20,4)
    9999999999.99,         -- SMALLDECIMAL

    -- String types
    'CHAR10    ',          -- CHAR(10)
    'NCHAR10   ',          -- NCHAR(10)
    'Variable length string',  -- VARCHAR
    'Unicode: 你好世界 🌍',     -- NVARCHAR
    'Large text content for CLOB testing with multiple lines and paragraphs',  -- CLOB
    'Unicode CLOB: Ñoño café résumé',  -- NCLOB

    -- Date/Time types
    '2024-12-25',          -- DATE
    '14:30:45',            -- TIME
    '2024-07-20 18:45:30', -- TIMESTAMP
    '2024-07-20 18:45:30', -- SECONDDATE

    -- Binary types (hex representation)
    '0123456789ABCDEF0123456789ABCDEF',  -- BINARY(16)
    '48656C6C6F',                          -- VARBINARY (Hello)
    '504E470D0A1A0A',                      -- BLOB (PNG header)

    -- Boolean
    TRUE,

    -- Nullable
    NULL
);

INSERT INTO type_test VALUES (
    2,
    -- Test MIN values and NULL handling
    0,                     -- TINYINT
    -32768,                -- SMALLINT (min)
    -2147483648,           -- INTEGER (min)
    -9223372036854775808,  -- BIGINT (min)

    -999.999,              -- REAL
    -1.23456789012345,     -- DOUBLE

    -9999.9999,            -- DECIMAL
    -9999999999.99,        -- SMALLDECIMAL

    '',                    -- CHAR (empty)
    '',                    -- NCHAR (empty)
    '',                    -- VARCHAR (empty)
    '',                    -- NVARCHAR (empty)
    '',                    -- CLOB (empty)
    '',                    -- NCLOB (empty)

    '1970-01-01',          -- DATE (epoch)
    '00:00:00',            -- TIME (midnight)
    '1970-01-01 00:00:00', -- TIMESTAMP (epoch)
    '1970-01-01 00:00:00', -- SECONDDATE (epoch)

    '00000000000000000000000000000000',  -- BINARY (zeros)
    '',                                   -- VARBINARY (empty)
    '',                                   -- BLOB (empty)

    FALSE,

    42  -- Non-null value
);

-- Create unique constraint for testing
ALTER TABLE customers ADD CONSTRAINT uk_customer_email UNIQUE (email);

-- Commit
COMMIT;
