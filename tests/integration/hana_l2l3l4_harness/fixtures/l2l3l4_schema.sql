-- HANA L2/L3/L4 Integration Test Fixture
-- Requires Phase 07 base tables (customers, products, orders, order_items)
--
-- Schema Strategy: Fresh schema per run_id (SAIQL_L2L3L4_TEST_<run_id>)
-- Privilege Requirements: See harness_config.json
--
-- Objects:
--   L2 Views: 4 (customer_summary, active_customers, high_balance_active, order_summary)
--   L3 Procedures: 2 (update_customer_status, get_customer_count)
--   L3 Functions: 2 (get_customer_balance, calculate_discount_price)
--   L4 Triggers: 3 (trg_upper_lastname, trg_trim_email, trg_order_audit)

-- ============================================================
-- CLEANUP: Drop existing objects for clean state
-- ============================================================

-- Drop triggers first (depend on tables)
DROP TRIGGER trg_order_audit;
DROP TRIGGER trg_trim_email;
DROP TRIGGER trg_upper_lastname;

-- Drop views (in dependency order - child first)
DROP VIEW high_balance_active;
DROP VIEW order_summary;
DROP VIEW active_customers;
DROP VIEW customer_summary;

-- Drop routines
DROP PROCEDURE update_customer_status;
DROP PROCEDURE get_customer_count;
DROP FUNCTION get_customer_balance;
DROP FUNCTION calculate_discount_price;

-- Drop audit table
DROP TABLE audit_log;

-- ============================================================
-- L2: VIEWS (SQL Views only - Calculation Views NOT supported)
-- ============================================================

-- View 1: Simple SELECT (supported subset)
CREATE VIEW customer_summary AS
SELECT
    customer_id,
    first_name || ' ' || last_name AS full_name,
    email,
    is_active,
    credit_score,
    account_balance
FROM customers;

-- View 2: Simple SELECT with WHERE (supported subset)
CREATE VIEW active_customers AS
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    account_balance
FROM customers
WHERE is_active = TRUE;

-- View 3: View-on-view (tests dependency ordering)
CREATE VIEW high_balance_active AS
SELECT
    customer_id,
    full_name,
    email,
    account_balance
FROM customer_summary
WHERE is_active = TRUE AND account_balance > 1000;

-- View 4: Simple JOIN (supported subset - equality only)
CREATE VIEW order_summary AS
SELECT
    o.order_id,
    o.order_date,
    c.first_name || ' ' || c.last_name AS customer_name,
    o.total_amount,
    o.status
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id;

-- ============================================================
-- L3: PROCEDURES (SQLScript)
-- ============================================================

-- Procedure 1: Simple UPDATE with parameters
CREATE PROCEDURE update_customer_status(
    IN p_customer_id INTEGER,
    IN p_is_active BOOLEAN
)
LANGUAGE SQLSCRIPT
AS
BEGIN
    UPDATE customers
    SET is_active = :p_is_active
    WHERE customer_id = :p_customer_id;
END;

-- Procedure 2: Simple SELECT INTO with OUT parameter
CREATE PROCEDURE get_customer_count(
    OUT p_count INTEGER
)
LANGUAGE SQLSCRIPT
AS
BEGIN
    SELECT COUNT(*) INTO p_count FROM customers;
END;

-- ============================================================
-- L3: FUNCTIONS (SQLScript)
-- ============================================================

-- Function 1: Scalar function returning DECIMAL
CREATE FUNCTION get_customer_balance(p_customer_id INTEGER)
RETURNS result DECIMAL(15,2)
LANGUAGE SQLSCRIPT
AS
BEGIN
    SELECT account_balance INTO result
    FROM customers
    WHERE customer_id = :p_customer_id;
END;

-- Function 2: Scalar function with calculation
CREATE FUNCTION calculate_discount_price(p_price DECIMAL(10,2), p_discount_percent DECIMAL(5,2))
RETURNS result DECIMAL(10,2)
LANGUAGE SQLSCRIPT
AS
BEGIN
    result := :p_price * (1 - :p_discount_percent / 100);
END;

-- ============================================================
-- L4: TRIGGERS (Conservative Subset)
-- ============================================================

-- Trigger 1: BEFORE INSERT normalization - SUPPORTED subset
-- Simple UPPER() on name column
CREATE TRIGGER trg_upper_lastname
BEFORE INSERT ON customers
REFERENCING NEW ROW NEWROW
FOR EACH ROW
BEGIN
    NEWROW.last_name = UPPER(:NEWROW.last_name);
END;

-- Trigger 2: BEFORE UPDATE normalization - SUPPORTED subset
-- Simple TRIM() on email column
CREATE TRIGGER trg_trim_email
BEFORE UPDATE ON customers
REFERENCING NEW ROW NEWROW
FOR EACH ROW
BEGIN
    NEWROW.email = TRIM(:NEWROW.email);
END;

-- Trigger 3: AFTER INSERT audit - UNSUPPORTED (complex, DML)
-- This should be classified as unsupported and stubbed
CREATE TABLE audit_log (
    audit_id INTEGER PRIMARY KEY,
    table_name NVARCHAR(100),
    operation NVARCHAR(20),
    record_id INTEGER,
    audit_timestamp TIMESTAMP
);

CREATE TRIGGER trg_order_audit
AFTER INSERT ON orders
REFERENCING NEW ROW NEWROW
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (audit_id, table_name, operation, record_id, audit_timestamp)
    VALUES (
        (SELECT COALESCE(MAX(audit_id), 0) + 1 FROM audit_log),
        'orders',
        'INSERT',
        :NEWROW.order_id,
        CURRENT_TIMESTAMP
    );
END;

-- ============================================================
-- SEED DATA FOR VALIDATION
-- ============================================================

-- Remove existing trigger test customer if present
DELETE FROM customers WHERE customer_id = 100;

-- Additional test customer for trigger validation
INSERT INTO customers (
    customer_id, first_name, last_name, email, phone,
    is_active, credit_score, account_balance, created_at, updated_at,
    profile_data, notes
) VALUES (
    100,
    'Trigger',
    'test',
    'trigger.test@example.com',
    '+1-555-9999',
    TRUE,
    500,
    500.00,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    'Test customer for trigger validation',
    'Used to verify BEFORE INSERT trigger'
);

COMMIT;
