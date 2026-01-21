#!/bin/bash
# HANA L2/L3/L4 Harness Setup Script
#
# Prerequisites:
# - Phase 07 HANA Docker container running (saiql_hana_test)
# - HANA Express initialized with base tables
#
# This script:
# 1. Creates the L2/L3/L4 test user with required privileges
# 2. Loads the L2/L3/L4 fixtures (views, procedures, functions, triggers)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/../fixtures"
PHASE07_FIXTURES="$SCRIPT_DIR/../../phase07_hana_harness/fixtures"

# HANA hdbsql path (via symlink to current version)
HDBSQL_PATH="/hana/shared/HXE/exe/linuxx86_64/hdb/hdbsql"

# Connection defaults (can be overridden via environment)
HANA_HOST="${HANA_HOST:-localhost}"
HANA_PORT="${HANA_PORT:-39017}"
HANA_ADMIN_USER="${HANA_ADMIN_USER:-SYSTEM}"
HANA_ADMIN_PASSWORD="${HANA_ADMIN_PASSWORD:-SaiqlTest123}"
HANA_DATABASE="${HANA_DATABASE:-HXE}"

# L2L3L4 Test user
L2L3L4_USER="SAIQL_L2L3L4_TEST"
L2L3L4_PASSWORD="L2L3L4Test123"

echo "=============================================="
echo "HANA L2/L3/L4 Harness Setup"
echo "=============================================="
echo "Host: $HANA_HOST:$HANA_PORT"
echo "Database: $HANA_DATABASE"
echo "Admin User: $HANA_ADMIN_USER"
echo "Test User: $L2L3L4_USER"
echo ""

# Check if HANA container is running
if ! docker ps | grep -q saiql_hana_test; then
    echo "ERROR: HANA container 'saiql_hana_test' not running."
    echo "Please run Phase 07 setup first:"
    echo "  cd tests/integration/phase07_hana_harness/scripts"
    echo "  ./setup_hana_docker.sh"
    exit 1
fi

echo "Step 1: Creating L2L3L4 test user with privileges..."
# Drop user if exists (ignore error if user doesn't exist)
docker exec saiql_hana_test $HDBSQL_PATH -i 90 -d "$HANA_DATABASE" -u "$HANA_ADMIN_USER" -p "$HANA_ADMIN_PASSWORD" \
    "DROP USER $L2L3L4_USER CASCADE" 2>/dev/null || true

docker exec saiql_hana_test $HDBSQL_PATH -i 90 -d "$HANA_DATABASE" -u "$HANA_ADMIN_USER" -p "$HANA_ADMIN_PASSWORD" <<EOF
-- Create test user
CREATE USER $L2L3L4_USER PASSWORD '$L2L3L4_PASSWORD' NO FORCE_FIRST_PASSWORD_CHANGE;

-- Grant system privileges
GRANT CREATE SCHEMA TO $L2L3L4_USER;

-- Grant catalog read for introspection
GRANT CATALOG READ TO $L2L3L4_USER;

-- Grant SELECT on system views
GRANT SELECT ON SCHEMA SYS TO $L2L3L4_USER;

COMMIT;
EOF

echo "Step 2: Ensuring Phase 07 base tables exist..."
if ! docker exec saiql_hana_test $HDBSQL_PATH -i 90 -d "$HANA_DATABASE" -u "$HANA_ADMIN_USER" -p "$HANA_ADMIN_PASSWORD" \
    "SELECT COUNT(*) FROM customers" 2>/dev/null | grep -q "3"; then
    echo "Loading Phase 07 base fixtures..."
    docker cp "$PHASE07_FIXTURES/test_schema.sql" saiql_hana_test:/tmp/test_schema.sql
    docker exec saiql_hana_test $HDBSQL_PATH -i 90 -d "$HANA_DATABASE" -u "$HANA_ADMIN_USER" -p "$HANA_ADMIN_PASSWORD" -I /tmp/test_schema.sql
fi

echo "Step 3: Loading L2/L3/L4 fixtures..."
# Copy fixture to container and execute with -I flag for proper multi-line processing
docker cp "$FIXTURES_DIR/l2l3l4_schema.sql" saiql_hana_test:/tmp/l2l3l4_schema.sql
# Use -e to continue on errors (DROP statements may fail if objects don't exist)
docker exec saiql_hana_test $HDBSQL_PATH -i 90 -d "$HANA_DATABASE" -u "$HANA_ADMIN_USER" -p "$HANA_ADMIN_PASSWORD" -e -I /tmp/l2l3l4_schema.sql 2>&1 | grep -v "cannot be found" || true

echo "Step 4: Granting access to test objects..."
docker exec saiql_hana_test $HDBSQL_PATH -i 90 -d "$HANA_DATABASE" -u "$HANA_ADMIN_USER" -p "$HANA_ADMIN_PASSWORD" <<EOF
-- Grant access to all test tables
GRANT SELECT, INSERT, UPDATE, DELETE ON customers TO $L2L3L4_USER;
GRANT SELECT, INSERT, UPDATE, DELETE ON products TO $L2L3L4_USER;
GRANT SELECT, INSERT, UPDATE, DELETE ON orders TO $L2L3L4_USER;
GRANT SELECT, INSERT, UPDATE, DELETE ON order_items TO $L2L3L4_USER;
GRANT SELECT, INSERT, UPDATE, DELETE ON type_test TO $L2L3L4_USER;
GRANT SELECT, INSERT, UPDATE, DELETE ON audit_log TO $L2L3L4_USER;

-- Grant access to views
GRANT SELECT ON customer_summary TO $L2L3L4_USER;
GRANT SELECT ON active_customers TO $L2L3L4_USER;
GRANT SELECT ON high_balance_active TO $L2L3L4_USER;
GRANT SELECT ON order_summary TO $L2L3L4_USER;

-- Grant EXECUTE on routines
GRANT EXECUTE ON update_customer_status TO $L2L3L4_USER;
GRANT EXECUTE ON get_customer_count TO $L2L3L4_USER;
GRANT EXECUTE ON get_customer_balance TO $L2L3L4_USER;
GRANT EXECUTE ON calculate_discount_price TO $L2L3L4_USER;

COMMIT;
EOF

echo ""
echo "=============================================="
echo "Setup Complete!"
echo "=============================================="
echo ""
echo "Connection Details for L2/L3/L4 Tests:"
echo "  Host:     $HANA_HOST"
echo "  Port:     $HANA_PORT"
echo "  Database: $HANA_DATABASE"
echo "  User:     $L2L3L4_USER"
echo "  Password: $L2L3L4_PASSWORD"
echo ""
echo "Environment variables:"
echo "  export HANA_HOST=$HANA_HOST"
echo "  export HANA_PORT=$HANA_PORT"
echo "  export HANA_DATABASE=$HANA_DATABASE"
echo "  export HANA_USER=$L2L3L4_USER"
echo "  export HANA_PASSWORD=$L2L3L4_PASSWORD"
echo ""
echo "Run integration tests:"
echo "  pytest tests/integration/test_hana_l2l3l4_harness.py -v -s"
echo ""
