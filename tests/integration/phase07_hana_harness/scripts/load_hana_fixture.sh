#!/bin/bash
# Load Phase 07 integration test fixture into HANA Express

set -e

CONTAINER_NAME="saiql_hana_test"
HANA_PASSWORD="SaiqlTest123"
FIXTURE_SQL="../fixtures/test_schema.sql"

echo "========================================="
echo "Loading Phase 07 Test Fixture into HANA"
echo "========================================="

# Check if container is running
if ! docker ps | grep -q $CONTAINER_NAME; then
    echo "❌ Error: HANA container '$CONTAINER_NAME' is not running"
    echo "Run: ./setup_hana_docker.sh first"
    exit 1
fi

echo "📋 Loading SQL fixture..."

# Copy SQL file into container
docker cp "$FIXTURE_SQL" $CONTAINER_NAME:/tmp/test_schema.sql

# Execute SQL using hdbsql
docker exec $CONTAINER_NAME bash -c "
    export MASTER_PASSWORD='$HANA_PASSWORD'
    hdbsql -i 90 -d HXE -u SYSTEM -p \$MASTER_PASSWORD -I /tmp/test_schema.sql
"

if [ $? -eq 0 ]; then
    echo "✅ Fixture loaded successfully!"
    echo ""
    echo "Test Tables Created:"
    echo "  • customers (3 rows)"
    echo "  • products (3 rows)"
    echo "  • orders (2 rows)"
    echo "  • order_items (4 rows)"
    echo "  • type_test (2 rows - comprehensive type coverage)"
    echo ""
    echo "Ready for integration tests!"
else
    echo "❌ Failed to load fixture"
    exit 1
fi

echo "========================================="
