#!/bin/bash
# Setup SAP HANA Express Edition for Phase 07 Integration Tests
# Uses official SAP HANA Express Docker image

set -e

CONTAINER_NAME="saiql_hana_test"
HANA_PASSWORD="SaiqlTest123"
HANA_PORT="39017"

echo "========================================="
echo "SAIQL Phase 07 - HANA Express Setup"
echo "========================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Stop and remove existing container if it exists
if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
    echo "🔄 Stopping existing container..."
    docker stop $CONTAINER_NAME || true
    docker rm $CONTAINER_NAME || true
fi

echo "📦 Pulling SAP HANA Express Edition image..."
echo "Note: This may take several minutes (image is ~5-7GB)"

# Pull official SAP HANA Express Edition image
docker pull saplabs/hanaexpress:latest

echo "🚀 Starting HANA Express container..."
docker run -d \
    --name $CONTAINER_NAME \
    -p ${HANA_PORT}:39017 \
    -e MASTER_PASSWORD=$HANA_PASSWORD \
    --ulimit nofile=1048576:1048576 \
    --sysctl kernel.shmmax=1073741824 \
    --sysctl kernel.shmmni=524288 \
    --sysctl kernel.shmall=8388608 \
    saplabs/hanaexpress:latest \
    --agree-to-sap-license \
    --passwords-url file:///hana/password.json

echo "⏳ Waiting for HANA to initialize (this may take 3-5 minutes)..."
echo "   HANA is initializing database system..."

# Wait for HANA to be ready (check HDB info port)
MAX_WAIT=300
WAIT_COUNT=0
until docker exec $CONTAINER_NAME bash -c 'hdbsql -i 90 -d SYSTEMDB -u SYSTEM -p $MASTER_PASSWORD "SELECT 1 FROM DUMMY"' > /dev/null 2>&1; do
    WAIT_COUNT=$((WAIT_COUNT + 10))
    if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
        echo "❌ Timeout waiting for HANA to start"
        echo "Check logs: docker logs $CONTAINER_NAME"
        exit 1
    fi
    printf "."
    sleep 10
done

echo ""
echo "✅ HANA Express is ready!"
echo ""
echo "Connection Details:"
echo "  Host: localhost"
echo "  Port: $HANA_PORT"
echo "  User: SYSTEM"
echo "  Password: $HANA_PASSWORD"
echo "  Database: HXE"
echo ""
echo "Next Steps:"
echo "  1. Run fixture setup: ./scripts/load_hana_fixture.sh"
echo "  2. Run integration tests: pytest tests/integration/test_phase07_integration.py"
echo ""
echo "To stop HANA: docker stop $CONTAINER_NAME"
echo "To remove HANA: docker rm $CONTAINER_NAME"
echo "========================================="
