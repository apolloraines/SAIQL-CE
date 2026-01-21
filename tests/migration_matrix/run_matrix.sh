#!/bin/bash
set -e

# Check if running inside Docker
if [ -f /.dockerenv ] || grep -q docker /proc/1/cgroup; then
    # ==========================================
    # INSIDE DOCKER: Execute Harness
    # ==========================================
    echo "=== SAIQL MIGRATION TRUTH HARNESS (Containerized) ==="
    
    # Internal Service URLs
    export SOURCE_POSTGRES_URL="postgresql://source_user:source_password@source_postgres:5432/source_db"
    export SOURCE_MYSQL_URL="mysql+pymysql://source_user:source_password@source_mysql:3306/source_db"
    export TARGET_POSTGRES_URL="postgresql://target_user:target_password@target_postgres:5432/target_db"
    export TARGET_MYSQL_URL="mysql+pymysql://target_user:target_password@target_mysql:3306/target_db"
    export SOURCE_MSSQL_URL="mssql+pymssql://sa:StrongPass123@source_mssql:1433/master"
    export TARGET_MSSQL_URL="mssql+pymssql://sa:StrongPass123@target_mssql:1433/master"
    export SOURCE_ORACLE_URL="oracle+oracledb://system:StrongPass123@source_oracle:1521/FREE"

    # Paths
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    SAIQL_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

    # 1. Seed
    echo "[1/3] Seeding source databases..."
    # Configurable retry loop for slow startups (Oracle)
    MAX_RETRIES=5
    RETRY_COUNT=0
    until python3 "$SCRIPT_DIR/seed_and_verify.py" seed || [ $RETRY_COUNT -eq $MAX_RETRIES ]; do
       echo "  > Seeding failed, retrying in 5s... ($((RETRY_COUNT+1))/$MAX_RETRIES)"
       sleep 5
       RETRY_COUNT=$((RETRY_COUNT+1))
    done
    if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
        echo "ERROR: Seeding failed after retries."
        exit 1
    fi

    # 2. Migrate
    echo "[2/3] Running SAIQL Migrator..."
    run_migration() {
        echo "  > $1"
        rm -f migration_state.json
        python3 "${SAIQL_ROOT}/tools/db_migrator.py" --source "$2" --target "$3"
    }

    # Matrix
    run_migration "Postgres -> Postgres" "$SOURCE_POSTGRES_URL" "$TARGET_POSTGRES_URL"
    run_migration "MySQL -> MySQL" "$SOURCE_MYSQL_URL" "$TARGET_MYSQL_URL"
    run_migration "MSSQL -> MSSQL" "$SOURCE_MSSQL_URL" "$TARGET_MSSQL_URL"
    run_migration "MSSQL -> Postgres" "$SOURCE_MSSQL_URL" "$TARGET_POSTGRES_URL"
    run_migration "MSSQL -> MySQL" "$SOURCE_MSSQL_URL" "$TARGET_MYSQL_URL"
    run_migration "Postgres -> MSSQL" "$SOURCE_POSTGRES_URL" "$TARGET_MSSQL_URL"
    run_migration "Oracle -> Postgres" "$SOURCE_ORACLE_URL" "$TARGET_POSTGRES_URL"

    # 3. Verify
    echo "[3/3] Verifying results..."
    python3 "$SCRIPT_DIR/seed_and_verify.py" verify
    
    echo "=== SUCCESS: All migrations verified! ==="

else
    # ==========================================
    # ON HOST: Launch Docker Wrapper
    # ==========================================
    echo "=== SAIQL MIGRATION TRUTH HARNESS (Launcher) ==="
    echo "Detected Host Environment. Launching Docker Harness..."
    
    # Ensure Docker is available
    if ! command -v docker &> /dev/null; then
        echo "Error: docker not found."
        exit 1
    fi

    # Cleanup old runs
    echo "Cleaning up..."
    docker compose down --remove-orphans

    # Run Harness
    # --abort-on-container-exit ensures we stop if the harness finishes (success or fail)
    # --exit-code-from migrator_runner propagates success/fail code to host
    echo "Starting Containers..."
    docker compose up --build --abort-on-container-exit --exit-code-from migrator_runner migrator_runner
fi
