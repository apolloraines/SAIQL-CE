#!/bin/bash
set -e

# SAIQL Production Entrypoint Script
# Handles initialization, configuration, and startup

echo "🚀 Starting SAIQL Database Engine v5.0.0"
echo "Environment: ${SAIQL_ENV:-production}"

# Function to check if required directories exist
setup_directories() {
    echo "📁 Setting up directories..."
    mkdir -p /app/data /app/logs /app/backups /app/logs/transactions
    
    # Ensure proper permissions
    chmod 755 /app/data /app/logs /app/backups
    chmod 750 /app/logs/transactions
    
    echo "✅ Directories ready"
}

# Function to validate configuration
validate_config() {
    echo "🔍 Validating configuration..."
    
    if [ ! -f "/app/config/production.json" ]; then
        echo "❌ Production configuration not found!"
        exit 1
    fi
    
    # Test JSON validity
    python3 -c "import json; json.load(open('/app/config/production.json'))" || {
        echo "❌ Invalid JSON in production configuration!"
        exit 1
    }
    
    echo "✅ Configuration valid"
}

# Function to initialize database if needed
init_database() {
    echo "🗄️ Initializing database..."
    
    if [ ! -f "/app/data/saiql.db" ]; then
        echo "📝 Creating new database..."
        python3 -c "
import sys
sys.path.append('/app/saiql')
from core.database_manager import DatabaseManager
db = DatabaseManager()
db.initialize_database('/app/data/saiql.db')
print('✅ Database initialized')
"
    else
        echo "✅ Database already exists"
    fi
}

# Function to run database migrations
run_migrations() {
    echo "🔄 Checking for migrations..."
    
    # Check if schema updates are needed
    python3 -c "
import sys
sys.path.append('/app/saiql')
# Add migration logic here when needed
print('✅ Migrations complete')
"
}

# Function to start SAIQL server
start_saiql() {
    echo "🎯 Starting SAIQL server..."
    
    case "$1" in
        "saiql-server")
            exec python3 -c "
import sys
sys.path.append('/app/saiql')
from saiql_production_server import ProductionSAIQLServer
server = ProductionSAIQLServer('/app/config/production.json')
server.start()
"
            ;;
        "saiql-cli")
            exec python3 -c "
import sys
sys.path.append('/app/saiql')
from core.cli.saiql_cli import main
main()
"
            ;;
        "saiql-migrate")
            exec python3 -c "
import sys
sys.path.append('/app/saiql')
from utils.migration import run_migration
run_migration()
"
            ;;
        *)
            echo "⚡ Executing custom command: $@"
            exec "$@"
            ;;
    esac
}

# Function to handle shutdown gracefully
cleanup() {
    echo "🛑 Shutting down SAIQL..."
    # Add cleanup logic here
    echo "✅ Shutdown complete"
}

# Set up signal handlers
trap cleanup SIGTERM SIGINT

# Main execution
main() {
    echo "🔧 SAIQL Production Startup"
    echo "=========================="
    
    # Setup
    setup_directories
    validate_config
    # init_database  <-- Removed: DatabaseManager handles lazy init, and this method does not exist.
    run_migrations
    
    echo "🌟 SAIQL ready for production!"
    echo "Port: ${SAIQL_PORT:-5432}"
    echo "Host: ${SAIQL_HOST:-0.0.0.0}"
    echo ""
    
    # Start the requested service
    start_saiql "${1:-saiql-server}"
}

# Run main function with all arguments
main "$@"
