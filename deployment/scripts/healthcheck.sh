#!/bin/bash

# SAIQL Health Check Script
# Used by Docker and Kubernetes for container health monitoring

set -e

SAIQL_HOST="${SAIQL_HOST:-localhost}"
SAIQL_PORT="${SAIQL_PORT:-5432}"
TIMEOUT="${HEALTH_CHECK_TIMEOUT:-10}"

# Function to check if SAIQL server is responding
check_server_health() {
    echo "🏥 Checking SAIQL server health..."
    
    # Try to connect to SAIQL port
    if timeout "$TIMEOUT" bash -c "</dev/tcp/$SAIQL_HOST/$SAIQL_PORT"; then
        echo "✅ Server port is accessible"
    else
        echo "❌ Server port is not accessible"
        return 1
    fi
    
    # Try to execute a simple health check query
    python3 -c "
import sys
import socket
import time
sys.path.append('/app/saiql')

def check_saiql_health():
    try:
        # Import SAIQL components
        from core.operators import SAIQLOperators
        
        # Test basic operation
        runtime = SAIQLOperators()
        result = runtime.execute_operator('*', 1, 'health_check')
        
        if result.get('operation') == 'SELECT':
            print('✅ SAIQL runtime is healthy')
            return True
        else:
            print('❌ SAIQL runtime returned unexpected result')
            return False
            
    except Exception as e:
        print(f'❌ SAIQL health check failed: {e}')
        return False

if not check_saiql_health():
    sys.exit(1)
" || return 1
    
    return 0
}

# Function to check database integrity
check_database_health() {
    echo "🗄️ Checking database health..."
    
    python3 -c "
import sys
import os
sys.path.append('/app/saiql')

def check_database():
    try:
        # Check if database file exists and is accessible
        db_path = '/app/data/saiql.db'
        if not os.path.exists(db_path):
            print('❌ Database file not found')
            return False
            
        if not os.access(db_path, os.R_OK | os.W_OK):
            print('❌ Database file not accessible')
            return False
            
        # Check database size (should be > 0)
        db_size = os.path.getsize(db_path)
        if db_size == 0:
            print('❌ Database file is empty')
            return False
            
        print(f'✅ Database healthy (size: {db_size} bytes)')
        return True
        
    except Exception as e:
        print(f'❌ Database check failed: {e}')
        return False

if not check_database():
    sys.exit(1)
" || return 1
    
    return 0
}

# Function to check system resources
check_system_health() {
    echo "💻 Checking system resources..."
    
    python3 -c "
import psutil
import sys

def check_resources():
    try:
        # Check memory usage
        memory = psutil.virtual_memory()
        if memory.percent > 95:
            print(f'⚠️ High memory usage: {memory.percent}%')
            return False
            
        # Check disk space
        disk = psutil.disk_usage('/app/data')
        disk_percent = (disk.used / disk.total) * 100
        if disk_percent > 90:
            print(f'⚠️ High disk usage: {disk_percent:.1f}%')
            return False
            
        # Check if we can write to log directory
        import tempfile
        import os
        try:
            with tempfile.NamedTemporaryFile(dir='/app/logs', delete=True):
                pass
        except:
            print('❌ Cannot write to log directory')
            return False
            
        print(f'✅ System resources OK (mem: {memory.percent}%, disk: {disk_percent:.1f}%)')
        return True
        
    except Exception as e:
        print(f'❌ System check failed: {e}')
        return False

if not check_resources():
    sys.exit(1)
" || return 1
    
    return 0
}

# Function to check configuration
check_config_health() {
    echo "⚙️ Checking configuration..."
    
    python3 -c "
import json
import sys

def check_config():
    try:
        with open('/app/config/production.json', 'r') as f:
            config = json.load(f)
            
        # Validate required sections
        required_sections = ['saiql', 'server', 'database', 'security']
        for section in required_sections:
            if section not in config:
                print(f'❌ Missing config section: {section}')
                return False
                
        # Check server configuration
        server_config = config.get('server', {})
        if not server_config.get('port') or not server_config.get('host'):
            print('❌ Invalid server configuration')
            return False
            
        print('✅ Configuration is valid')
        return True
        
    except Exception as e:
        print(f'❌ Configuration check failed: {e}')
        return False

if not check_config():
    sys.exit(1)
" || return 1
    
    return 0
}

# Main health check function
main() {
    echo "🏥 SAIQL Health Check Starting..."
    echo "================================"
    
    # Run all health checks
    check_config_health || exit 1
    check_system_health || exit 1
    check_database_health || exit 1
    check_server_health || exit 1
    
    echo ""
    echo "🎉 All health checks passed!"
    echo "SAIQL is healthy and ready for production"
    
    exit 0
}

# Run main function
main "$@"
