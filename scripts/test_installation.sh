#!/bin/bash
# SAIQL Installation Test Script

echo "========================================="
echo "  SAIQL Installation Pre-Check"
echo "========================================="
echo

# Check Python version
echo "✓ Checking Python version..."
python3 --version

# Check if running as nova user (not root for test)
echo "✓ Current user: $(whoami)"

# Check core files exist
echo "✓ Checking SAIQL core files..."
if [ -d "${SAIQL_HOME:-$HOME/SAIQL}/core" ]; then
    echo "  Core directory exists"
fi

if [ -f "${SAIQL_HOME:-$HOME/SAIQL}/interface/saiql_server_secured.py" ]; then
    echo "  Server daemon exists"
fi

if [ -f "${SAIQL_HOME:-$HOME/SAIQL}/core/cli/saiql_client.py" ]; then
    echo "  Client CLI exists"
fi

# Check if we can import the modules
echo "✓ Testing Python imports..."
python3 -c "
import sys
sys.path.insert(0, '${SAIQL_HOME:-$HOME/SAIQL}')
try:
    from core.database_manager import DatabaseManager
    print('  Database manager: OK')
except ImportError as e:
    print(f'  Database manager: MISSING - {e}')

try:
    from interface.saiql_server_secured import SecuredSAIQLServer
    print('  Server module: OK')
except ImportError as e:
    print(f'  Server module: MISSING - {e}')
"

echo
echo "Pre-check complete!"
echo
echo "To install SAIQL system-wide, run as root:"
echo "  sudo ${SAIQL_HOME:-$HOME/SAIQL}/install_system.sh"
echo
echo "This will:"
echo "  • Create system user 'saiql'"
echo "  • Install to /usr/lib/saiql"
echo "  • Create service at /etc/systemd/system/saiql.service"
echo "  • Set up database at /var/lib/saiql"
echo "  • Configure logging at /var/log/saiql"
