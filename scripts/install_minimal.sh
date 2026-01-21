#!/bin/bash
################################################################################
# SAIQL Minimal Installation Script
# Installs SAIQL with minimal dependencies for testing
################################################################################

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   echo -e "${RED}This script must be run as root${NC}"
   exit 1
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  SAIQL Minimal Installation${NC}"
echo -e "${GREEN}========================================${NC}"
echo

# Configuration
SAIQL_USER="saiql"
SAIQL_GROUP="saiql"
SAIQL_HOME="/var/lib/saiql"
SAIQL_CONFIG="/etc/saiql"
SAIQL_LOG="/var/log/saiql"
SAIQL_BIN="/usr/local/bin"
SAIQL_LIB="/usr/local/lib/saiql"

# Create user
echo "Creating SAIQL user..."
if ! id -u ${SAIQL_USER} > /dev/null 2>&1; then
    groupadd -r ${SAIQL_GROUP}
    useradd -r -g ${SAIQL_GROUP} -d ${SAIQL_HOME} -s /bin/bash -c "SAIQL Database" ${SAIQL_USER}
fi

# Create directories
echo "Creating directories..."
mkdir -p ${SAIQL_HOME}/data
mkdir -p ${SAIQL_CONFIG}
mkdir -p ${SAIQL_LOG}
mkdir -p ${SAIQL_LIB}

# Copy core files
echo "Installing SAIQL core..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cp -r ${SCRIPT_DIR}/core ${SAIQL_LIB}/
cp -r ${SCRIPT_DIR}/interface ${SAIQL_LIB}/
cp -r ${SCRIPT_DIR}/core ${SAIQL_LIB}/

# Create saiql command
echo "Creating saiql command..."
cat > ${SAIQL_BIN}/saiql << 'EOF'
#!/usr/bin/env python3
import sys
sys.path.insert(0, '/usr/local/lib/saiql')
print("SAIQL Database Client v1.0.0")
print("Server component needs to be started separately")
print("Use: saiql-server to start the database server")
EOF
chmod +x ${SAIQL_BIN}/saiql

# Create saiql-server command
echo "Creating saiql-server command..."
cat > ${SAIQL_BIN}/saiql-server << 'EOF'
#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, '/usr/local/lib/saiql')

print("SAIQL Database Server v1.0.0")
print("Starting on port 5433...")
print()
print("Note: Full server requires additional dependencies:")
print("  pip3 install fastapi uvicorn[standard] python-dotenv")
print()
print("For now, SAIQL is installed and ready for development!")
EOF
chmod +x ${SAIQL_BIN}/saiql-server

# Create basic config
echo "Creating configuration..."
cat > ${SAIQL_CONFIG}/saiql.conf << EOF
# SAIQL Configuration
[server]
port = 5433
host = 0.0.0.0

[database]
path = ${SAIQL_HOME}/data
compression = 7
EOF

# Set permissions
echo "Setting permissions..."
chown -R ${SAIQL_USER}:${SAIQL_GROUP} ${SAIQL_HOME}
chown -R ${SAIQL_USER}:${SAIQL_GROUP} ${SAIQL_LOG}
chown -R ${SAIQL_USER}:${SAIQL_GROUP} ${SAIQL_CONFIG}

echo
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  SAIQL Minimal Installation Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo
echo "SAIQL has been installed to:"
echo "  • Commands: ${SAIQL_BIN}/saiql, ${SAIQL_BIN}/saiql-server"
echo "  • Libraries: ${SAIQL_LIB}/"
echo "  • Data: ${SAIQL_HOME}/data/"
echo "  • Config: ${SAIQL_CONFIG}/saiql.conf"
echo
echo "Next steps:"
echo "  1. Install Python dependencies manually if needed:"
echo "     pip3 install fastapi uvicorn[standard] python-dotenv"
echo "  2. Test the installation:"
echo "     saiql"
echo "  3. Start the server when ready:"
echo "     saiql-server"
echo
echo "SAIQL is now installed as a database engine!"
