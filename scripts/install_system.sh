#!/bin/bash
################################################################################
# SAIQL Database Engine System Installation Script
# Installs SAIQL as a system-wide database service like PostgreSQL/MySQL
################################################################################

set -e

# Configuration
SAIQL_VERSION="1.0.0"
SAIQL_USER="saiql"
SAIQL_GROUP="saiql"
SAIQL_HOME="/var/lib/saiql"
SAIQL_CONFIG="/etc/saiql"
SAIQL_LOG="/var/log/saiql"
SAIQL_RUN="/var/run/saiql"
SAIQL_BIN="/usr/bin"
SAIQL_LIB="/usr/lib/saiql"
SAIQL_SHARE="/usr/share/saiql"
SAIQL_PORT=5433

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
echo -e "${GREEN}  SAIQL Database Engine Installation${NC}"
echo -e "${GREEN}  Version: ${SAIQL_VERSION}${NC}"
echo -e "${GREEN}========================================${NC}"
echo

# Function to print progress
print_progress() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

# Function to print success
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Function to print error
print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Step 1: Create SAIQL system user and group
print_progress "Creating SAIQL system user and group..."
if ! id -u ${SAIQL_USER} > /dev/null 2>&1; then
    groupadd -r ${SAIQL_GROUP}
    useradd -r -g ${SAIQL_GROUP} -d ${SAIQL_HOME} -s /bin/bash -c "SAIQL Database System" ${SAIQL_USER}
    print_success "Created user and group: ${SAIQL_USER}:${SAIQL_GROUP}"
else
    print_success "User ${SAIQL_USER} already exists"
fi

# Step 2: Create directory structure
print_progress "Creating directory structure..."
mkdir -p ${SAIQL_HOME}/data
mkdir -p ${SAIQL_HOME}/backups
mkdir -p ${SAIQL_HOME}/temp
mkdir -p ${SAIQL_CONFIG}/conf.d
mkdir -p ${SAIQL_LOG}
mkdir -p ${SAIQL_RUN}
mkdir -p ${SAIQL_LIB}
mkdir -p ${SAIQL_SHARE}/docs
mkdir -p ${SAIQL_SHARE}/examples
mkdir -p ${SAIQL_SHARE}/schemas
print_success "Directory structure created"

# Step 3: Install Python dependencies
print_progress "Installing Python dependencies..."
# Skip pip upgrade if system-managed
if pip3 --version | grep -q "/usr/lib"; then
    print_success "Using system pip"
else
    pip3 install --upgrade pip --break-system-packages || print_error "Failed to upgrade pip"
fi

# Install required packages
pip3 install \
    fastapi \
    "uvicorn[standard]" \
    psycopg2-binary \
    PyMySQL \
    redis \
    prometheus-client \
    python-dotenv \
    pyyaml \
    cryptography \
    numpy \
    pandas \
    pytest \
    --break-system-packages || {
    print_error "Some dependencies failed to install, continuing anyway..."
}
print_success "Python dependencies processed"

# Step 4: Copy SAIQL core files
print_progress "Installing SAIQL core engine..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Copy core engine files
cp -r ${ROOT_DIR}/core/* ${SAIQL_LIB}/
cp -r ${ROOT_DIR}/interface/* ${SAIQL_LIB}/
cp -r ${ROOT_DIR}/security/* ${SAIQL_LIB}/
cp -r ${ROOT_DIR}/extensions/* ${SAIQL_LIB}/
cp -r ${ROOT_DIR}/core ${SAIQL_LIB}/
cp -r ${ROOT_DIR}/saiql_meta ${SAIQL_LIB}/
cp -r ${ROOT_DIR}/utils ${SAIQL_LIB}/
cp ${ROOT_DIR}/saiql_production_server.py ${SAIQL_LIB}/

# Copy data files
cp -r ${ROOT_DIR}/data/* ${SAIQL_HOME}/data/

# Copy documentation
cp -r ${ROOT_DIR}/docs/* ${SAIQL_SHARE}/docs/
cp -r ${ROOT_DIR}/examples/* ${SAIQL_SHARE}/examples/

print_success "SAIQL core engine installed"

# Step 5: Create main SAIQL executable
print_progress "Creating SAIQL command-line client..."
cat > ${SAIQL_BIN}/saiql << 'EOF'
#!/usr/bin/env python3
"""
SAIQL Command-Line Interface
"""
import sys
import os
sys.path.insert(0, '/usr/lib/saiql')

# Patched to avoid recursion and import errors
from shell.query_shell import main as shell_main

def main():
    shell_main()

if __name__ == "__main__":
    main()
EOF
chmod +x ${SAIQL_BIN}/saiql
print_success "SAIQL command-line client created"

# Step 6: Create SAIQL server executable
print_progress "Creating SAIQL server daemon..."
cat > ${SAIQL_BIN}/saiql-server << 'EOF'
#!/usr/bin/env python3
"""
SAIQL Database Server
"""
import sys
import os
import argparse
sys.path.insert(0, '/usr/lib/saiql')

from saiql_server_secured import SecuredSAIQLServer

def main():
    parser = argparse.ArgumentParser(description='SAIQL Database Server')
    parser.add_argument('--port', type=int, default=5433, help='Port to listen on')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--config', default='/etc/saiql/saiql.conf', help='Configuration file')
    parser.add_argument('--mode', choices=['standalone', 'translation'], default='standalone',
                       help='Server mode: standalone or translation')
    args = parser.parse_args()
    
    server = SecuredSAIQLServer(
        host=args.host,
        port=args.port,
        config_file=args.config,
        mode=args.mode
    )
    server.run()

if __name__ == "__main__":
    main()
EOF
chmod +x ${SAIQL_BIN}/saiql-server
print_success "SAIQL server daemon created"

# Step 7: Create configuration file
print_progress "Creating configuration files..."
cat > ${SAIQL_CONFIG}/saiql.conf << EOF
# SAIQL Database Configuration File
# Generated on $(date)

[server]
host = 0.0.0.0
port = ${SAIQL_PORT}
workers = 4
max_connections = 1000
timeout = 30

[database]
data_dir = ${SAIQL_HOME}/data
compression_level = 7
cache_size = 1GB
wal_enabled = true
auto_vacuum = true

[security]
authentication = jwt
tls_enabled = false
tls_cert = ${SAIQL_CONFIG}/server.crt
tls_key = ${SAIQL_CONFIG}/server.key
rate_limit = 1000

[logging]
log_level = INFO
log_file = ${SAIQL_LOG}/saiql.log
log_rotation = daily
log_retention = 30

[clustering]
enabled = false
node_id = 1
nodes = []

[ai]
natural_language = true
vector_search = true
adaptive_compression = true
model_cache = 512MB
EOF
print_success "Configuration file created"

# Step 8: Create systemd service
print_progress "Creating systemd service..."
cat > /etc/systemd/system/saiql.service << EOF
[Unit]
Description=SAIQL Database Server
Documentation=https://docs.saiql.dev
After=network.target

[Service]
Type=simple
User=${SAIQL_USER}
Group=${SAIQL_GROUP}
WorkingDirectory=${SAIQL_HOME}
ExecStart=${SAIQL_BIN}/saiql-server --config ${SAIQL_CONFIG}/saiql.conf
ExecReload=/bin/kill -HUP \$MAINPID
KillMode=mixed
KillSignal=SIGTERM
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=saiql

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${SAIQL_HOME} ${SAIQL_LOG} ${SAIQL_RUN}

[Install]
WantedBy=multi-user.target
EOF
print_success "Systemd service created"

# Step 9: Create log rotation
print_progress "Setting up log rotation..."
cat > /etc/logrotate.d/saiql << EOF
${SAIQL_LOG}/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 640 ${SAIQL_USER} ${SAIQL_GROUP}
    sharedscripts
    postrotate
        systemctl reload saiql.service > /dev/null 2>&1 || true
    endscript
}
EOF
print_success "Log rotation configured"

# Step 10: Set permissions
print_progress "Setting permissions..."
chown -R ${SAIQL_USER}:${SAIQL_GROUP} ${SAIQL_HOME}
chown -R ${SAIQL_USER}:${SAIQL_GROUP} ${SAIQL_LOG}
chown -R ${SAIQL_USER}:${SAIQL_GROUP} ${SAIQL_RUN}
chown -R ${SAIQL_USER}:${SAIQL_GROUP} ${SAIQL_CONFIG}
chmod 750 ${SAIQL_HOME}
chmod 750 ${SAIQL_LOG}
chmod 755 ${SAIQL_CONFIG}
print_success "Permissions set"

# Step 11: Initialize database
print_progress "Initializing SAIQL database..."
su - ${SAIQL_USER} -c "cd ${SAIQL_HOME} && python3 -c \"
import sys
sys.path.insert(0, '${SAIQL_LIB}')
from core.database_manager import DatabaseManager
db = DatabaseManager('${SAIQL_HOME}/data/saiql.db')
print('Database initialized successfully')
\""
print_success "Database initialized"

# Step 12: Create client configuration
print_progress "Creating client configuration..."
cat > /etc/saiql/client.conf << EOF
# SAIQL Client Configuration
[connection]
host = localhost
port = ${SAIQL_PORT}
timeout = 30

[authentication]
method = jwt
token_file = ~/.saiql/token

[display]
format = table
color = true
pager = less
EOF
print_success "Client configuration created"

# Step 13: Create bash completion
print_progress "Installing bash completion..."
cat > /etc/bash_completion.d/saiql << 'EOF'
_saiql_completion() {
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    opts="--help --version --host --port --user --password --database --file --format"
    
    case "${prev}" in
        --format)
            COMPREPLY=( $(compgen -W "table json csv yaml" -- ${cur}) )
            return 0
            ;;
        *)
            ;;
    esac
    
    COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
}
complete -F _saiql_completion saiql
EOF
print_success "Bash completion installed"

# Step 14: Enable and start service
print_progress "Enabling SAIQL service..."
systemctl daemon-reload
systemctl enable saiql.service
print_success "SAIQL service enabled"

# Step 15: Create uninstall script
print_progress "Creating uninstall script..."
cat > ${SAIQL_BIN}/saiql-uninstall << 'EOF'
#!/bin/bash
# SAIQL Uninstall Script

if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root"
   exit 1
fi

echo "Uninstalling SAIQL Database Engine..."

# Stop and disable service
systemctl stop saiql.service
systemctl disable saiql.service
rm -f /etc/systemd/system/saiql.service

# Remove files and directories
rm -rf /var/lib/saiql
rm -rf /etc/saiql
rm -rf /var/log/saiql
rm -rf /var/run/saiql
rm -rf /usr/lib/saiql
rm -rf /usr/share/saiql
rm -f /usr/bin/saiql
rm -f /usr/bin/saiql-server
rm -f /usr/bin/saiql-uninstall
rm -f /etc/logrotate.d/saiql
rm -f /etc/bash_completion.d/saiql

# Remove user and group
userdel saiql
groupdel saiql

echo "SAIQL has been uninstalled"
EOF
chmod +x ${SAIQL_BIN}/saiql-uninstall
print_success "Uninstall script created"

# Final summary
echo
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  SAIQL Installation Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo
echo "Installation Summary:"
echo "  • Version: ${SAIQL_VERSION}"
echo "  • User/Group: ${SAIQL_USER}:${SAIQL_GROUP}"
echo "  • Data Directory: ${SAIQL_HOME}/data"
echo "  • Configuration: ${SAIQL_CONFIG}/saiql.conf"
echo "  • Log Directory: ${SAIQL_LOG}"
echo "  • Port: ${SAIQL_PORT}"
echo
echo "Available Commands:"
echo "  • saiql              - Command-line client"
echo "  • saiql-server       - Start server manually"
echo "  • saiql-uninstall    - Uninstall SAIQL"
echo
echo "Service Management:"
echo "  • systemctl start saiql    - Start SAIQL service"
echo "  • systemctl stop saiql     - Stop SAIQL service"
echo "  • systemctl status saiql   - Check service status"
echo "  • systemctl restart saiql  - Restart service"
echo
echo "Quick Start:"
echo "  1. Start the service: systemctl start saiql"
echo "  2. Connect with client: saiql"
echo "  3. Run a query: *5[users]::name,email>>oQ"
echo
echo -e "${GREEN}SAIQL is ready to revolutionize your data infrastructure!${NC}"
