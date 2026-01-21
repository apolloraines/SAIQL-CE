# 🐧 Ubuntu Preparation Guide

**Goal**: Prepare your Ubuntu 22.04/24.04 system for a production SAIQL deployment.

---

## 1. System Updates
Ensure your system is up to date to avoid dependency conflicts.

```bash
sudo apt update && sudo apt upgrade -y
```

## 2. Required Packages
Install Python 3.10+, build tools, and database libraries.

```bash
sudo apt install -y \
    python3 \
    python3-pip \
    python3-venv \
    git \
    build-essential \
    libpq-dev \
    libssl-dev \
    curl \
    jq
```

*   `libpq-dev`: Required for PostgreSQL support.
*   `libssl-dev`: Required for secure connections.
*   `jq`: Useful for processing JSON output from the API.

## 3. Create a Dedicated User (Production Only)
For production, run SAIQL as a dedicated user, not root.

```bash
# Create user 'saiql' with no login shell
sudo useradd -r -s /bin/false saiql

# Create directory for SAIQL data
sudo mkdir -p /var/lib/saiql
sudo chown saiql:saiql /var/lib/saiql
```

## 4. Firewall Setup (UFW)
Secure your server by only allowing necessary ports.

```bash
# Allow SSH (don't lock yourself out!)
sudo ufw allow ssh

# Allow SAIQL API port (default 8000)
# WARNING: Only do this if you need external access. 
# For production, we recommend a reverse proxy (Nginx) on port 443.
sudo ufw allow 8000/tcp

# Enable Firewall
sudo ufw enable
```

## 5. Time Synchronization
Database consistency relies on accurate time.

```bash
sudo apt install -y chrony
sudo systemctl enable --now chrony
timedatectl status
```
**Expected Output**: `System clock synchronized: yes`

---

### Next Step
- **[02_Install.md](./02_Install.md)**: Install the SAIQL software.
