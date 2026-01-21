# 🚀 SAIQL Quick Start Guide

**Goal**: Go from zero to running your first SAIQL query in **10 minutes**.

This guide assumes you are on **Ubuntu 22.04 LTS** or **24.04 LTS**.

---

## 1. Install Dependencies
Open your terminal and run:

```bash
sudo apt update && sudo apt install -y python3 python3-pip python3-venv git build-essential libpq-dev
```

## 2. Clone & Setup
Clone the repository and set up a virtual environment:

```bash
# Clone repo
git clone https://github.com/nova-org/SAIQL.DEV.git saiql
cd saiql

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install psycopg2-binary mysql-connector-python  # Optional: for migration/legacy support
```

## 3. Configure Security
SAIQL requires a secure configuration. Generate secrets and set up the environment:

```bash
# Generate a secure JWT secret
export SAIQL_JWT_SECRET=$(openssl rand -hex 32)
echo "Generated Secret: $SAIQL_JWT_SECRET"

# Set environment variables (for this session)
export SAIQL_PROFILE=dev
export SAIQL_DB_PASSWORD=secure_password
export SAIQL_PORT=8000
export SAIQL_BOOTSTRAP_TEMPLATE=true  # Required for first run
```

## 4. Start the Server
Launch the SAIQL standalone engine:

```bash
# Start server on port 8000
python3 saiql_production_server.py
```

*You should see output indicating the server is running on `http://0.0.0.0:8000`.*

## 5. Run Your First Query
Open a **new terminal window** (keep the server running) and use `curl` to execute a query:

```bash
# 1. Get an Auth Token (Default Admin)
TOKEN=$(curl -s -X POST "http://localhost:8000/auth/token" \
     -H "Content-Type: application/json" \
     -d '{"username": "admin", "password": "admin_password"}' | jq -r .access_token)

# 2. Execute a Query
curl -X POST "http://localhost:8000/query" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"query": "SELECT * FROM users WHERE active = true"}'
```

**Expected Result**:
```json
{
  "success": true,
  "data": [],
  "execution_time": 0.001,
  "backend": "sqlite"
}
```

---

## 🎉 Success!
You have a running SAIQL instance.

### Next Steps
- **[02_Install.md](./02_Install.md)**: Perform a permanent production installation.
- **[05_CLI_Guide.md](./05_CLI_Guide.md)**: Explore the Command Line Interface.
- **[07_Query_Language_Basics.md](./07_Query_Language_Basics.md)**: Learn SAIQL syntax.
