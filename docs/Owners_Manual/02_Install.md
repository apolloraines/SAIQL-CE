# 📥 SAIQL Installation Guide

**Goal**: Install SAIQL for Development or Production usage.

---

## Option A: Development Install (Quick)
Best for testing, contributing, or local experiments.

### 1. Clone Repository
```bash
git clone https://github.com/nova-org/SAIQL.DEV.git saiql
cd saiql
```

### 2. Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
# Optional: Install database drivers
pip install psycopg2-binary mysql-connector-python
```

---

## Option B: Production Install (Robust)
Best for long-running servers.

### 1. Install to `/opt`
```bash
cd /opt
sudo git clone https://github.com/nova-org/SAIQL.DEV.git saiql
sudo chown -R saiql:saiql /opt/saiql
cd saiql
```

### 2. Create Virtual Environment
```bash
# Switch to saiql user
sudo -u saiql python3 -m venv venv
```

### 3. Install Dependencies
```bash
sudo -u saiql ./venv/bin/pip install -r requirements.txt
sudo -u saiql ./venv/bin/pip install psycopg2-binary mysql-connector-python gunicorn
```

*Note: `gunicorn` is recommended for production deployment behind Nginx.*

### 4. Verify Installation
Run the version check to confirm everything is ready.

```bash
# Dev
python3 bin/saiql.py --version

# Prod
sudo -u saiql ./venv/bin/python3 bin/saiql.py --version
```

**Expected Output**:
```
SAIQL Version: 0.3.0-alpha
```

---

### Next Step
- **[03_Config_and_Profiles.md](./03_Config_and_Profiles.md)**: Configure your instance.
