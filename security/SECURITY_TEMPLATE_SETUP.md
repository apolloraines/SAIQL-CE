# SAIQL-Charlie Security Template Setup
========================================

⚠️ **IMPORTANT**: This directory contains template files for security artifacts. You MUST configure these properly before production deployment.

## Quick Security Setup

### 1. JWT Secret Key
```bash
# Generate a secure JWT secret (64+ characters)
openssl rand -hex 32 > security/jwt_secret.key

# Or use Python
python3 -c "import secrets; print(secrets.token_urlsafe(64))" > security/jwt_secret.key
```

### 2. API Keys Setup
```bash
# Copy template and configure
cp security/api_keys.json.template security/api_keys.json

# Edit the file and replace:
# - TEMPLATE_ADMIN_KEY_ID with a unique key ID
# - TEMPLATE_ADMIN_KEY_HASH with a secure hash
# - TEMPLATE_CREATION_DATE with current timestamp
# - TEMPLATE_EXPIRY_DATE with appropriate expiry
```

### 3. Environment Variables
Set these environment variables in production:

```bash
export SAIQL_MASTER_KEY="your-master-encryption-key-here"
export SAIQL_JWT_SECRET="path-to-jwt-secret-file"
export SAIQL_ADMIN_PASSWORD="secure-admin-password"
```

### 4. File Permissions
Secure the files properly:

```bash
chmod 600 security/jwt_secret.key
chmod 600 security/api_keys.json
chmod 600 security/secrets.enc
```

## Template Files

- `jwt_secret.key.template` → `jwt_secret.key`
- `api_keys.json.template` → `api_keys.json`
- `secrets_metadata.json` (configured automatically)

## Security Checklist

- [ ] Generate new JWT secret key
- [ ] Configure API keys with proper hashes
- [ ] Set environment variables
- [ ] Set proper file permissions (600)
- [ ] Remove any test/demo credentials
- [ ] Enable TLS/SSL in production
- [ ] Configure firewall rules
- [ ] Set up log monitoring

## Auto-Setup Script

Run the automated setup:

```bash
./security/setup_security.sh
```

---

🔒 **Never commit actual secrets to‍‍‍‌‍‍‌‌​‌‍‌‍‌‌‍‌​‌‍‌‍‌‌‌‌​‍‍‌‍‍‍‍‌​‍‍‌‍‍‍‍‍​‌‍‌‍‌‍‍‌​‍‍‌‌‍‍‍‌​‌‍‍‌‍‌‍‍​‌‌‌‌‌‌‌‌​‍‌‌‌‌‍‌‌ version control!**