#!/bin/bash
# SAIQL-Charlie Security Setup Script
# ===================================

set -e

echo "🔒 SAIQL-Charlie Security Setup"
echo "==============================="

SECURITY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SECURITY_DIR"

# Check if running in production
if [[ "${SAIQL_ENV}" == "production" ]]; then
    echo "⚠️  PRODUCTION MODE - Extra security checks enabled"
    PROD_MODE=true
else
    echo "🔧 DEVELOPMENT MODE - Using generated secrets"
    PROD_MODE=false
fi

# 1. Generate JWT Secret
echo "📝 Generating JWT secret..."
if [[ ! -f "jwt_secret.key" ]] || [[ "$PROD_MODE" == "true" ]]; then
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex 32 > jwt_secret.key
    else
        python3 -c "import secrets; print(secrets.token_urlsafe(64))" > jwt_secret.key
    fi
    chmod 600 jwt_secret.key
    echo "   ✅ JWT secret generated"
else
    echo "   ⚠️  JWT secret already exists"
fi

# 2. Setup API Keys
echo "🔑 Setting up API keys..."
if [[ ! -f "api_keys.json" ]]; then
    # Generate admin key
    ADMIN_KEY_ID="sk_$(python3 -c "import secrets; print(secrets.token_hex(8))")"
    ADMIN_KEY_HASH="$(python3 -c "import hashlib, secrets; print(hashlib.sha256(secrets.token_bytes(32)).hexdigest())")"
    CURRENT_TIME="$(python3 -c "from datetime import datetime; print(datetime.now().isoformat())")"
    EXPIRY_TIME="$(python3 -c "from datetime import datetime, timedelta; print((datetime.now() + timedelta(days=365)).isoformat())")"
    
    sed "s/TEMPLATE_ADMIN_KEY_ID/$ADMIN_KEY_ID/g; s/TEMPLATE_ADMIN_KEY_HASH/$ADMIN_KEY_HASH/g; s/TEMPLATE_CREATION_DATE/$CURRENT_TIME/g; s/TEMPLATE_EXPIRY_DATE/$EXPIRY_TIME/g" api_keys.json.template > api_keys.json
    
    chmod 600 api_keys.json
    echo "   ✅ API keys configured"
else
    echo "   ⚠️  API keys already exist"
fi

# 3. Setup secrets metadata
echo "🗄️ Setting up secrets metadata..."
if [[ ! -f "secrets_metadata.json" ]]; then
    cat > secrets_metadata.json << EOF
{
  "encryption_algorithm": "AES-256-GCM",
  "key_derivation": "PBKDF2",
  "secrets": {},
  "created_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "last_updated": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF
    chmod 600 secrets_metadata.json
    echo "   ✅ Secrets metadata created"
fi

# 4. Set proper permissions
echo "🔐 Setting file permissions..."
chmod 600 *.key *.json *.enc 2>/dev/null || true
echo "   ✅ Permissions secured"

# 5. Environment variables check
echo "🌍 Checking environment variables..."
if [[ "$PROD_MODE" == "true" ]]; then
    if [[ -z "$SAIQL_MASTER_KEY" ]]; then
        echo "   ⚠️  WARNING: SAIQL_MASTER_KEY not set in production!"
    else
        echo "   ✅ SAIQL_MASTER_KEY configured"
    fi
else
    echo "   ℹ️  Development mode - environment variables optional"
fi

# 6. Create .gitignore for security files
echo "📝 Creating security .gitignore..."
cat > .gitignore << EOF
# Actual secrets - never commit these!
jwt_secret.key
api_keys.json
secrets.enc
users.json

# Keep templates
!*.template
!*.md
!setup_security.sh
EOF

echo ""
echo "🎉 Security setup complete!"
echo ""

if [[ "$PROD_MODE" == "true" ]]; then
    echo "📋 Production Checklist:"
    echo "   [ ] Verify SAIQL_MASTER_KEY is set"
    echo "   [ ] Review API key configurations"
    echo "   [ ] Enable TLS/SSL"
    echo "   [ ] Configure firewall"
    echo "   [ ] Set up monitoring"
else
    echo "📋 Development Setup:"
    echo "   ✅ JWT secrets generated"
    echo "   ✅ API keys configured"
    echo "   ✅ File permissions secured"
    echo ""
    echo "🚀 Ready for development!"
fi

echo ""
echo "🔒 Remember: Never commit actual secrets to version control!"
