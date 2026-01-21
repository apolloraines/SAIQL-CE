#!/bin/bash
set -e

# Configuration
CERT_DIR=${1:-"./config/certs"}
DAYS=365
KEY_SIZE=2048

mkdir -p "$CERT_DIR"

echo "Generating self-signed certificate in $CERT_DIR..."

# Generate private key
openssl genrsa -out "$CERT_DIR/server.key" $KEY_SIZE

# Generate CSR
openssl req -new -key "$CERT_DIR/server.key" -out "$CERT_DIR/server.csr" \
    -subj "/C=US/ST=State/L=City/O=SAIQL/OU=Engineering/CN=localhost"

# Generate Self-Signed Certificate
openssl x509 -req -days $DAYS -in "$CERT_DIR/server.csr" \
    -signkey "$CERT_DIR/server.key" -out "$CERT_DIR/server.crt"

# Set permissions
chmod 600 "$CERT_DIR/server.key"
chmod 644 "$CERT_DIR/server.crt"

echo "Certificate generation complete."
echo "Key: $CERT_DIR/server.key"
echo "Cert: $CERT_DIR/server.crt"
