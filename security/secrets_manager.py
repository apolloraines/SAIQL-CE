#!/usr/bin/env python3
"""
SAIQL Secrets Manager - Secure Configuration and Secrets Management

This module provides secure storage and retrieval of sensitive configuration
data including database passwords, API keys, and certificates.

Features:
- Environment variable integration
- File-based encrypted storage
- Runtime secret injection
- Secret rotation capabilities
- Audit logging

Author: Apollo & Claude
Version: 1.0.0
Status: Production-Ready for SAIQL-Delta
"""

import os
import json
import base64
import logging
from typing import Dict, Any, Optional, Union, List
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import hashlib
import secrets
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import time

logger = logging.getLogger(__name__)

class SecretType(Enum):
    """Types of secrets that can be managed"""
    DATABASE_PASSWORD = "database_password"
    API_KEY = "api_key"
    JWT_SECRET = "jwt_secret"
    SSL_CERTIFICATE = "ssl_certificate"
    SSL_PRIVATE_KEY = "ssl_private_key"
    ENCRYPTION_KEY = "encryption_key"
    OAUTH_CLIENT_SECRET = "oauth_client_secret"

@dataclass
class SecretMetadata:
    """Metadata for stored secrets"""
    name: str
    secret_type: SecretType
    created_at: float
    updated_at: float
    expires_at: Optional[float] = None
    rotation_interval: Optional[int] = None  # days
    description: Optional[str] = None
    tags: Optional[Dict[str, str]] = None

class SecretsManager:
    """
    Secure secrets management system for SAIQL
    
    Provides encryption, rotation, and secure access to sensitive configuration data.
    Integrates with environment variables and encrypted file storage.
    """
    
    def __init__(self, secrets_file: Optional[str] = None, master_key: Optional[str] = None):
        """Initialize secrets manager"""
        self.secrets_file = Path(secrets_file or "security/secrets.enc")
        self.metadata_file = Path(str(self.secrets_file).replace(".enc", "_metadata.json"))
        
        # Initialize encryption
        self.master_key = master_key or os.getenv("SAIQL_MASTER_KEY")
        if not self.master_key:
            # Generate a master key for development
            self.master_key = base64.urlsafe_b64encode(os.urandom(32)).decode()
            logger.warning("Using generated master key for development. Set SAIQL_MASTER_KEY for production!")
        
        self.cipher_suite = self._initialize_encryption()
        
        # Storage
        self.secrets = {}
        self.metadata = {}
        
        # Load existing secrets
        self._load_secrets()
        
        # Initialize default secrets if none exist
        if not self.secrets:
            self._initialize_default_secrets()
        
        logger.info("Secrets manager initialized")
    
    def _initialize_encryption(self) -> Fernet:
        """Initialize encryption cipher"""
        # Derive key from master key
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b'saiql_secrets_salt',  # In production, use random salt
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(self.master_key.encode()))
        return Fernet(key)
    
    def _load_secrets(self):
        """Load secrets from encrypted file"""
        try:
            # Load metadata
            if self.metadata_file.exists():
                with open(self.metadata_file, 'r') as f:
                    metadata_data = json.load(f)
                    self.metadata = {}
                    for name, data in metadata_data.items():
                        # Convert secret_type string back to SecretType enum
                        if 'secret_type' in data and isinstance(data['secret_type'], str):
                            data['secret_type'] = SecretType(data['secret_type'])
                        self.metadata[name] = SecretMetadata(**data)
            
            # Load encrypted secrets
            if self.secrets_file.exists():
                with open(self.secrets_file, 'rb') as f:
                    encrypted_data = f.read()
                
                if encrypted_data:
                    decrypted_data = self.cipher_suite.decrypt(encrypted_data)
                    self.secrets = json.loads(decrypted_data.decode())
                    
                    logger.info(f"Loaded {len(self.secrets)} secrets from storage")
                
        except Exception as e:
            logger.error(f"Failed to load secrets: {e}")
            self.secrets = {}
            self.metadata = {}
    
    def _save_secrets(self):
        """Save secrets to encrypted file with restrictive permissions"""
        try:
            # Ensure directory exists with restrictive permissions (owner-only)
            self.secrets_file.parent.mkdir(parents=True, exist_ok=True)
            os.chmod(self.secrets_file.parent, 0o700)

            # Save encrypted secrets
            secrets_json = json.dumps(self.secrets)
            encrypted_data = self.cipher_suite.encrypt(secrets_json.encode())

            # Write secrets file with restrictive permissions (0600 = owner read/write only)
            # Use os.open with explicit mode to avoid race conditions
            fd = os.open(self.secrets_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            try:
                os.write(fd, encrypted_data)
            finally:
                os.close(fd)

            # Save metadata with restrictive permissions
            metadata_dict = {
                name: {
                    'name': meta.name,
                    'secret_type': meta.secret_type.value,
                    'created_at': meta.created_at,
                    'updated_at': meta.updated_at,
                    'expires_at': meta.expires_at,
                    'rotation_interval': meta.rotation_interval,
                    'description': meta.description,
                    'tags': meta.tags
                }
                for name, meta in self.metadata.items()
            }

            # Write metadata file with restrictive permissions (0600)
            fd = os.open(self.metadata_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            try:
                os.write(fd, json.dumps(metadata_dict, indent=2).encode('utf-8'))
            finally:
                os.close(fd)

            logger.debug("Secrets saved to storage with secure permissions")
            
        except Exception as e:
            logger.error(f"Failed to save secrets: {e}")
    
    def _initialize_default_secrets(self):
        """Initialize default secrets for development"""
        defaults = {
            'jwt_secret': {
                'value': base64.urlsafe_b64encode(os.urandom(32)).decode(),
                'type': SecretType.JWT_SECRET,
                'description': 'JWT signing secret'
            },
            'api_encryption_key': {
                'value': base64.urlsafe_b64encode(os.urandom(32)).decode(),
                'type': SecretType.ENCRYPTION_KEY,
                'description': 'API data encryption key'
            },
            'default_admin_password': {
                'value': secrets.token_urlsafe(16),
                'type': SecretType.DATABASE_PASSWORD,
                'description': 'Default admin user password'
            }
        }
        
        for name, config in defaults.items():
            self.store_secret(name, config['value'], config['type'], config['description'])
        
        logger.info("Initialized default secrets for development")
    
    def store_secret(self, name: str, value: str, secret_type: SecretType, 
                    description: Optional[str] = None, tags: Optional[Dict[str, str]] = None,
                    expires_at: Optional[float] = None, rotation_interval: Optional[int] = None) -> bool:
        """Store a secret securely"""
        try:
            current_time = time.time()
            
            # Store secret value
            self.secrets[name] = value
            
            # Store metadata
            self.metadata[name] = SecretMetadata(
                name=name,
                secret_type=secret_type,
                created_at=current_time if name not in self.metadata else self.metadata[name].created_at,
                updated_at=current_time,
                expires_at=expires_at,
                rotation_interval=rotation_interval,
                description=description,
                tags=tags or {}
            )
            
            # Save to storage
            self._save_secrets()
            
            logger.info(f"Stored secret: {name} ({secret_type.value})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store secret {name}: {e}")
            return False
    
    def get_secret(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """Retrieve a secret by name"""
        # First check environment variables
        env_name = f"SAIQL_{name.upper()}"
        env_value = os.getenv(env_name)
        if env_value:
            return env_value
        
        # Then check stored secrets
        secret_value = self.secrets.get(name, default)
        
        if secret_value is None:
            logger.warning(f"Secret not found: {name}")
        
        return secret_value
    
    def get_secret_metadata(self, name: str) -> Optional[SecretMetadata]:
        """Get metadata for a secret"""
        return self.metadata.get(name)
    
    def delete_secret(self, name: str) -> bool:
        """Delete a secret"""
        try:
            if name in self.secrets:
                del self.secrets[name]
            if name in self.metadata:
                del self.metadata[name]
            
            self._save_secrets()
            logger.info(f"Deleted secret: {name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete secret {name}: {e}")
            return False
    
    def list_secrets(self) -> Dict[str, SecretMetadata]:
        """List all secrets (metadata only, not values)"""
        return self.metadata.copy()
    
    def rotate_secret(self, name: str, new_value: Optional[str] = None) -> bool:
        """Rotate a secret to a new value"""
        if name not in self.metadata:
            logger.error(f"Cannot rotate non-existent secret: {name}")
            return False
        
        metadata = self.metadata[name]
        
        # Generate new value if not provided
        if new_value is None:
            if metadata.secret_type == SecretType.JWT_SECRET:
                new_value = base64.urlsafe_b64encode(os.urandom(32)).decode()
            elif metadata.secret_type == SecretType.API_KEY:
                new_value = secrets.token_urlsafe(32)
            elif metadata.secret_type == SecretType.ENCRYPTION_KEY:
                new_value = base64.urlsafe_b64encode(os.urandom(32)).decode()
            elif metadata.secret_type == SecretType.DATABASE_PASSWORD:
                new_value = secrets.token_urlsafe(16)
            else:
                logger.error(f"Cannot auto-generate value for secret type: {metadata.secret_type}")
                return False
        
        # Update secret
        old_value = self.secrets.get(name)
        success = self.store_secret(
            name, new_value, metadata.secret_type, 
            metadata.description, metadata.tags,
            metadata.expires_at, metadata.rotation_interval
        )
        
        if success:
            logger.info(f"Rotated secret: {name}")
        
        return success
    
    def check_expired_secrets(self) -> List[str]:
        """Check for expired secrets"""
        current_time = time.time()
        expired = []
        
        for name, metadata in self.metadata.items():
            if metadata.expires_at and metadata.expires_at < current_time:
                expired.append(name)
        
        return expired
    
    def check_rotation_needed(self) -> List[str]:
        """Check for secrets that need rotation"""
        current_time = time.time()
        needs_rotation = []
        
        for name, metadata in self.metadata.items():
            if metadata.rotation_interval:
                days_since_update = (current_time - metadata.updated_at) / (24 * 60 * 60)
                if days_since_update >= metadata.rotation_interval:
                    needs_rotation.append(name)
        
        return needs_rotation
    
    def get_database_config(self, backend: str) -> Dict[str, Any]:
        """Get database configuration with secrets injected"""
        config = {}
        
        # Common database secrets
        password_key = f"{backend}_password"
        password = self.get_secret(password_key, "")
        
        if password:
            config['password'] = password
        
        # SSL certificates for secure connections
        ssl_cert = self.get_secret(f"{backend}_ssl_cert")
        ssl_key = self.get_secret(f"{backend}_ssl_key")
        ssl_ca = self.get_secret(f"{backend}_ssl_ca")
        
        if ssl_cert and ssl_key:
            config['ssl_cert'] = ssl_cert
            config['ssl_key'] = ssl_key
            if ssl_ca:
                config['ssl_ca'] = ssl_ca
        
        return config
    
    def get_server_config(self) -> Dict[str, Any]:
        """Get server configuration with secrets injected"""
        return {
            'jwt_secret': self.get_secret('jwt_secret'),
            'api_encryption_key': self.get_secret('api_encryption_key'),
            'admin_api_key': self.get_secret('admin_api_key'),
            'oauth_client_secret': self.get_secret('oauth_client_secret')
        }
    
    def generate_api_key(self, name: str, description: Optional[str] = None) -> str:
        """Generate and store a new API key"""
        api_key = f"sk_{secrets.token_hex(16)}"
        
        self.store_secret(
            name,
            api_key,
            SecretType.API_KEY,
            description or f"Generated API key: {name}"
        )
        
        return api_key
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on secrets manager"""
        try:
            # Check if we can encrypt/decrypt
            test_data = "health_check_test"
            encrypted = self.cipher_suite.encrypt(test_data.encode())
            decrypted = self.cipher_suite.decrypt(encrypted).decode()
            
            encryption_ok = (test_data == decrypted)
            
            # Check for expired secrets
            expired_secrets = self.check_expired_secrets()
            rotation_needed = self.check_rotation_needed()
            
            return {
                'status': 'healthy' if encryption_ok else 'unhealthy',
                'encryption': 'working' if encryption_ok else 'failed',
                'secrets_count': len(self.secrets),
                'expired_secrets': len(expired_secrets),
                'rotation_needed': len(rotation_needed),
                'storage_accessible': self.secrets_file.parent.exists()
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e)
            }

# Utility functions
def create_secrets_manager(config_path: Optional[str] = None) -> SecretsManager:
    """Create secrets manager from configuration"""
    if config_path and Path(config_path).exists():
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        return SecretsManager(
            secrets_file=config.get('secrets_file'),
            master_key=config.get('master_key')
        )
    else:
        return SecretsManager()

def inject_secrets_into_config(config: Dict[str, Any], secrets_manager: SecretsManager) -> Dict[str, Any]:
    """Inject secrets into configuration dictionary"""
    def process_value(value):
        if isinstance(value, str) and value.startswith("${SECRET:") and value.endswith("}"):
            # Extract secret name from ${SECRET:secret_name}
            secret_name = value[9:-1]  # Remove ${SECRET: and }
            return secrets_manager.get_secret(secret_name, value)
        elif isinstance(value, dict):
            return {k: process_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [process_value(item) for item in value]
        else:
            return value
    
    return process_value(config)

# Example usage and testing
if __name__ == "__main__":
    # Test the secrets manager
    secrets_manager = SecretsManager()
    
    # Store test secrets
    secrets_manager.store_secret(
        "test_db_password",
        "super_secure_password_123",
        SecretType.DATABASE_PASSWORD,
        "Test database password"
    )
    
    # Generate API key
    api_key = secrets_manager.generate_api_key("test_api_key", "Test API key")
    print(f"Generated API key: {api_key}")
    
    # Retrieve secrets
    db_password = secrets_manager.get_secret("test_db_password")
    print(f"Retrieved DB password: {db_password}")
    
    # Test configuration injection
    config = {
        "database": {
            "password": "${SECRET:test_db_password}",
            "host": "localhost"
        },
        "api": {
            "key": "${SECRET:test_api_key}"
        }
    }
    
    injected_config = inject_secrets_into_config(config, secrets_manager)
    print(f"Injected config: {json.dumps(injected_config, indent=2)}")
    
    # Health check
    health = secrets_manager.health_check()
    print(f"Health check: {json.dumps(health, indent=2)}")
    
    # List secrets (metadata only)
    secrets_list = secrets_manager.list_secrets()
    print(f"Stored secrets: {list(secrets_list.keys())}")
