#!/usr/bin/env python3
"""
Secure Configuration Manager for SAIQL-Delta
Handles environment variables, secrets, and paths centrally
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass
import secrets

@dataclass
class SAIQLConfig:
    """SAIQL configuration settings"""

    # Base paths - use environment or defaults
    base_dir: Path = None
    data_dir: Path = None
    config_dir: Path = None
    log_dir: Path = None

    # Database settings
    db_host: str = "localhost"
    db_port: int = 5433
    db_name: str = "saiql_db"
    db_user: str = "saiql_user"

    # Security settings (loaded from environment)
    db_password: str = None
    jwt_secret: str = None
    api_key_salt: str = None
    master_key: str = None

    # Runtime settings
    debug_mode: bool = False
    log_level: str = "INFO"
    max_connections: int = 100

    # Trading safety limits (consensus item #13)
    min_order_value: float = 0.01
    max_order_value: float = 10000.0

    # Profile settings
    profile: str = "dev"  # dev, demo, prod

    def __post_init__(self):
        """Initialize paths and load environment variables"""
        # Load profile from environment
        self.profile = os.environ.get('SAIQL_PROFILE', 'dev')
        
        # Set base directory
        if self.base_dir is None:
            # Default to project root relative to this file
            default_root = Path(__file__).parent.parent
            self.base_dir = Path(os.environ.get('SAIQL_HOME', default_root))
        else:
            self.base_dir = Path(self.base_dir)

        # Set subdirectories relative to base
        if self.data_dir is None:
            self.data_dir = self.base_dir / 'data'
        if self.config_dir is None:
            self.config_dir = self.base_dir / 'config'
        if self.log_dir is None:
            self.log_dir = self.base_dir / 'logs'

        # Load secrets from environment
        self.db_password = os.environ.get('SAIQL_DB_PASSWORD')
        self.jwt_secret = os.environ.get('SAIQL_JWT_SECRET')
        self.api_key_salt = os.environ.get('SAIQL_API_KEY_SALT')
        self.master_key = os.environ.get('SAIQL_MASTER_KEY')

        # Load debug settings
        self.debug_mode = os.environ.get('SAIQL_DEBUG', '').lower() == 'true'
        self.log_level = os.environ.get('SAIQL_LOG_LEVEL', 'INFO')

        # Database settings from environment
        self.db_host = os.environ.get('SAIQL_DB_HOST', self.db_host)
        self.db_port = int(os.environ.get('SAIQL_DB_PORT', str(self.db_port)))
        self.db_name = os.environ.get('SAIQL_DB_NAME', self.db_name)
        self.db_user = os.environ.get('SAIQL_DB_USER', self.db_user)

        # Apply profile defaults if not overridden
        if self.profile == 'prod':
            self.debug_mode = False
            self.log_level = 'WARNING'
        elif self.profile == 'demo':
            self.debug_mode = True
            self.log_level = 'INFO'

    def validate_secrets(self, silent: bool = False) -> bool:
        """Validate that required secrets are configured.

        Args:
            silent: If True, suppress warning output (for use in serialization)
        """
        missing = []

        if not self.db_password:
            missing.append('SAIQL_DB_PASSWORD')
        if not self.jwt_secret:
            missing.append('SAIQL_JWT_SECRET')
        if not self.master_key:
            missing.append('SAIQL_MASTER_KEY')

        if missing:
            if not silent:
                print(f"Missing required environment variables: {', '.join(missing)}")
                print("Please set these in your environment or .env file")
            return False
        return True

    def get_db_url(self, include_password: bool = False) -> str:
        """Get database connection URL"""
        if include_password and self.db_password:
            return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        else:
            return f"postgresql://{self.db_user}@{self.db_host}:{self.db_port}/{self.db_name}"

    def get_safe_dict(self) -> Dict[str, Any]:
        """Get configuration as dict without sensitive values (side-effect free)"""
        return {
            'base_dir': str(self.base_dir),
            'data_dir': str(self.data_dir),
            'config_dir': str(self.config_dir),
            'log_dir': str(self.log_dir),
            'db_host': self.db_host,
            'db_port': self.db_port,
            'db_name': self.db_name,
            'db_user': self.db_user,
            'debug_mode': self.debug_mode,
            'log_level': self.log_level,
            'max_connections': self.max_connections,
            'secrets_configured': self.validate_secrets(silent=True),
            'edition': 'ce',  # CE Edition
        }

class ConfigManager:
    """Singleton configuration manager"""

    _instance: Optional['ConfigManager'] = None
    _config: Optional[SAIQLConfig] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._config is None:
            self.load_config()

    def load_config(self, config_file: Optional[Path] = None):
        """Load configuration from file and environment.

        Priority (highest to lowest):
        1. Environment variables (SAIQL_*)
        2. .env file (loaded into os.environ before config creation)
        3. SAIQLConfig dataclass defaults
        """
        # Step 1: Load .env file FIRST to populate os.environ
        # Use default paths since SAIQLConfig doesn't exist yet
        default_base = Path(__file__).parent.parent
        base_dir = Path(os.environ.get('SAIQL_HOME', default_base))
        env_file = base_dir / '.env'
        if env_file.exists():
            self._load_env_file(env_file)

        # Step 2: Create SAIQLConfig (reads from os.environ in __post_init__)
        self._config = SAIQLConfig()

        # Note: JSON config files are NOT loaded to preserve env var precedence.
        # Use environment variables or .env file for all configuration.

    def _load_env_file(self, env_file: Path):
        """Load environment variables from .env file.

        Only sets values for keys not already in os.environ,
        ensuring exported env vars take precedence over .env file.
        """
        try:
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        if '=' in line:
                            key, value = line.split('=', 1)
                            key = key.strip()
                            # Only set if not already present (env vars take precedence)
                            if key not in os.environ:
                                os.environ[key] = value.strip()
        except Exception as e:
            print(f"Warning: Could not load .env file: {e}")

    @property
    def config(self) -> SAIQLConfig:
        """Get the current configuration"""
        if self._config is None:
            self.load_config()
        return self._config

    def generate_api_key(self) -> str:
        """Generate a secure API key"""
        return f"sk-{secrets.token_urlsafe(32)}"

    def generate_jwt_secret(self) -> str:
        """Generate a secure JWT secret"""
        return secrets.token_urlsafe(64)


# Global config instance
def get_config() -> SAIQLConfig:
    """Get the global configuration instance"""
    return ConfigManager().config


if __name__ == "__main__":
    # Test configuration
    config = get_config()
    print("SAIQL Configuration Status:")
    print("-" * 40)
    for key, value in config.get_safe_dict().items():
        print(f"{key}: {value}")