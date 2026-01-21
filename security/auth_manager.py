#!/usr/bin/env python3
"""
SAIQL Authentication Manager - JWT/API Key Authentication System

This module provides comprehensive authentication and authorization for SAIQL-Bravo:
- JWT token authentication
- API key management
- Role-based access control (RBAC)
- Rate limiting
- Session management
- Security logging

Author: Apollo & Claude
Version: 1.0.0
Status: Production-Ready for SAIQL-Bravo

Usage:
    auth_manager = AuthManager()
    token = auth_manager.create_token(user_id="user123", roles=["read", "write"])
    is_valid = auth_manager.verify_token(token)
"""

import jwt
import hashlib
import secrets
import time
import json
import logging
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import threading
from collections import defaultdict, deque
import os
import shutil
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

class UserRole(Enum):
    """User roles for RBAC"""
    ADMIN = "admin"
    READ_WRITE = "read_write"
    READ_ONLY = "read_only"
    EXECUTE_ONLY = "execute_only"
    GUEST = "guest"

class AuthMethod(Enum):
    """Authentication methods"""
    JWT_TOKEN = "jwt_token"
    API_KEY = "api_key"
    SESSION = "session"

@dataclass
class User:
    """User information"""
    user_id: str
    username: str
    email: str
    roles: List[UserRole]
    created_at: datetime
    last_login: Optional[datetime] = None
    is_active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def has_role(self, role: UserRole) -> bool:
        """Check if user has specific role"""
        return role in self.roles
    
    def has_permission(self, permission: str) -> bool:
        """Check if user has specific permission"""
        permission_map = {
            UserRole.ADMIN: ["read", "write", "execute", "admin", "manage_users"],
            UserRole.READ_WRITE: ["read", "write", "execute"],
            UserRole.READ_ONLY: ["read"],
            UserRole.EXECUTE_ONLY: ["execute"],
            UserRole.GUEST: []
        }
        
        for role in self.roles:
            if permission in permission_map.get(role, []):
                return True
        return False

@dataclass
class APIKey:
    """API key information"""
    key_id: str
    key_hash: str
    user_id: str
    name: str
    roles: List[UserRole]
    created_at: datetime
    expires_at: Optional[datetime]
    last_used: Optional[datetime] = None
    usage_count: int = 0
    is_active: bool = True
    rate_limit: Optional[int] = None  # requests per minute
    allowed_ips: List[str] = field(default_factory=list)

@dataclass
class ExternalProviderConfig:
    name: str
    issuer: str
    audience: str
    secret: str
    algorithm: str = 'HS256'

@dataclass
class AuthResult:
    """Authentication result"""
    success: bool
    user_id: Optional[str] = None
    user: Optional[User] = None
    roles: List[UserRole] = field(default_factory=list)
    auth_method: Optional[AuthMethod] = None
    token_expires_at: Optional[datetime] = None
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

class RateLimiter:
    """Rate limiting for API requests"""
    
    def __init__(self, max_requests: int = 100, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = defaultdict(deque)
        self._lock = threading.RLock()
    
    def is_allowed(self, identifier: str) -> Tuple[bool, int]:
        """Check if request is allowed and return remaining requests"""
        with self._lock:
            now = time.time()
            request_times = self.requests[identifier]
            
            # Remove old requests outside time window
            while request_times and request_times[0] < now - self.time_window:
                request_times.popleft()
            
            # Check if limit exceeded
            if len(request_times) >= self.max_requests:
                return False, 0
            
            # Add current request
            request_times.append(now)
            remaining = self.max_requests - len(request_times)
            
            return True, remaining

class AuthManager:
    """
    Comprehensive authentication manager for SAIQL-Bravo
    
    Features:
    - JWT token generation and validation
    - API key management with rotation
    - Role-based access control
    - Rate limiting per user/API key
    - Session management
    - Security event logging
    - Configurable security policies
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize authentication manager"""
        self.config = self._load_config(config_path)

        default_storage = Path.home() / ".saiql" / "security"
        storage_root = os.environ.get('SAIQL_SECURITY_STATE', str(default_storage))
        self.storage_dir = Path(storage_root).expanduser()
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.template_dir = Path(__file__).resolve().parent

        self.secret_key = self._get_secret_key()
        
        # User and API key storage
        self.users = {}
        self.api_keys = {}
        self.active_sessions = {}
        
        # External identity providers
        self.external_providers: Dict[str, ExternalProviderConfig] = {}
        self._load_external_providers()
        
        # Rate limiting
        self.rate_limiter = RateLimiter(
            max_requests=self.config.get('rate_limit', {}).get('max_requests', 100),
            time_window=self.config.get('rate_limit', {}).get('time_window', 60)
        )
        
        # Security tracking
        self.failed_attempts = defaultdict(list)
        self.security_events = deque(maxlen=1000)
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Load existing data
        self._load_persistent_data()
        
        # Create default admin user if none exists
        self._ensure_default_admin()
        
        logger.info("Authentication manager initialized")
    
    def _load_external_providers(self):
        """Load external identity providers from config"""
        providers = self.config.get('external_identity', {}).get('providers', [])
        for provider_config in providers:
            try:
                provider = ExternalProviderConfig(
                    name=provider_config['name'],
                    issuer=provider_config['issuer'],
                    audience=provider_config['audience'],
                    secret=provider_config['secret'],
                    algorithm=provider_config.get('algorithm', 'HS256')
                )
                self.external_providers[provider.name] = provider
            except KeyError as e:
                logger.warning(f"Invalid provider config: missing {e}")

    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Load authentication configuration"""
        default_config = {
            'jwt': {
                'algorithm': 'HS256',
                'expiry_hours': 24,
                'refresh_expiry_days': 30
            },
            'api_keys': {
                'default_expiry_days': 365,
                'key_length': 32
            },
            'rate_limit': {
                'max_requests': 100,
                'time_window': 60,
                'enable_per_user': True,
                'enable_per_ip': True
            },
            'security': {
                'max_failed_attempts': 5,
                'lockout_duration_minutes': 15,
                'require_strong_passwords': True,
                'log_security_events': True,
                'allow_secret_autogenerate': False,
                'allow_bootstrap_admin': False
            },
            'sessions': {
                'max_sessions_per_user': 5,
                'session_timeout_hours': 8
            },
            'external_identity': {
                'providers': []
            }
        }
        
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    file_config = json.load(f)
                default_config.update(file_config)
            except Exception as e:
                logger.warning(f"Error loading auth config: {e}, using defaults")
        
        return default_config
    
    def _get_secret_key(self) -> str:
        """Resolve JWT secret key from environment or runtime storage"""
        secret = os.environ.get('SAIQL_JWT_SECRET')
        if secret:
            return secret

        secret_file = self.storage_dir / 'jwt_secret.key'
        if secret_file.exists():
            try:
                candidate = secret_file.read_text().strip()
                if candidate:
                    return candidate
                logger.warning("JWT secret file is empty; ignoring stored value")
            except Exception as e:
                logger.warning(f"Error reading JWT secret file: {e}")

        if not self.config.get('security', {}).get('allow_secret_autogenerate', False):
            raise RuntimeError(
                "JWT secret not configured. Set SAIQL_JWT_SECRET or place a key file at "
                f"{secret_file}"
            )

        secret = secrets.token_urlsafe(64)
        try:
            secret_file.write_text(secret)
            secret_file.chmod(0o600)
            logger.info("Generated new JWT secret key in runtime storage")
        except Exception as e:
            logger.warning(f"Could not persist generated JWT secret: {e}")

        return secret
    
    def _load_persistent_data(self):
        """Load users and API keys from persistent storage"""
        self.users.clear()
        self.api_keys.clear()
        try:
            # Load users
            users_file = self.storage_dir / 'users.json'
            if users_file.exists():
                with open(users_file, 'r') as f:
                    users_data = json.load(f)
                    for user_data in users_data:
                        user = User(
                            user_id=user_data['user_id'],
                            username=user_data['username'],
                            email=user_data['email'],
                            roles=[UserRole(role) for role in user_data['roles']],
                            created_at=datetime.fromisoformat(user_data['created_at']),
                            last_login=datetime.fromisoformat(user_data['last_login']) if user_data.get('last_login') else None,
                            is_active=user_data.get('is_active', True),
                            metadata=user_data.get('metadata', {})
                        )
                        self.users[user.user_id] = user
            
            # Load API keys
            keys_file = self.storage_dir / 'api_keys.json'
            if keys_file.exists():
                with open(keys_file, 'r') as f:
                    keys_data = json.load(f)
                    for key_data in keys_data:
                        api_key = APIKey(
                            key_id=key_data['key_id'],
                            key_hash=key_data['key_hash'],
                            user_id=key_data['user_id'],
                            name=key_data['name'],
                            roles=[UserRole(role) for role in key_data['roles']],
                            created_at=datetime.fromisoformat(key_data['created_at']),
                            expires_at=datetime.fromisoformat(key_data['expires_at']) if key_data.get('expires_at') else None,
                            last_used=datetime.fromisoformat(key_data['last_used']) if key_data.get('last_used') else None,
                            usage_count=key_data.get('usage_count', 0),
                            is_active=key_data.get('is_active', True),
                            rate_limit=key_data.get('rate_limit'),
                            allowed_ips=key_data.get('allowed_ips', [])
                        )
                        self.api_keys[api_key.key_id] = api_key
                
        except Exception as e:
            logger.error(f"Error loading persistent data: {e}")
    
    def _save_persistent_data(self):
        """Save users and API keys to persistent storage"""
        try:
            self.storage_dir.mkdir(parents=True, exist_ok=True)
            
            # Save users
            users_data = []
            for user in self.users.values():
                users_data.append({
                    'user_id': user.user_id,
                    'username': user.username,
                    'email': user.email,
                    'roles': [role.value for role in user.roles],
                    'created_at': user.created_at.isoformat(),
                    'last_login': user.last_login.isoformat() if user.last_login else None,
                    'is_active': user.is_active,
                    'metadata': user.metadata
                })
            
            with open(self.storage_dir / 'users.json', 'w') as f:
                json.dump(users_data, f, indent=2)
            
            # Save API keys
            keys_data = []
            for api_key in self.api_keys.values():
                keys_data.append({
                    'key_id': api_key.key_id,
                    'key_hash': api_key.key_hash,
                    'user_id': api_key.user_id,
                    'name': api_key.name,
                    'roles': [role.value for role in api_key.roles],
                    'created_at': api_key.created_at.isoformat(),
                    'expires_at': api_key.expires_at.isoformat() if api_key.expires_at else None,
                    'last_used': api_key.last_used.isoformat() if api_key.last_used else None,
                    'usage_count': api_key.usage_count,
                    'is_active': api_key.is_active,
                    'rate_limit': api_key.rate_limit,
                    'allowed_ips': api_key.allowed_ips
                })
            
            with open(self.storage_dir / 'api_keys.json', 'w') as f:
                json.dump(keys_data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error saving persistent data: {e}")
    
    def _ensure_default_admin(self):
        """Ensure at least one administrative user is present"""
        admin_users = [user for user in self.users.values() if UserRole.ADMIN in user.roles]
        if admin_users:
            return

        allow_bootstrap = (
            os.environ.get('SAIQL_BOOTSTRAP_TEMPLATE', '').lower() == 'true'
            or self.config.get('security', {}).get('allow_bootstrap_admin', False)
        )

        if allow_bootstrap:
            self._seed_from_templates()
            admin_users = [user for user in self.users.values() if UserRole.ADMIN in user.roles]
            if admin_users:
                logger.warning(
                    "Bootstrap admin user(s) loaded from template. Update credentials and rotate "
                    "API keys immediately."
                )
                return

        raise RuntimeError(
            "No administrative users configured. Provide at least one admin record in "
            f"{self.storage_dir / 'users.json'} or explicitly enable bootstrap via "
            "SAIQL_BOOTSTRAP_TEMPLATE=true."
        )

    def _seed_from_templates(self):
        """Load placeholder users/API keys from templates for bootstrap scenarios"""
        try:
            users_template = self.template_dir / 'users.json.template'
            if users_template.exists():
                shutil.copy(users_template, self.storage_dir / 'users.json')
            else:
                (self.storage_dir / 'users.json').write_text('[]')

            keys_template = self.template_dir / 'api_keys.json.template'
            if keys_template.exists():
                shutil.copy(keys_template, self.storage_dir / 'api_keys.json')
            else:
                (self.storage_dir / 'api_keys.json').write_text('[]')

            self._load_persistent_data()
            
            # Set default password for bootstrap admin
            admin = self.users.get("admin")
            if admin and not admin.metadata.get("password_hash"):
                 logger.warn("Setting default password for bootstrap admin user: 'admin_password'")
                 self.set_password("admin", "admin_password")
        except Exception as exc:
            logger.error(f"Bootstrap from templates failed: {exc}")
    
    def create_user(self, username: str, email: str, roles: List[UserRole], 
                   user_id: Optional[str] = None) -> User:
        """Create new user"""
        if user_id is None:
            user_id = f"user_{secrets.token_hex(8)}"
        
        with self._lock:
            if user_id in self.users:
                raise ValueError(f"User {user_id} already exists")
            
            user = User(
                user_id=user_id,
                username=username,
                email=email,
                roles=roles,
                created_at=datetime.now()
            )
            
            self.users[user_id] = user
            self._save_persistent_data()
            
            self._log_security_event("user_created", user_id=user_id, username=username)
            
            return user

    def set_password(self, user_id: str, password: str) -> bool:
        """Set user password"""
        import bcrypt
        user = self.users.get(user_id)
        if not user:
            return False
            
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
        
        with self._lock:
            user.metadata['password_hash'] = hashed.decode('utf-8')
            self._save_persistent_data()
            
        return True

    def authenticate_user(self, username: str, password: str) -> Optional[User]:
        """Authenticate user with password"""
        import bcrypt
        
        # Find user by username
        user = next((u for u in self.users.values() if u.username == username), None)
        if not user or not user.is_active:
            return None
            
        # Check password
        stored_hash = user.metadata.get('password_hash')
        if not stored_hash:
            return None
            
        try:
            if bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8')):
                with self._lock:
                    user.last_login = datetime.now()
                    self._save_persistent_data()
                return user
        except Exception as e:
            logger.error(f"Password check failed: {e}")
            
        return None
    
    def create_token(self, user_id: str, additional_claims: Optional[Dict] = None) -> str:
        """Create JWT token for user"""
        user = self.users.get(user_id)
        if not user or not user.is_active:
            raise ValueError("Invalid or inactive user")
        
        now = datetime.utcnow()
        expiry = now + timedelta(hours=self.config['jwt']['expiry_hours'])
        
        payload = {
            'user_id': user_id,
            'username': user.username,
            'roles': [role.value for role in user.roles],
            'iat': now,
            'exp': expiry,
            'iss': 'SAIQL-Bravo'
        }
        
        if additional_claims:
            payload.update(additional_claims)
        
        token = jwt.encode(
            payload,
            self.secret_key,
            algorithm=self.config['jwt']['algorithm']
        )
        
        # Update last login
        user.last_login = now
        self._save_persistent_data()
        
        self._log_security_event("token_created", user_id=user_id)
        
        return token

    def refresh_token(self, token: str) -> AuthResult:
        """Refresh an access token, returning a new JWT for the same user."""
        verification = self.verify_token(token)
        if not verification.success or not verification.user:
            return verification

        additional = dict(verification.metadata.get('token_payload', {}))
        additional.pop('exp', None)
        additional.pop('iat', None)

        new_token = self.create_token(verification.user_id, additional_claims=additional)
        expiry = datetime.utcnow() + timedelta(hours=self.config['jwt']['expiry_hours'])

        refreshed_result = AuthResult(
            success=True,
            user_id=verification.user_id,
            user=verification.user,
            roles=verification.roles,
            auth_method=AuthMethod.JWT_TOKEN,
            token_expires_at=expiry,
            metadata={
                'token': new_token,
                'refreshed_from': token
            }
        )

        self._log_security_event("token_refreshed", user_id=verification.user_id)
        return refreshed_result
    
    def verify_token(self, token: str) -> AuthResult:
        """Verify JWT token"""
        try:
            payload = jwt.decode(
                token,
                self.secret_key,
                algorithms=[self.config['jwt']['algorithm']],
                issuer='SAIQL-Bravo'
            )
            
            user_id = payload.get('user_id')
            user = self.users.get(user_id)
            
            if not user or not user.is_active:
                return AuthResult(success=False, error_message="User not found or inactive")
            
            roles = [UserRole(role) for role in payload.get('roles', [])]
            
            return AuthResult(
                success=True,
                user_id=user_id,
                user=user,
                roles=roles,
                auth_method=AuthMethod.JWT_TOKEN,
                token_expires_at=datetime.fromtimestamp(payload['exp']),
                metadata={'token_payload': payload}
            )
            
        except jwt.ExpiredSignatureError:
            return AuthResult(success=False, error_message="Token expired")
        except jwt.InvalidTokenError as e:
            return AuthResult(success=False, error_message=f"Invalid token: {str(e)}")
        except Exception as e:
            logger.error(f"Token verification error: {e}")
            return AuthResult(success=False, error_message="Token verification failed")
    
    def create_api_key(self, user_id: str, name: str, roles: List[UserRole],
                      expires_days: Optional[int] = None, rate_limit: Optional[int] = None,
                      allowed_ips: Optional[List[str]] = None) -> Tuple[APIKey, str]:
        """Create API key for user"""
        user = self.users.get(user_id)
        if not user:
            raise ValueError("User not found")
        
        # Generate key
        key_secret = secrets.token_urlsafe(self.config['api_keys']['key_length'])
        key_hash = hashlib.sha256(key_secret.encode()).hexdigest()
        key_id = f"sk_{secrets.token_hex(8)}"
        
        # Set expiry
        expires_at = None
        if expires_days:
            expires_at = datetime.now() + timedelta(days=expires_days)
        
        api_key = APIKey(
            key_id=key_id,
            key_hash=key_hash,
            user_id=user_id,
            name=name,
            roles=roles,
            created_at=datetime.now(),
            expires_at=expires_at,
            rate_limit=rate_limit,
            allowed_ips=allowed_ips or []
        )
        
        with self._lock:
            self.api_keys[key_id] = api_key
            self._save_persistent_data()
        
        self._log_security_event("api_key_created", user_id=user_id, key_id=key_id, name=name)
        
        return api_key, key_secret

    def rotate_api_key(self, key_id: str, expires_days: Optional[int] = None) -> Tuple[APIKey, str]:
        """Rotate an API key by deactivating the old one and issuing a new secret."""
        with self._lock:
            existing = self.api_keys.get(key_id)
            if not existing or not existing.is_active:
                raise ValueError("API key not found or already inactive")

            existing.is_active = False
            self.api_keys[key_id] = existing
            self._save_persistent_data()

        remaining_days = expires_days
        if remaining_days is None and existing.expires_at:
            delta = existing.expires_at - datetime.now()
            remaining_days = max(int(delta.total_seconds() // 86400), 1)

        new_key, secret = self.create_api_key(
            user_id=existing.user_id,
            name=existing.name,
            roles=list(existing.roles),
            expires_days=remaining_days,
            rate_limit=existing.rate_limit,
            allowed_ips=list(existing.allowed_ips)
        )

        self._log_security_event(
            "api_key_rotated",
            previous_key_id=key_id,
            new_key_id=new_key.key_id,
            user_id=existing.user_id
        )

        return new_key, secret
    
    def verify_api_key(self, key_secret: str, client_ip: Optional[str] = None) -> AuthResult:
        """Verify API key"""
        key_hash = hashlib.sha256(key_secret.encode()).hexdigest()
        
        # Find matching API key
        api_key = None
        for key in self.api_keys.values():
            if key.key_hash == key_hash and key.is_active:
                api_key = key
                break
        
        if not api_key:
            self._log_security_event("api_key_invalid", client_ip=client_ip)
            return AuthResult(success=False, error_message="Invalid API key")
        
        # Check expiry
        if api_key.expires_at and datetime.now() > api_key.expires_at:
            return AuthResult(success=False, error_message="API key expired")
        
        # Check IP restrictions
        if api_key.allowed_ips and client_ip not in api_key.allowed_ips:
            self._log_security_event("api_key_ip_blocked", key_id=api_key.key_id, client_ip=client_ip)
            return AuthResult(success=False, error_message="IP not allowed")
        
        # Check rate limit
        if api_key.rate_limit:
            allowed, remaining = self.rate_limiter.is_allowed(f"api_key_{api_key.key_id}")
            if not allowed:
                return AuthResult(success=False, error_message="Rate limit exceeded")
        
        # Get user
        user = self.users.get(api_key.user_id)
        if not user or not user.is_active:
            return AuthResult(success=False, error_message="User not found or inactive")
        
        # Update usage
        with self._lock:
            api_key.last_used = datetime.now()
            api_key.usage_count += 1
            self._save_persistent_data()
        
        return AuthResult(
            success=True,
            user_id=user.user_id,
            user=user,
            roles=api_key.roles,
            auth_method=AuthMethod.API_KEY,
            metadata={'api_key_id': api_key.key_id, 'usage_count': api_key.usage_count}
        )
    
    def _log_security_event(self, event_type: str, **kwargs):
        """Log security event"""
        if not self.config.get('security', {}).get('log_security_events', True):
            return
        
        event = {
            'timestamp': datetime.now().isoformat(),
            'event_type': event_type,
            'details': kwargs
        }
        
        self.security_events.append(event)
        logger.info(f"Security event: {event_type} - {kwargs}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get authentication statistics"""
        return {
            'total_users': len(self.users),
            'active_users': len([u for u in self.users.values() if u.is_active]),
            'total_api_keys': len(self.api_keys),
            'active_api_keys': len([k for k in self.api_keys.values() if k.is_active]),
            'recent_security_events': list(self.security_events)[-10:],
            'rate_limiter_stats': {
                'tracked_identifiers': len(self.rate_limiter.requests)
            }
        }

# Flask middleware for authentication
def create_auth_middleware(auth_manager: AuthManager):
    """Create Flask middleware for authentication"""
    
    def authenticate_request():
        """Authenticate incoming request"""
        from flask import request, g
        
        # Skip authentication for health check and public endpoints
        if request.path in ['/health', '/version', '/']:
            return None
        
        auth_result = None
        
        # Try JWT token from Authorization header
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header[7:]
            auth_result = auth_manager.verify_token(token)
        
        # Try API key from header
        elif request.headers.get('X-API-Key'):
            api_key = request.headers.get('X-API-Key')
            client_ip = request.remote_addr
            auth_result = auth_manager.verify_api_key(api_key, client_ip)

        # NOTE: API keys via URL query string (?api_key=...) are NOT supported.
        # Query strings are logged in access logs, browser history, and Referer headers,
        # making them insecure for credential transmission.
        
        if auth_result and auth_result.success:
            g.current_user = auth_result.user
            g.user_roles = auth_result.roles
            g.auth_method = auth_result.auth_method
            return None
        else:
            from flask import jsonify
            return jsonify({
                'error': 'Authentication required',
                'message': auth_result.error_message if auth_result else 'No authentication provided'
            }), 401
    
    return authenticate_request

# Example usage
if __name__ == "__main__":
    # Test authentication manager
    auth_manager = AuthManager()
    
    print("Authentication Manager Test")
    print("=" * 50)
    
    # Get statistics
    stats = auth_manager.get_statistics()
    print(f"Statistics: {json.dumps(stats, indent=2)}")
    
    # Create test user
    try:
        user = auth_manager.create_user(
            username="testuser",
            email="test@example.com",
            roles=[UserRole.READ_WRITE]
        )
        print(f"Created user: {user.username}")
        
        # Create API key
        api_key, key_secret = auth_manager.create_api_key(
            user_id=user.user_id,
            name="Test Key",
            roles=[UserRole.READ_WRITE],
            expires_days=30
        )
        print(f"Created API key: {key_secret}")
        
        # Test API key verification
        auth_result = auth_manager.verify_api_key(key_secret)
        print(f"API key verification: {auth_result.success}")
        
        # Create JWT token
        token = auth_manager.create_token(user.user_id)
        print(f"Created token: {token[:50]}...")
        
        # Test token verification
        token_result = auth_manager.verify_token(token)
        print(f"Token verification: {token_result.success}")
        
    except Exception as e:
        print(f"Test error: {e}")
    
    # Final statistics
    final_stats = auth_manager.get_statistics()
    print(f"Final statistics: {json.dumps(final_stats, indent=2)}")
