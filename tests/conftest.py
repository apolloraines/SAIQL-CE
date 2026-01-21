#!/usr/bin/env python3
"""
SAIQL Test Configuration - PyTest Configuration and Fixtures

This module provides shared fixtures and configuration for SAIQL-Bravo tests.
It sets up test databases, authentication, and common test utilities.

Author: Apollo & Claude
Version: 1.0.0
Status: Production-Ready for SAIQL-Bravo
"""

import pytest
import tempfile
import os
import sys
import json
import sqlite3
from pathlib import Path
from typing import Dict, Any, Generator
import threading
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import SAIQL components
from core.engine import SAIQLEngine, ExecutionContext
from core.database_manager import DatabaseManager
from security.auth_manager import AuthManager, User, UserRole
from interface.saiql_server_secured import SAIQLServer

@pytest.fixture(scope="session")
def temp_dir():
    """Create temporary directory for test files"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir

@pytest.fixture(scope="session")
def test_config(temp_dir):
    """Create test configuration"""
    config = {
        "database": {
            "default_backend": "sqlite",
            "backends": {
                "sqlite": {
                    "type": "sqlite",
                    "path": os.path.join(temp_dir, "test_saiql.db"),
                    "timeout": 30
                },
                "sqlite_memory": {
                    "type": "sqlite", 
                    "path": ":memory:",
                    "timeout": 30
                }
            }
        },
        "legend": {
            "path": "data/legend_map.lore"
        },
        "compilation": {
            "target_dialect": "sqlite",
            "optimization_level": "standard",
            "enable_caching": True
        },
        "execution": {
            "default_timeout": 300,
            "max_memory_mb": 1024,
            "enable_async": False
        },
        "cache_size": 100,
        "session_cleanup_interval": 3600,
        "performance_tracking": True
    }
    
    # Save config to file
    config_path = os.path.join(temp_dir, "test_config.json")
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    return config, config_path

@pytest.fixture(scope="session")
def test_database(temp_dir):
    """Create and populate test database"""
    db_path = os.path.join(temp_dir, "test_saiql.db")
    
    # Create database with test data
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        # Create tables
        cursor.executescript("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                age INTEGER,
                department TEXT,
                salary DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                product TEXT NOT NULL,
                price DECIMAL(10,2),
                quantity INTEGER DEFAULT 1,
                status TEXT DEFAULT 'pending',
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id)
            );
            
            CREATE TABLE products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price DECIMAL(10,2),
                category TEXT,
                stock INTEGER DEFAULT 0,
                description TEXT
            );
            
            CREATE TABLE categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                parent_id INTEGER,
                FOREIGN KEY (parent_id) REFERENCES categories (id)
            );
        """)
        
        # Insert test data
        users_data = [
            ('Alice Johnson', 'alice@example.com', 28, 'Engineering', 75000.00),
            ('Bob Smith', 'bob@example.com', 35, 'Sales', 65000.00),
            ('Carol Davis', 'carol@example.com', 42, 'Engineering', 85000.00),
            ('David Wilson', 'david@example.com', 29, 'Marketing', 55000.00),
            ('Eve Brown', 'eve@example.com', 31, 'Sales', 70000.00)
        ]
        
        cursor.executemany(
            "INSERT INTO users (name, email, age, department, salary) VALUES (?, ?, ?, ?, ?)",
            users_data
        )
        
        orders_data = [
            (1, 'Laptop Pro', 1299.99, 1, 'completed'),
            (1, 'Mouse Wireless', 29.99, 2, 'completed'),
            (2, 'Keyboard Mechanical', 149.99, 1, 'pending'),
            (3, 'Monitor 4K', 399.99, 1, 'shipped'),
            (2, 'Headphones', 199.99, 1, 'completed'),
            (4, 'Tablet', 599.99, 1, 'pending'),
            (5, 'Smartphone', 899.99, 1, 'completed')
        ]
        
        cursor.executemany(
            "INSERT INTO orders (user_id, product, price, quantity, status) VALUES (?, ?, ?, ?, ?)",
            orders_data
        )
        
        products_data = [
            ('Laptop Pro', 1299.99, 'Electronics', 50, 'High-performance laptop'),
            ('Mouse Wireless', 29.99, 'Electronics', 200, 'Wireless optical mouse'),
            ('Keyboard Mechanical', 149.99, 'Electronics', 75, 'RGB mechanical keyboard'),
            ('Monitor 4K', 399.99, 'Electronics', 30, '27-inch 4K monitor'),
            ('Headphones', 199.99, 'Electronics', 100, 'Noise-canceling headphones'),
            ('Tablet', 599.99, 'Electronics', 25, '10-inch tablet with stylus'),
            ('Smartphone', 899.99, 'Electronics', 40, 'Latest smartphone model')
        ]
        
        cursor.executemany(
            "INSERT INTO products (name, price, category, stock, description) VALUES (?, ?, ?, ?, ?)",
            products_data
        )
        
        categories_data = [
            ('Electronics', None),
            ('Computers', 1),
            ('Mobile Devices', 1),
            ('Accessories', 1)
        ]
        
        cursor.executemany(
            "INSERT INTO categories (name, parent_id) VALUES (?, ?)",
            categories_data
        )
        
        conn.commit()
    
    return db_path

@pytest.fixture
def database_manager(test_config):
    """Create database manager with test configuration"""
    config, config_path = test_config
    manager = DatabaseManager(config_path)
    yield manager
    manager.close_all()

@pytest.fixture
def saiql_engine(test_config):
    """Create SAIQL engine with test configuration"""
    config, config_path = test_config
    engine = SAIQLEngine(config_path)
    yield engine
    engine.shutdown()

@pytest.fixture
def auth_manager(temp_dir):
    """Create authentication manager for testing"""
    # Create auth config
    auth_config = {
        'jwt': {
            'algorithm': 'HS256',
            'expiry_hours': 1,  # Short expiry for testing
            'refresh_expiry_days': 1
        },
        'api_keys': {
            'default_expiry_days': 30,
            'key_length': 16  # Shorter for testing
        },
        'rate_limit': {
            'max_requests': 1000,  # High limit for testing
            'time_window': 60
        },
        'security': {
            'max_failed_attempts': 3,
            'lockout_duration_minutes': 1,
            'log_security_events': True,
            'allow_bootstrap_admin': True,
            'allow_secret_autogenerate': True
        }
    }
    
    config_path = os.path.join(temp_dir, "auth_config.json")
    with open(config_path, 'w') as f:
        json.dump(auth_config, f)
    
    # Change to temp directory for auth manager files
    original_cwd = os.getcwd()
    os.chdir(temp_dir)
    
    security_state = os.path.join(temp_dir, "security_state")
    original_env = {
        'SAIQL_JWT_SECRET': os.environ.get('SAIQL_JWT_SECRET'),
        'SAIQL_SECURITY_STATE': os.environ.get('SAIQL_SECURITY_STATE'),
        'SAIQL_BOOTSTRAP_TEMPLATE': os.environ.get('SAIQL_BOOTSTRAP_TEMPLATE')
    }
    os.environ['SAIQL_JWT_SECRET'] = 'unit-test-secret-key'
    os.environ['SAIQL_SECURITY_STATE'] = security_state
    os.environ['SAIQL_BOOTSTRAP_TEMPLATE'] = 'true'
    
    try:
        manager = AuthManager(config_path)
        yield manager
    finally:
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        os.chdir(original_cwd)

@pytest.fixture
def test_user(auth_manager):
    """Create test user"""
    user = auth_manager.create_user(
        username="testuser",
        email="test@example.com",
        roles=[UserRole.READ_WRITE],
        user_id="test_user_123"
    )
    return user

@pytest.fixture
def admin_user(auth_manager):
    """Create admin test user"""
    user = auth_manager.create_user(
        username="admin",
        email="admin@example.com", 
        roles=[UserRole.ADMIN],
        user_id="admin_user_123"
    )
    return user

@pytest.fixture
def test_api_key(auth_manager, test_user):
    """Create test API key"""
    api_key, key_secret = auth_manager.create_api_key(
        user_id=test_user.user_id,
        name="Test API Key",
        roles=[UserRole.READ_WRITE],
        expires_days=30
    )
    return api_key, key_secret

@pytest.fixture
def test_server(test_config, temp_dir):
    """Create test server instance"""
    config, config_path = test_config
    
    # Create server config
    server_config = {
        'server': {
            'host': '127.0.0.1',
            'port': 0,  # Let system assign port
            'debug': True,
            'threaded': True
        },
        'security': {
            'require_auth': False,  # Disable auth for basic tests
            'log_requests': False
        },
        'api': {
            'max_query_length': 1000,
            'default_timeout': 30,
            'max_batch_size': 10
        }
    }
    
    server_config_path = os.path.join(temp_dir, "server_config.json")
    with open(server_config_path, 'w') as f:
        json.dump(server_config, f)
    
    # Change to temp directory
    original_cwd = os.getcwd()
    os.chdir(temp_dir)
    
    try:
        server = SAIQLServer(server_config_path)
        yield server
    finally:
        os.chdir(original_cwd)

@pytest.fixture
def sample_queries():
    """Sample SAIQL queries for testing"""
    return {
        'simple_select': "*3[users]::name,email>>oQ",
        'count_query': "*COUNT[users]::*>>oQ", 
        'join_query': "=J[users+orders]::users.name,orders.product>>oQ",
        'filter_query': "*3[users]::name,age|age>30>>oQ",
        'transaction': "$1",
        'invalid_syntax': "*3[nonexistent]::>>oQ",
        'complex_query': "=J[users+orders]::users.name,SUM(orders.price)|users.department='Engineering'>>GROUP(users.name)>>oQ"
    }

@pytest.fixture
def execution_context():
    """Create test execution context"""
    return ExecutionContext(
        session_id="test_session_123",
        user_id="test_user",
        timeout_seconds=30,
        debug=True
    )

# Test utilities
class TestTimer:
    """Simple timer for performance testing"""
    def __init__(self):
        self.start_time = None
        self.end_time = None
    
    def start(self):
        self.start_time = time.time()
    
    def stop(self):
        self.end_time = time.time()
        return self.elapsed()
    
    def elapsed(self):
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0

@pytest.fixture
def timer():
    """Performance timer fixture"""
    return TestTimer()

# Custom markers for test organization
def pytest_configure(config):
    """Configure custom pytest markers"""
    config.addinivalue_line(
        "markers", "unit: Unit tests that test individual components"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests that test component interaction"
    )
    config.addinivalue_line(
        "markers", "performance: Performance and load tests"
    )
    config.addinivalue_line(
        "markers", "auth: Authentication and authorization tests"
    )
    config.addinivalue_line(
        "markers", "database: Database-related tests"
    )
    config.addinivalue_line(
        "markers", "api: REST API tests"
    )
    config.addinivalue_line(
        "markers", "slow: Tests that take more than a few seconds"
    )

# Test data generators
def generate_test_queries(count: int = 100):
    """Generate test queries for performance testing"""
    queries = []
    
    for i in range(count):
        # Generate various query patterns
        if i % 4 == 0:
            queries.append(f"*3[users]::name,email|id={i % 5 + 1}>>oQ")
        elif i % 4 == 1:
            queries.append(f"*COUNT[orders]::*|user_id={i % 5 + 1}>>oQ")
        elif i % 4 == 2:
            queries.append("=J[users+orders]::users.name,orders.product>>oQ")
        else:
            queries.append(f"*3[products]::name,price|stock>{i % 100}>>oQ")
    
    return queries

@pytest.fixture
def performance_queries():
    """Generate queries for performance testing"""
    return generate_test_queries(50)

# Cleanup utilities
@pytest.fixture(autouse=True)
def cleanup_logs():
    """Clean up log files after tests"""
    yield
    
    # Clean up any test log files
    log_patterns = ['test_*.log', 'saiql_test_*.log']
    for pattern in log_patterns:
        import glob
        for log_file in glob.glob(pattern):
            try:
                os.remove(log_file)
            except:
                pass

# Test environment validation
def pytest_sessionstart(session):
    """Validate test environment before running tests"""
    required_modules = [
        'pytest', 'flask', 'psycopg2', 'jwt', 'cryptography'
    ]
    
    missing_modules = []
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing_modules.append(module)
    
    if missing_modules:
        pytest.exit(f"Missing required modules: {', '.join(missing_modules)}")
    
    print("\n" + "="*50)
    print("SAIQL-Bravo Test Suite")
    print("="*50)
    print(f"Python version: {sys.version}")
    print(f"Test environment validated")
    print("="*50)

def pytest_sessionfinish(session, exitstatus):
    """Clean up after test session"""
    print("\n" + "="*50)
    print("Test session completed")
    print("="*50)
# IHNpZ25hdHVyZTplYzUyNTBkZWRmNTZjZTZiMDA4NCBpZDpTQUlRTC1DSEFSTElFLVBST0QgYnk6QXBvbGxvICYgQ2xhdWRlIA==
