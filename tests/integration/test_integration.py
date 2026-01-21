import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from saiql_production_server import ProductionSAIQLServer

# Mock dependencies
@pytest.fixture
def client():
    # Mock configuration
    with patch('saiql_production_server.ProductionSAIQLServer._load_config') as mock_config:
        mock_config.return_value = {
            "saiql": {"debug": True},
            "server": {"host": "0.0.0.0", "port": 5433},
            "security": {
                "enable_authentication": False, # Disable auth for testing convenience
                "tls_enabled": False
            },
            "logging": {"level": "DEBUG"}
        }
        
        # Mock runtime and transaction manager
        with patch('saiql_production_server.ProductionOperatorRuntime') as MockRuntime, \
             patch('saiql_production_server.TransactionManager') as MockTM, \
             patch('saiql_production_server.AdvancedPerformanceMonitor') as MockPM, \
             patch('saiql_production_server.AuthManager') as MockAuth:
            
            server = ProductionSAIQLServer()
            
            # Setup mock behaviors
            server.runtime.execute_operator.return_value = [{"id": 1, "name": "Test User"}]
            server.transaction_manager.begin_transaction.return_value = "tx_123"
            server.transaction_manager.commit_transaction.return_value = True
            
            return TestClient(server.app)

def test_health_check(client):
    """Test health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "version" in data

def test_query_execution(client):
    """Test query execution endpoint"""
    query = {
        "operation": "*5[users]::name>>oQ",
        "parameters": []
    }
    response = client.post("/query", json=query)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert len(data["result"]) > 0
    assert data["result"][0]["name"] == "Test User"

def test_transaction_lifecycle(client):
    """Test transaction begin and commit"""
    # Begin
    response = client.post("/transaction/begin", params={"isolation_level": "READ_COMMITTED"})
    assert response.status_code == 200
    data = response.json()
    assert "transaction_id" in data
    tx_id = data["transaction_id"]
    
    # Commit
    response = client.post(f"/transaction/{tx_id}/commit")
    assert response.status_code == 200
    assert response.json()["status"] == "committed"

def test_server_status(client):
    """Test server status endpoint"""
    response = client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["server"]["status"] == "running"
    assert "transactions" in data
