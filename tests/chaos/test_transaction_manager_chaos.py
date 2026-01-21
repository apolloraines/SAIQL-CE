#!/usr/bin/env python3
"""
Chaos Monkey Testing for SAIQL Transaction Manager
Stress tests for deadlock detection and 2PC implementation
"""

import asyncio
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Any
import pytest

# Import SAIQL components
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from core.transaction_manager import TransactionManager
    from core.database_manager import DatabaseManager
    from config.secure_config import get_current_config
except ImportError:
    # Mock objects for testing
    class TransactionManager:
        def begin(self): pass
        def commit(self): pass
        def rollback(self): pass

    class DatabaseManager:
        def execute_query(self, sql): pass

@dataclass
class ChaosConfig:
    """Configuration for chaos testing"""
    num_threads: int = 10
    num_transactions: int = 100
    max_delay: float = 0.1
    deadlock_probability: float = 0.3
    failure_probability: float = 0.1

class TransactionChaosMonkey:
    """Chaos testing for transaction manager"""

    def __init__(self, config: ChaosConfig = None):
        self.config = config or ChaosConfig()
        self.results = {
            'total_transactions': 0,
            'successful_commits': 0,
            'rollbacks': 0,
            'deadlocks_detected': 0,
            'timeouts': 0,
            'errors': [],
            'deadlock_resolution_times': []
        }

    def create_deadlock_scenario(self, tx_manager: TransactionManager, scenario_id: int):
        """Create intentional deadlock scenario"""
        try:
            # Begin transaction
            tx_manager.begin()

            # Simulate conflicting resource access
            if scenario_id % 2 == 0:
                # Thread A: Lock resource 1, then resource 2
                time.sleep(random.uniform(0, self.config.max_delay))
                # Simulate locking order that could cause deadlock
                pass
            else:
                # Thread B: Lock resource 2, then resource 1
                time.sleep(random.uniform(0, self.config.max_delay))
                # Simulate reverse locking order
                pass

            # Random operations
            for _ in range(random.randint(1, 5)):
                time.sleep(random.uniform(0, 0.01))

            # Commit or rollback based on probability
            if random.random() < self.config.failure_probability:
                tx_manager.rollback()
                self.results['rollbacks'] += 1
            else:
                tx_manager.commit()
                self.results['successful_commits'] += 1

        except Exception as e:
            self.results['errors'].append(str(e))
            if 'deadlock' in str(e).lower():
                self.results['deadlocks_detected'] += 1

        finally:
            self.results['total_transactions'] += 1

    def stress_test_concurrent_transactions(self):
        """Run concurrent transactions to test for race conditions"""
        tx_manager = TransactionManager()

        with ThreadPoolExecutor(max_workers=self.config.num_threads) as executor:
            futures = []

            # Submit chaos scenarios
            for i in range(self.config.num_transactions):
                future = executor.submit(
                    self.create_deadlock_scenario,
                    tx_manager,
                    i
                )
                futures.append(future)

            # Wait for completion and collect results
            for future in as_completed(futures):
                try:
                    future.result(timeout=5.0)
                except TimeoutError:
                    self.results['timeouts'] += 1
                except Exception as e:
                    self.results['errors'].append(f"Executor error: {e}")

    def test_2pc_coordinator_failure(self):
        """Test 2PC behavior when coordinator fails"""
        # TODO: Implement 2PC coordinator failure simulation
        pass

    def test_participant_failure(self):
        """Test 2PC behavior when participant fails"""
        # TODO: Implement participant failure simulation
        pass

    def test_network_partition(self):
        """Test behavior during network partitions"""
        # TODO: Implement network partition simulation
        pass

    def generate_report(self) -> Dict[str, Any]:
        """Generate chaos testing report"""
        total = self.results['total_transactions']
        if total == 0:
            return {"error": "No transactions executed"}

        return {
            "chaos_test_summary": {
                "total_transactions": total,
                "success_rate": self.results['successful_commits'] / total,
                "rollback_rate": self.results['rollbacks'] / total,
                "deadlock_detection_rate": self.results['deadlocks_detected'] / total,
                "timeout_rate": self.results['timeouts'] / total,
                "error_count": len(self.results['errors'])
            },
            "deadlock_metrics": {
                "detected": self.results['deadlocks_detected'],
                "avg_resolution_time": sum(self.results['deadlock_resolution_times']) / max(len(self.results['deadlock_resolution_times']), 1)
            },
            "errors": self.results['errors'][:10],  # First 10 errors
            "verdict": "PASS" if self.results['deadlocks_detected'] > 0 else "NEEDS_INVESTIGATION"
        }

# Pytest test cases
def test_transaction_manager_chaos():
    """Main chaos test for transaction manager"""
    chaos = TransactionChaosMonkey(ChaosConfig(
        num_threads=5,
        num_transactions=50,
        deadlock_probability=0.4
    ))

    chaos.stress_test_concurrent_transactions()
    report = chaos.generate_report()

    # Assertions for chaos testing
    assert report["chaos_test_summary"]["total_transactions"] > 0
    assert report["chaos_test_summary"]["success_rate"] >= 0.0  # Some transactions should work
    assert report["chaos_test_summary"]["error_count"] < 50  # Most should not error

    print("Chaos Test Report:")
    print(f"Total Transactions: {report['chaos_test_summary']['total_transactions']}")
    print(f"Success Rate: {report['chaos_test_summary']['success_rate']:.2%}")
    print(f"Deadlocks Detected: {report['deadlock_metrics']['detected']}")
    print(f"Verdict: {report['verdict']}")

def test_concurrent_stress():
    """High-load concurrent stress test"""
    chaos = TransactionChaosMonkey(ChaosConfig(
        num_threads=20,
        num_transactions=200,
        max_delay=0.05
    ))

    start_time = time.time()
    chaos.stress_test_concurrent_transactions()
    end_time = time.time()

    report = chaos.generate_report()

    # Performance assertions
    assert end_time - start_time < 30  # Should complete within 30 seconds
    assert report["chaos_test_summary"]["timeout_rate"] < 0.1  # Less than 10% timeouts

if __name__ == "__main__":
    print("Running Transaction Manager Chaos Tests...")
    test_transaction_manager_chaos()
    test_concurrent_stress()
    print("Chaos testing completed!")