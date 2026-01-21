#!/usr/bin/env python3
"""
SAIQL Regression Suite - Phase 12

Single command to run all harness tests and verify:
- Phase 11 Tier 1 harnesses (PostgreSQL, MySQL, SQLite, MariaDB)
- Phase 12 validation infrastructure
- Bundle integrity checks

Usage:
    python tests/regression_suite.py [--quick] [--full]

Author: Apollo & Claude
Version: 1.0.0
"""

import subprocess
import sys
import argparse
from pathlib import Path
from datetime import datetime
import json


# Test suites configuration
HARNESS_SUITES = {
    # Phase 11 - Tier 1 L0 Harnesses
    "postgresql_l0": {
        "path": "tests/integration/test_phase11_postgresql_l0.py",
        "description": "PostgreSQL L0 Harness",
        "required": True,
        "phase": 11
    },
    "mysql_l0": {
        "path": "tests/integration/test_phase11_mysql_l0.py",
        "description": "MySQL L0 Harness",
        "required": True,
        "phase": 11
    },
    "sqlite_l0": {
        "path": "tests/integration/test_phase11_sqlite_l0.py",
        "description": "SQLite L0 Harness",
        "required": True,
        "phase": 11
    },
    "mariadb_compat": {
        "path": "tests/integration/test_phase11_mariadb_compat.py",
        "description": "MariaDB Compatibility Harness",
        "required": True,
        "phase": 11
    },

    # Phase 11 - Tier 1 L1 Harnesses
    "postgresql_l1": {
        "path": "tests/integration/test_phase11_postgresql_l1.py",
        "description": "PostgreSQL L1 Harness",
        "required": True,
        "phase": 11
    },
    "mysql_l1": {
        "path": "tests/integration/test_phase11_mysql_l1.py",
        "description": "MySQL L1 Harness",
        "required": True,
        "phase": 11
    },

    # Phase 12 - Validation Infrastructure
    "phase12_validation": {
        "path": "tests/integration/test_phase12_validation.py",
        "description": "Phase 12 Validation Infrastructure",
        "required": True,
        "phase": 12
    },
}

# Quick mode only runs essential tests
QUICK_SUITES = ["sqlite_l0", "phase12_validation"]


def run_suite(suite_name: str, suite_config: dict, verbose: bool = True) -> dict:
    """Run a single test suite and return results."""
    result = {
        "suite": suite_name,
        "description": suite_config["description"],
        "path": suite_config["path"],
        "passed": False,
        "tests_run": 0,
        "tests_passed": 0,
        "tests_failed": 0,
        "duration": 0.0,
        "error": None
    }

    test_path = Path(suite_config["path"])
    if not test_path.exists():
        result["error"] = f"Test file not found: {test_path}"
        return result

    start_time = datetime.now()

    try:
        cmd = [
            sys.executable, "-m", "pytest",
            str(test_path),
            "-v" if verbose else "-q",
            "--tb=short",
            "-q"  # Add quiet for cleaner output
        ]

        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout per suite
        )

        duration = (datetime.now() - start_time).total_seconds()
        result["duration"] = duration

        # Parse output for test counts
        output = proc.stdout + proc.stderr

        # Look for pytest summary line like "15 passed in 0.03s"
        for line in output.split('\n'):
            if 'passed' in line.lower():
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == 'passed':
                        try:
                            result["tests_passed"] = int(parts[i-1])
                            result["tests_run"] = result["tests_passed"]
                        except (ValueError, IndexError):
                            pass
                    elif part == 'failed':
                        try:
                            result["tests_failed"] = int(parts[i-1])
                            result["tests_run"] += result["tests_failed"]
                        except (ValueError, IndexError):
                            pass

        result["passed"] = proc.returncode == 0

    except subprocess.TimeoutExpired:
        result["error"] = "Test suite timed out"
    except Exception as e:
        result["error"] = str(e)

    return result


def run_regression_suite(quick: bool = False, verbose: bool = True) -> dict:
    """Run the full regression suite."""
    print("=" * 70)
    print("SAIQL REGRESSION SUITE")
    print(f"Mode: {'QUICK' if quick else 'FULL'}")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 70)
    print()

    suites_to_run = QUICK_SUITES if quick else list(HARNESS_SUITES.keys())
    results = []
    all_passed = True

    for suite_name in suites_to_run:
        suite_config = HARNESS_SUITES[suite_name]
        print(f"Running: {suite_config['description']}...")

        result = run_suite(suite_name, suite_config, verbose)
        results.append(result)

        if result["passed"]:
            print(f"  ✓ PASS ({result['tests_passed']} tests, {result['duration']:.2f}s)")
        else:
            print(f"  ✗ FAIL ({result['tests_failed']} failed, {result.get('error', '')})")
            if suite_config["required"]:
                all_passed = False

        print()

    # Summary
    print("=" * 70)
    print("REGRESSION SUITE SUMMARY")
    print("=" * 70)

    total_tests = sum(r["tests_run"] for r in results)
    total_passed = sum(r["tests_passed"] for r in results)
    total_failed = sum(r["tests_failed"] for r in results)
    total_duration = sum(r["duration"] for r in results)

    suites_passed = sum(1 for r in results if r["passed"])
    suites_failed = len(results) - suites_passed

    print(f"  Suites: {suites_passed}/{len(results)} passed")
    print(f"  Tests:  {total_passed}/{total_tests} passed")
    print(f"  Failed: {total_failed}")
    print(f"  Duration: {total_duration:.2f}s")
    print()

    if all_passed:
        print("✓ REGRESSION SUITE PASSED")
    else:
        print("✗ REGRESSION SUITE FAILED")
        print("  Failed suites:")
        for r in results:
            if not r["passed"]:
                print(f"    - {r['suite']}: {r.get('error', 'tests failed')}")

    print("=" * 70)

    return {
        "passed": all_passed,
        "suites_run": len(results),
        "suites_passed": suites_passed,
        "tests_run": total_tests,
        "tests_passed": total_passed,
        "tests_failed": total_failed,
        "duration": total_duration,
        "results": results,
        "timestamp": datetime.now().isoformat()
    }


def main():
    parser = argparse.ArgumentParser(description="SAIQL Regression Suite")
    parser.add_argument("--quick", action="store_true", help="Run quick subset of tests")
    parser.add_argument("--full", action="store_true", help="Run full test suite (default)")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    parser.add_argument("--output", type=str, help="Save results to file")

    args = parser.parse_args()

    quick_mode = args.quick and not args.full

    results = run_regression_suite(quick=quick_mode, verbose=not args.json)

    if args.json:
        print(json.dumps(results, indent=2))

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nResults saved to: {args.output}")

    return 0 if results["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
