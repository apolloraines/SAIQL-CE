#!/usr/bin/env python3
"""
SAIQL Security Verification Utility
===================================

This script performs a lightweight security posture check for local or CI runs.
It validates that critical secrets are externalised and that deployment artefacts
reference the expected environment variables.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import List

REPO_ROOT = Path(__file__).resolve().parents[1]
SECURITY_DIR = REPO_ROOT / "security"
ENV_TEMPLATE = REPO_ROOT / "config" / ".env.template"
DOCKER_COMPOSE = REPO_ROOT / "docker-compose.yml"

REQUIRED_SECRET_FILES = [
    SECURITY_DIR / "jwt_secret.key",
    SECURITY_DIR / "api_keys.json",
    SECURITY_DIR / "users.json",
]

RESERVED_ENV_VARS = [
    "SAIQL_JWT_SECRET",
    "SAIQL_MASTER_KEY",
    "POSTGRES_PASSWORD",
    "MYSQL_ROOT_PASSWORD",
    "MYSQL_PASSWORD",
    "DATABASE_URL",
    "JWT_SECRET_KEY",
    "GF_SECURITY_ADMIN_PASSWORD",
]

class SecurityCheck:
    """Container for security verification results."""

    def __init__(self) -> None:
        self.warnings: List[str] = []
        self.errors: List[str] = []
        self.passed: bool = True

    def fail(self, message: str) -> None:
        self.errors.append(message)
        self.passed = False

    def warn(self, message: str) -> None:
        self.warnings.append(message)


def ensure_secret_files_absent(check: SecurityCheck) -> None:
    """Verify that sensitive files are not present in the repository."""
    for secret_file in REQUIRED_SECRET_FILES:
        if secret_file.exists():
            check.fail(f"Sensitive file should not exist in repository: {secret_file}")


def ensure_env_template(check: SecurityCheck) -> None:
    """Ensure the template exists and includes critical environment variables."""
    if not ENV_TEMPLATE.exists():
        check.fail(f"Missing environment template: {ENV_TEMPLATE}")
        return

    content = ENV_TEMPLATE.read_text()
    missing = [var for var in RESERVED_ENV_VARS if var not in content]
    if missing:
        check.fail(f"Template is missing variables: {', '.join(missing)}")


def ensure_env_values_available(check: SecurityCheck) -> None:
    """Check whether runtime environment exposes the critical variables."""
    missing = [var for var in RESERVED_ENV_VARS if not os.environ.get(var)]
    if missing:
        check.warn(
            "Environment variables not set for current session "
            f"(expected via .env or secret store): {', '.join(missing)}"
        )


def ensure_docker_compose_references(check: SecurityCheck) -> None:
    """Confirm docker-compose references the required env keys."""
    if not DOCKER_COMPOSE.exists():
        check.warn("docker-compose.yml not found; skipping compose checks.")
        return

    content = DOCKER_COMPOSE.read_text()
    required_tokens = ("${POSTGRES_PASSWORD", "${MYSQL_ROOT_PASSWORD", "${JWT_SECRET_KEY")
    missing = [token for token in required_tokens if token not in content]
    if missing:
        check.fail(
            "docker-compose.yml is missing env placeholders: "
            f"{', '.join(missing)}"
        )


def run_checks() -> SecurityCheck:
    check = SecurityCheck()
    ensure_secret_files_absent(check)
    ensure_env_template(check)
    ensure_env_values_available(check)
    ensure_docker_compose_references(check)
    return check


def print_report(check: SecurityCheck) -> None:
    if check.passed:
        status = "✅ SECURITY CHECK PASSED"
    else:
        status = "❌ SECURITY CHECK FAILED"

    print(status)
    if check.errors:
        print("\nErrors:")
        for err in check.errors:
            print(f"  - {err}")
    if check.warnings:
        print("\nWarnings:")
        for warn in check.warnings:
            print(f"  - {warn}")


def main() -> int:
    check = run_checks()
    print_report(check)
    return 0 if check.passed else 1


if __name__ == "__main__":
    sys.exit(main())
