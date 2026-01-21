#!/usr/bin/env python3
"""
SAIQL Deployment Configuration Audit
====================================

Scans docker-compose.yml and install scripts for common pitfalls:
  * External port bindings must default to loopback for local dev.
  * Required scripts exist and are executable.
  * Requirements files are synchronised (no obvious duplicates).
"""

from __future__ import annotations

import stat
import sys
import yaml
from pathlib import Path
from typing import List, Set

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCKER_COMPOSE = REPO_ROOT / "docker-compose.yml"
INSTALL_SCRIPTS = [
    REPO_ROOT / "install_minimal.sh",
    REPO_ROOT / "install_system.sh",
]
REQUIREMENTS_FILES = [
    REPO_ROOT / "requirements.txt",
    REPO_ROOT / "requirements-prod.txt",
]


class DeploymentReport:
    def __init__(self) -> None:
        self.issues: List[str] = []
        self.warnings: List[str] = []

    @property
    def passed(self) -> bool:
        return not self.issues

    def fail(self, message: str) -> None:
        self.issues.append(message)

    def warn(self, message: str) -> None:
        self.warnings.append(message)


def check_docker_ports(report: DeploymentReport) -> None:
    if not DOCKER_COMPOSE.exists():
        report.fail("docker-compose.yml not found.")
        return

    try:
        data = yaml.safe_load(DOCKER_COMPOSE.read_text())
        services = data.get("services", {})

        for name, service in services.items():
            ports = service.get("ports", [])
            for mapping in ports:
                host_part = str(mapping).split(":")[0]
                if not host_part.startswith("127.0.0.1"):
                    report.warn(f"Service '{name}' exposes port '{mapping}' beyond loopback.")
    except yaml.YAMLError:
        report.warn("docker-compose.yml contains non-standard tokens; falling back to textual scan.")
        for line in DOCKER_COMPOSE.read_text().splitlines():
            if "0.0.0.0:" in line or line.strip().startswith("- \"0.0.0.0"):
                report.warn(f"Potential wide port binding detected: {line.strip()}")


def check_install_scripts(report: DeploymentReport) -> None:
    for script in INSTALL_SCRIPTS:
        if not script.exists():
            report.fail(f"Install script missing: {script}")
            continue
        mode = script.stat().st_mode
        if not (mode & stat.S_IXUSR):
            report.fail(f"Install script is not executable: {script}")


def check_requirements(report: DeploymentReport) -> None:
    packages: List[Set[str]] = []
    for req_file in REQUIREMENTS_FILES:
        if not req_file.exists():
            report.fail(f"Requirements file missing: {req_file}")
            continue
        entries = {
            line.strip().split("==")[0].split(">")[0]
            for line in req_file.read_text().splitlines()
            if line and not line.startswith("#")
        }
        packages.append(entries)

    if len(packages) == 2:
        prod_only = packages[1] - packages[0]
        overlap = packages[0] & packages[1]
        if not prod_only:
            report.warn("requirements-prod.txt does not introduce any packages beyond requirements.txt.")
        if "httpx" in overlap:
            report.warn("httpx is present in both requirement files; ensure version alignment is intentional.")


def main() -> int:
    report = DeploymentReport()
    check_docker_ports(report)
    check_install_scripts(report)
    check_requirements(report)

    if report.passed:
        print("✅ Deployment configuration check passed.")
    else:
        print("❌ Deployment configuration check failed.")
        for issue in report.issues:
            print(f"  - {issue}")

    if report.warnings:
        print("\nWarnings:")
        for warning in report.warnings:
            print(f"  - {warning}")

    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
