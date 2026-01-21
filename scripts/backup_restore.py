#!/usr/bin/env python3
"""SAIQL Echo backup/restore utility."""

import argparse
import shutil
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
BACKUP_DIR = ROOT / "backups"


def create_backup(tag: str | None = None) -> Path:
    BACKUP_DIR.mkdir(exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    name = f"backup_{stamp}"
    if tag:
        name += f"_{tag}"
    dest = BACKUP_DIR / name
    shutil.copytree(DATA_DIR, dest)
    return dest


def restore_backup(src: Path) -> None:
    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
    shutil.copytree(src, DATA_DIR)


def list_backups() -> list[Path]:
    if not BACKUP_DIR.exists():
        return []
    return sorted([p for p in BACKUP_DIR.iterdir() if p.is_dir()])


def main() -> int:
    parser = argparse.ArgumentParser(description="SAIQL Echo backup/restore manager")
    sub = parser.add_subparsers(dest="command")

    create = sub.add_parser("create", help="Create a new backup")
    create.add_argument("--tag", help="Optional tag for backup")

    restore = sub.add_parser("restore", help="Restore from a backup")
    restore.add_argument("path", help="Path or name of backup directory")

    sub.add_parser("list", help="List available backups")

    args = parser.parse_args()

    if args.command == "create":
        dest = create_backup(args.tag)
        print(dest)
    elif args.command == "restore":
        candidate = Path(args.path)
        if not candidate.is_dir():
            candidate = BACKUP_DIR / args.path
        if not candidate.is_dir():
            raise SystemExit(f"Backup not found: {args.path}")
        restore_backup(candidate)
        print(f"Restored from {candidate}")
    elif args.command == "list":
        for backup in list_backups():
            print(backup)
    else:
        parser.print_help()
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
