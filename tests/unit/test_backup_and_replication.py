import tempfile
import shutil
from pathlib import Path

from scripts import backup_restore
from core.cluster.replication_manager import ReplicationManager


def test_backup_create_and_restore(tmp_path: Path, monkeypatch):
    repo_root = tmp_path / "repo"
    data_dir = repo_root / "data"
    data_dir.mkdir(parents=True)
    (data_dir / "sample.txt").write_text("hello")
    (repo_root / "backups").mkdir()

    monkeypatch.setattr(backup_restore, "ROOT", repo_root)
    monkeypatch.setattr(backup_restore, "DATA_DIR", data_dir)
    monkeypatch.setattr(backup_restore, "BACKUP_DIR", repo_root / "backups")

    dest = backup_restore.create_backup("test")
    assert dest.exists()
    (data_dir / "sample.txt").write_text("modified")
    backup_restore.restore_backup(dest)
    assert (data_dir / "sample.txt").read_text() == "hello"


def test_replication_manager_marks_failures():
    events = []

    def ok(event):
        events.append(event)

    def fail(event):
        raise RuntimeError("boom")

    manager = ReplicationManager(heartbeat_interval=0.1)
    manager.register_replica("ok", ok)
    manager.register_replica("fail", fail)
    failures = manager.replicate({"op": "insert"})
    assert "fail" in failures
    assert events == [{"op": "insert"}]
