import pytest
import os
import shutil
import json
from pathlib import Path
from tools.db_migrator import DBMigrator

# Test path
TEST_PKG_DIR = Path("./test_phase05_packaging")

@pytest.fixture
def clean_pkg_artifacts():
    if TEST_PKG_DIR.exists():
        shutil.rmtree(TEST_PKG_DIR)
    yield
    if TEST_PKG_DIR.exists():
        shutil.rmtree(TEST_PKG_DIR)

class TestJobPackaging:
    
    def test_run_folder_creation(self, clean_pkg_artifacts):
        """Verify run folder is created with correct structure"""
        # Create Dummy Source
        source_dir = TEST_PKG_DIR / "source"
        source_dir.mkdir(parents=True)
        (source_dir / "t1.csv").write_text("id,val\n1,a")
        
        migrator = DBMigrator(
            source_url=f"file://{source_dir.absolute()}",
            output_mode="files",
            output_dir=str(TEST_PKG_DIR)
        )
        migrator.run()
        
        # Verify "runs" directory exists
        runs_dir = TEST_PKG_DIR / "runs"
        assert runs_dir.exists()
        
        # Verify run_ID folder exists
        run_folders = list(runs_dir.glob("run_*"))
        assert len(run_folders) == 1
        run_dir = run_folders[0]
        
        # Check subfolders
        assert (run_dir / "input").exists()
        assert (run_dir / "output").exists()
        assert (run_dir / "logs").exists()
        assert (run_dir / "reports").exists()
        
        # Check Manifest
        manifest_path = run_dir / "run_manifest.json"
        assert manifest_path.exists()
        with open(manifest_path) as f:
            manifest = json.load(f)
            assert "run_id" in manifest
            assert manifest["output_mode"] == "files"
            
        # Check Logs
        assert (run_dir / "logs" / "migration.log").exists()
        
        # Check Artifacts Redirected
        assert (run_dir / "output" / "data" / "t1.csv").exists()
        # Ensure artifacts NOT in base dir
        assert not (TEST_PKG_DIR / "data").exists()

    def test_resume_run(self, clean_pkg_artifacts):
        """Verify resume functionality"""
        # Create Dummy Source
        source_dir = TEST_PKG_DIR / "res_source"
        source_dir.mkdir(parents=True)
        (source_dir / "t1.csv").write_text("id,val\n1,a")
        
        # 1. Initial Run
        migrator = DBMigrator(
            source_url=f"file://{source_dir.absolute()}",
            output_mode="files",
            output_dir=str(TEST_PKG_DIR)
        )
        migrator.run()
        run_id = migrator.run_id
        checkpoint_file = migrator.checkpoint_file
        
        # Modify checkpoint to simulate partial progress
        with open(checkpoint_file, "w") as f:
            json.dump({"t1": "in_progress"}, f)
            
        # 2. Resume Run
        migrator_2 = DBMigrator(
            source_url=f"file://{source_dir.absolute()}", # Source irrelevant on resume logic if cached, but required by init
            output_mode="files",
            output_dir=str(TEST_PKG_DIR)
        )
        migrator_2.resume_run(run_id)
        
        # Check run_id preserved
        assert migrator_2.run_id == run_id
        # Check output dir preserved
        assert str(migrator_2.output_dir).endswith("output")
        assert run_id in str(migrator_2.output_dir)

    def test_audit_report_generation(self, clean_pkg_artifacts):
        """Verify audit report is generated"""
        # Create Dummy Source
        source_dir = TEST_PKG_DIR / "audit_source"
        source_dir.mkdir(parents=True)
        (source_dir / "t2.csv").write_text("id,val\n1,x")
        
        migrator = DBMigrator(
            source_url=f"file://{source_dir.absolute()}",
            output_mode="files",
            output_dir=str(TEST_PKG_DIR)
        )
        migrator.run()
        
        # Check report exists
        run_dir = Path(migrator.run_dir)
        report_path = run_dir / "reports" / "audit_report.md"
        assert report_path.exists()
        
        content = report_path.read_text()
        assert "# Migration Audit Report" in content
        assert "## Summary" in content
        assert "## Objects Converted" in content
        # t2 dummy table should be successfully "converted" (dumped)
        assert "t2" in content
        assert "✅ Success" in content


