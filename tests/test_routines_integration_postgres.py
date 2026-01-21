import pytest
import subprocess
import time
import os
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from core.schema_ir import RoutineIR, RoutineCapability, RoutineArgumentIR
from core.routine_translator import RoutineTranslator

# Check if docker is available
def is_docker_available():
    try:
        subprocess.run(["docker", "--version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except:
        return False

@pytest.mark.skipif(not is_docker_available(), reason="Docker not available")
class TestRoutineIntegrationPostgres:
    
    @classmethod
    def setup_class(cls):
        # 1. Bring up Postgres
        cwd = os.path.join(os.getcwd(), "tests/migration_matrix")
        print(f"Starting Postgres container from {cwd}...")
        subprocess.run(
            ["docker", "compose", "up", "-d", "target_postgres"], 
            cwd=cwd, check=True
        )
        # Give it a moment to initialize
        time.sleep(10)
        
    @classmethod
    def teardown_class(cls):
        # Optional: Keep it running for debug, or tear down
        # subprocess.run(["docker", "compose", "down"], cwd="tests/migration_matrix", check=True)
        pass

    def test_postgres_compilation(self):
        """
        1. Generate SQL for a Stub and a Translated routine.
        2. Apply to Postgres container.
        3. Verify success.
        """
        translator = RoutineTranslator("oracle", "postgres")
        
        # Case A: Stub
        ir_stub = RoutineIR(
            name="stubbed_proc", 
            arguments=[RoutineArgumentIR("p_id", "NUMBER")], 
            return_type="NUMBER",
            body_source="BEGIN ... complex ... END;"
        )
        res_stub = translator.process(ir_stub, RoutineCapability.STUB)
        
        # Case B: Translated
        ir_trans = RoutineIR(
            name="translated_func",
            arguments=[RoutineArgumentIR("p_val", "VARCHAR2")],
            return_type="VARCHAR2",
            body_source="BEGIN RETURN NVL(p_val, 'default'); END;"
        )
        res_trans = translator.process(ir_trans, RoutineCapability.SUBSET_TRANSLATE)
        
        # Combine SQL
        sql_content = f"{res_stub.generated_code}\n\n{res_trans.generated_code}"
        
        # Write to temp file
        with open("temp_routines.sql", "w") as f:
            f.write(sql_content)
            
        # Copy to container
        # Container name typically: migration_matrix-target_postgres-1 or similar
        # We can get it via docker compose ps -q target_postgres
        cwd = os.path.join(os.getcwd(), "tests/migration_matrix")
        
        # Get container ID
        cid = subprocess.check_output(
            ["docker", "compose", "ps", "-q", "target_postgres"], 
            cwd=cwd
        ).decode().strip()
        
        subprocess.run(["docker", "cp", "temp_routines.sql", f"{cid}:/tmp/routines.sql"], check=True)
        
        # Execute via psql
        # target_user / target_db from docker-compose.yml
        cmd = [
            "docker", "exec", cid, 
            "psql", "-U", "target_user", "-d", "target_db", 
            "-v", "ON_ERROR_STOP=1", 
            "-f", "/tmp/routines.sql"
        ]
        
        print("Running psql compilation check...")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        
        assert result.returncode == 0, "Postgres compilation failed"
        assert "CREATE FUNCTION" in result.stdout

if __name__ == "__main__":
    t = TestRoutineIntegrationPostgres()
    if is_docker_available():
        t.setup_class()
        try:
            t.test_postgres_compilation()
            print("Integration test PASSED")
        finally:
            t.teardown_class()
    else:
        print("Skipping: Docker not available")
