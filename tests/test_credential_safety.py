
import unittest
from unittest.mock import MagicMock, patch
import logging
from tools.db_migrator import DBMigrator
import io

class TestCredentialSafety(unittest.TestCase):
    
    def test_log_sanitization_on_error(self):
        secret = "SUPER_SECRET_PASSWORD"
        dangerous_url = f"postgresql://user:{secret}@localhost/db"
        
        # Patch psycopg2.connect directly since it is installed
        with patch('psycopg2.connect') as mock_connect:
            # Set side effect on connect
            mock_connect.side_effect = Exception(f"Unable to connect to {dangerous_url}")
            
            # Use file db for target to avoid init error
            import os
            cwd = os.getcwd()
            db_path = f"{cwd}/test_safety.db"
            if os.path.exists("test_safety.db"): os.remove("test_safety.db")

            # Initialize migrator
            migrator = DBMigrator(dangerous_url, target_url=f"sqlite:///{db_path}", dry_run=True)
            
            with self.assertLogs('tools.db_migrator', level='ERROR') as cm:
                try:
                    migrator.connect_source()
                except:
                    pass 
            
            # Clean up
            if os.path.exists("test_safety.db"): os.remove("test_safety.db")
            
            # Check logs
            found_secret = False
            print(f"DEBUG LOGS: {cm.output}")
            for log_record in cm.output:
                if secret in log_record:
                    found_secret = True
                    break
            
            self.assertFalse(found_secret, "Credentials leaked in error logs!")

if __name__ == '__main__':
    unittest.main()
