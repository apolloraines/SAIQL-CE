#!/usr/bin/env python3
"""
SAIQL Database Migrator
=======================

A tool to migrate data between databases using SAIQL.

Community Edition (CE) Supported Databases:
-------------------------------------------
SOURCE: SQLite, PostgreSQL, MySQL/MariaDB, CSV/Excel
TARGET: SQLite, PostgreSQL, MySQL/MariaDB

Enterprise Edition Additional Support:
--------------------------------------
SOURCE: +Oracle, SQL Server (MSSQL), DuckDB, MongoDB, SAP HANA, etc.
TARGET: +Oracle, SQL Server (MSSQL), DuckDB, etc.

Note: Code for Enterprise databases exists but requires Enterprise license.

Secured against SQL Injection and supports transactional checkpoints.

Usage:
    # Target specific database
    python3 tools/db_migrator.py --source "postgresql://user:pass@localhost/source_db" --target "postgresql://user:pass@localhost/target_db"

    # Target local directory (Legacy SQLite mode)
    python3 tools/db_migrator.py --source "postgresql://user:pass@localhost/source_db" --target-dir "/path/to/saiql/data"

Author: Apollo & Claude
Version: 1.3.0
"""

import argparse
import logging
import sys
import time
import json
import csv
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse

# Add parent directory to path to import SAIQL core
sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))

from core.database_manager import DatabaseManager, BackendType
# Note: Using stdlib logger to avoid module-level side effects from core.logging
from core.type_registry import TypeRegistry, IRType
from extensions.plugins.file_adapter import FileAdapter
from core.audit_generator import AuditBundleGenerator
import uuid
from datetime import datetime
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DBMigrator:
    """Database migration tool"""
    
    def __init__(self, source_url: str, target_url: Optional[str] = None, target_dir: Optional[str] = None, 
                 dry_run: bool = False, checkpoint_file: str = "migration_state.json", clean_on_failure: bool = False,
                 output_mode: str = 'db', output_dir: str = './migration_artifacts'):
        
        self.output_mode = output_mode
        self.output_dir = Path(output_dir)
        self.source_adapter = None
        self.run_id = None
        self.run_dir = None
        
        if self.output_mode in ('db', 'both') and not target_url and not target_dir:
            raise ValueError("Either --target or --target-dir must be specified when output-mode is 'db' or 'both'")

        self.source_url = source_url
        self.dry_run = dry_run
        self.checkpoint_file = checkpoint_file
        self.clean_on_failure = clean_on_failure
        
        self.parsed_source = urlparse(source_url)
        self.report = {
            "tables": [],
            "total_rows": 0,
            "estimated_time": "0s",
            "warnings": [],
            "post_migration_sql": []
        }
        self.created_tables = [] # Track for cleanup
        self.schema_map = {} # Store schema for preflight checks

        self.source_type = self.parsed_source.scheme.split('+')[0]
        self.state = self._load_state()
        
        # Determine target config
        self.target_config = {}
        if target_url:
            self.target_url = target_url
            parsed_target = urlparse(target_url)
            target_scheme = parsed_target.scheme.split('+')[0]
            
            if 'sqlite' in target_scheme:
                self.target_config = {
                    'type': 'sqlite',
                    'path': parsed_target.path.replace('//', '/')
                }
            elif 'postgres' in target_scheme:
                self.target_config = {
                    'type': 'postgresql',
                    'host': parsed_target.hostname,
                    'port': parsed_target.port or 5432,
                    'user': parsed_target.username,
                    'password': parsed_target.password,
                    'database': parsed_target.path.lstrip('/')
                }
            elif 'mysql' in target_scheme:
                self.target_config = {
                    'type': 'mysql',
                    'host': parsed_target.hostname,
                    'port': parsed_target.port or 3306,
                    'user': parsed_target.username,
                    'password': parsed_target.password,
                    'database': parsed_target.path.lstrip('/')
                }
            elif 'mssql' in target_scheme:
                 self.target_config = {
                     'type': 'mssql',
                     'host': parsed_target.hostname,
                     'port': parsed_target.port or 1433,
                     'user': parsed_target.username,
                     'password': parsed_target.password,
                     'database': parsed_target.path.lstrip('/')
                 }
            elif 'duckdb' in target_scheme:
                 self.target_config = {
                     'type': 'duckdb',
                     'path': parsed_target.path.replace('//', '/')
                 }
            else:
                 # Generic fallback
                 self.target_config = {'type': target_scheme, 'path': target_url}

        elif target_dir:
            # Legacy mode
            self.target_dir = Path(target_dir)
            self.target_db_path = self.target_dir / "saiql_store.db"
            self.target_config = {
                'type': 'sqlite',
                'path': str(self.target_db_path)
            }
            logger.info(f"Using legacy target directory: {target_dir}")
            
        elif self.output_mode == 'files':
            # Dummy target for type mapping context
            self.target_config = {
                'type': 'sqlite',
                'path': ':memory:'
            }
            logger.info("Output mode is FILES (no active target connection)")
            
        # Initialize DatabaseManager
        db_config = {
            'default_backend': 'target',
            'backends': {
                'target': self.target_config
            }
        }
        
        self.db_manager = DatabaseManager(config=db_config)
        logger.info(f"Initialized backend: target ({self.target_config['type']})")

    def _sanitize_error(self, e: Exception) -> str:
        """Mask credentials in error messages"""
        import re
        msg = str(e)
        # Mask postgres/mysql/etc connection strings: protocol://user:pass@host
        return re.sub(r'://([^:]+):([^@]+)@', r'://\1:***@', msg)


    def _load_state(self) -> Dict:
        """Load migration state from checkpoint file"""
        if Path(self.checkpoint_file).exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load checkpoint file: {e}. Starting fresh.")
        
        return {"completed_tables": [], "current_table": None, "current_offset": 0}

    def _save_state(self):
        """Save migration state to checkpoint file"""
        if self.dry_run: return
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def connect_source(self):
        """Connect to source database or file adapter"""
        logger.info(f"Connecting to source: {self.source_url}...")
        
        # Check if this is a filesystem path (not a database URL)
        # Database URLs have schemes like: sqlite://, postgresql://, mysql://, etc.
        is_db_url = self.source_type in ['sqlite', 'postgresql', 'postgres', 'mysql', 'oracle', 'mssql', 'duckdb']
        
        # Only check filesystem for non-database sources (files, directories, or file:// URLs)
        if not is_db_url:
            if self.source_url.startswith('file://') or Path(self.source_url).is_dir() or (Path(self.source_url).exists() and Path(self.source_url).suffix in ['.csv', '.xlsx', '.xls']):
                try:
                    self.source_adapter = FileAdapter(self.source_url)
                    self.source_adapter.connect()
                    logger.info("Connected to File Source Adapter")
                    return
                except Exception as e:
                    logger.error(f"Failed to connect to file source: {e}")
                    sys.exit(1)

        logger.info(f"Connecting to source database: {self.source_type}...")
    
        if 'postgres' in self.source_type:
            try:
                import psycopg2
                import psycopg2.extensions
                self.source_conn = psycopg2.connect(
                    host=self.parsed_source.hostname,
                    port=self.parsed_source.port or 5432,
                    database=self.parsed_source.path.lstrip('/'),
                    user=self.parsed_source.username,
                    password=self.parsed_source.password
                )
                # Consistent Snapshot
                self.source_conn.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE, readonly=True)
                logger.info("Connected to PostgreSQL source (SERIALIZABLE READ ONLY)")
            except ImportError:
                logger.error("psycopg2 not installed. Please install it: pip install psycopg2-binary")
                sys.exit(1)
            except Exception as e:
                logger.error(f"Failed to connect to source: {self._sanitize_error(e)}")
                sys.exit(1)
                
        elif 'mysql' in self.source_type:
            try:
                import mysql.connector
                self.source_conn = mysql.connector.connect(
                    host=self.parsed_source.hostname,
                    port=self.parsed_source.port or 3306,
                    database=self.parsed_source.path.lstrip('/'),
                    user=self.parsed_source.username,
                    password=self.parsed_source.password
                )
                cursor = self.source_conn.cursor()
                cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                logger.info("Connected to MySQL source (CONSISTENT SNAPSHOT)")
            except ImportError:
                logger.error("mysql-connector-python not installed. Please install it: pip install mysql-connector-python")
                sys.exit(1)
            except Exception as e:
                logger.error(f"Failed to connect to source: {self._sanitize_error(e)}")
                sys.exit(1)

        elif 'sqlite' in self.source_type:
            try:
                import sqlite3
                from pathlib import Path
                import os
                
                # Get path from URL
                db_path_str = self.parsed_source.path.replace('//', '/')
                
                # Windows fix: urlparse produces /C:/path for sqlite:///C:/path
                # Strip leading slash if it's followed by a drive letter
                if os.name == 'nt' and len(db_path_str) > 2 and db_path_str[0] == '/' and db_path_str[2] == ':':
                    db_path_str = db_path_str[1:]  # Remove leading /
                
                db_path = Path(db_path_str).resolve()
                self.source_conn = sqlite3.connect(str(db_path))
                logger.info("Connected to SQLite source")
            except Exception as e:
                logger.error(f"Failed to connect to source: {self._sanitize_error(e)}")
                sys.exit(1)

        elif 'oracle' in self.source_type:
            try:
                import oracledb
                # Enable LOB fetching as strings/bytes automatically
                def output_type_handler(cursor, name, default_type, size, precision, scale):
                    if default_type == oracledb.CLOB:
                        return cursor.var(oracledb.LONG_STRING, arraysize=cursor.arraysize)
                    if default_type == oracledb.BLOB:
                        return cursor.var(oracledb.LONG_BINARY, arraysize=cursor.arraysize)
                
                dsn = f"{self.parsed_source.hostname}:{self.parsed_source.port or 1521}/{self.parsed_source.path.lstrip('/')}"
                self.source_conn = oracledb.connect(
                    user=self.parsed_source.username,
                    password=self.parsed_source.password,
                    dsn=dsn
                )
                self.source_conn.outputtypehandler = output_type_handler
                
                cursor = self.source_conn.cursor()
                cursor.execute("SET TRANSACTION READ ONLY")
                logger.info("Connected to Oracle source (READ ONLY, Thin Mode)")
            except ImportError:
                logger.error("oracledb not installed. Please install it: pip install oracledb")
                sys.exit(1)
            except Exception as e:
                logger.error(f"Failed to connect to source: {self._sanitize_error(e)}")
                sys.exit(1)

        elif 'mssql' in self.source_type:
            try:
                import pymssql
                self.source_conn = pymssql.connect(
                    server=self.parsed_source.hostname,
                    port=self.parsed_source.port or 1433,
                    user=self.parsed_source.username,
                    password=self.parsed_source.password,
                    database=self.parsed_source.path.lstrip('/')
                )
                logger.info("Connected to MSSQL source")
            except ImportError:
                logger.error("pymssql not installed. Please install it: pip install pymssql")
                sys.exit(1)
            except Exception as e:
                logger.error(f"Failed to connect to source: {self._sanitize_error(e)}")
                sys.exit(1)

        elif 'duckdb' in self.source_type:
            try:
                import duckdb_engine
                from sqlalchemy import create_engine
                # For DuckDB, we traffic via SQLAlchemy engine to get the DBAPI connection
                # or just use duckdb.connect if path is local
                self.source_conn = create_engine(self.source_url).connect().connection
                logger.info("Connected to DuckDB source")
            except ImportError:
                 logger.error("duckdb-engine not installed. pip install duckdb-engine")
                 sys.exit(1)
            except Exception as e:
                 logger.error(f"Failed to connect to source: {self._sanitize_error(e)}")
                 sys.exit(1)

        else:
            logger.error(f"Unsupported database type: {self.source_type}")
            sys.exit(1)

    def _quote_impl(self, identifier: str, dialect: str) -> str:
        """Internal quoting implementation"""
        if 'mysql' in dialect.lower() or 'mariadb' in dialect.lower():
            return f'`{identifier}`'
        return f'"{identifier}"'

    def quote_source_ident(self, identifier: str) -> str:
        """Quote identifier for source database"""
        return self._quote_impl(identifier, self.source_type)

    def quote_target_ident(self, identifier: str) -> str:
        """Quote identifier for target database"""
        target_type = self.target_config.get('type', 'sqlite')
        return self._quote_impl(identifier, target_type)

    def quote_identifier(self, identifier: str) -> str:
        """Deprecated: Try to infer or default to generic"""
        # Kept for backward compat but should be replaced
        return f'"{identifier}"'

    def _get_row_count(self, table_name: str) -> int:
        """Get total row count for a table"""
        if self.source_adapter:
             # This is expensive for files, maybe skip or implement count in adapter
             return -1 
             
        try:
            cursor = self.source_conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.quote_source_ident(table_name)}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception:
            return 0

    def get_tables(self) -> List[str]:
        """Get list of tables from source"""
        if self.source_adapter:
            return self.source_adapter.get_tables()
            
        cursor = self.source_conn.cursor()
        tables = []
        
        if 'postgres' in self.source_type:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
        elif 'mysql' in self.source_type:
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
        
        elif 'sqlite' in self.source_type:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]

        elif 'oracle' in self.source_type:
            cursor.execute("""
                SELECT table_name
                FROM user_tables
                WHERE tablespace_name IS NOT NULL
                AND table_name NOT LIKE 'MVIEW%'
                AND table_name NOT LIKE 'LOGMNR%'
                AND table_name NOT LIKE 'AQ$%'
                AND table_name NOT LIKE 'DEF$%'
                AND table_name NOT LIKE 'REPCAT%'
                AND table_name NOT LIKE 'SQLPLUS%'
                AND table_name NOT LIKE 'HELP%'
            """)
            tables = [row[0].lower() for row in cursor.fetchall()]
            
        elif 'mssql' in self.source_type:
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_NAME NOT LIKE 'spt_%'
                AND TABLE_NAME NOT LIKE 'MS%'
            """)
            tables = [row[0] for row in cursor.fetchall()]

        elif 'duckdb' in self.source_type:
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            
        cursor.close()
        return tables

    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema for a table"""
        if self.source_adapter:
            return self.source_adapter.get_schema(table_name)
            
        cursor = self.source_conn.cursor()
        quoted_table = self.quote_identifier(table_name)
        schema = {
            'columns': [],
            'pk': [],
            'fks': [],
            'indexes': []
        }
        
        if 'postgres' in self.source_type:
            # Get columns
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            
            for row in cursor.fetchall():
                schema['columns'].append({
                    'name': row[0],
                    'type': self._map_type_to_saiql('postgres', row[1]),
                    'nullable': row[2] == 'YES'
                })
            
            # Get Primary Keys
            cursor.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_name = %s
                ORDER BY kcu.ordinal_position
            """, (table_name,))
            schema['pk'] = [row[0] for row in cursor.fetchall()]

            # Get Foreign Keys
            cursor.execute("""
                SELECT kcu.column_name, ccu.table_name AS foreign_table, ccu.column_name AS foreign_column
                FROM information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = %s
            """, (table_name,))
            for row in cursor.fetchall():
                schema['fks'].append({
                    'column': row[0],
                    'ref_table': row[1],
                    'ref_column': row[2]
                })

        elif 'mysql' in self.source_type:
            # Get columns
            cursor.execute(f"DESCRIBE {table_name}")
            for row in cursor.fetchall():
                schema['columns'].append({
                    'name': row[0],
                    'type': self._map_type_to_saiql('mysql', row[1]),
                    'nullable': row[2] == 'YES'
                })
                if row[3] == 'PRI':
                    schema['pk'].append(row[0])
            
            # Get FKs
            cursor.execute("""
                SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_NAME = %s AND REFERENCED_TABLE_NAME IS NOT NULL
            """, (table_name,))
            for row in cursor.fetchall():
                schema['fks'].append({
                    'column': row[0],
                    'ref_table': row[1],
                    'ref_column': row[2]
                })

        elif 'sqlite' in self.source_type:
            # Get columns using PRAGMA
            cursor.execute(f"PRAGMA table_info({self.quote_identifier(table_name)})")
            for row in cursor.fetchall():
                # cid, name, type, notnull, dflt_value, pk
                schema['columns'].append({
                    'name': row[1],
                    'type': self._map_type_to_saiql('sqlite', row[2]),
                    'nullable': row[3] == 0,
                    'default': row[4] # Capture default value
                })
                if row[5] == 1: # pk
                    schema['pk'].append(row[1])
            
            # Get FKs
            cursor.execute(f"PRAGMA foreign_key_list({self.quote_identifier(table_name)})")
            for row in cursor.fetchall():
                # id, seq, table, from, to, on_update, on_delete, match
                schema['fks'].append({
                    'column': row[3],
                    'ref_table': row[2],
                    'ref_column': row[4]
                })

        elif 'oracle' in self.source_type:
            # Get columns
            cursor.execute("""
                SELECT column_name, data_type, nullable, data_length, data_precision, data_scale
                FROM user_tab_columns
                WHERE table_name IN (:1, :2)
                ORDER BY column_id
            """, (table_name, table_name.upper()))
        
            for row in cursor.fetchall():
                col_name = row[0].lower()
                data_type = row[1]
                nullable = row[2] == 'Y'
        
                # Oracle NUMBER type handling
                if data_type == 'NUMBER':
                    precision, scale = row[4], row[5]
                    if precision and scale:
                        type_str = f"NUMBER({precision},{scale})"
                    elif precision:
                        type_str = f"NUMBER({precision})"
                    else:
                        type_str = "NUMBER"
                elif data_type in ('VARCHAR2', 'CHAR'):
                    length = row[3]
                    type_str = f"{data_type}({length})"
                else:
                    type_str = data_type
        
                schema['columns'].append({
                    'name': col_name,
                    'type': self._map_type_to_saiql('oracle', type_str),
                    'nullable': nullable
                })
        
            # Get Primary Keys
            cursor.execute("""
                SELECT cols.column_name
                FROM user_constraints cons
                JOIN user_cons_columns cols ON cons.constraint_name = cols.constraint_name
                WHERE cons.constraint_type = 'P'
                  AND cons.table_name IN (:1, :2)
                ORDER BY cols.position
            """, (table_name, table_name.upper()))
            schema['pk'] = [row[0].lower() for row in cursor.fetchall()]
        
            # Get Foreign Keys
            cursor.execute("""
                SELECT a.column_name, c_pk.table_name AS ref_table, b.column_name AS ref_column
                FROM user_cons_columns a
                JOIN user_constraints c ON a.constraint_name = c.constraint_name
                JOIN user_constraints c_pk ON c.r_constraint_name = c_pk.constraint_name
                JOIN user_cons_columns b ON c_pk.constraint_name = b.constraint_name
                WHERE c.constraint_type = 'R'
                  AND a.table_name IN (:1, :2)
            """, (table_name, table_name.upper()))
            for row in cursor.fetchall():
                schema['fks'].append({
                    'column': row[0].lower(),
                    'ref_table': row[1].lower(),
                    'ref_column': row[2].lower()
                })

        elif 'mssql' in self.source_type:
            # Get columns
            cursor.execute("""
                SELECT 
                    COLUMN_NAME, 
                    DATA_TYPE, 
                    IS_NULLABLE, 
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (table_name,))
            for row in cursor.fetchall():
                type_str = row[1]
                if type_str in ('varchar', 'char', 'nvarchar', 'nchar', 'varbinary', 'binary') and row[4]:
                     length = row[4]
                     if length == -1: type_str += "(MAX)"
                     else: type_str += f"({length})"
                
                schema['columns'].append({
                    'name': row[0],
                    'type': self._map_type_to_saiql('mssql', type_str),
                    'nullable': row[2] == 'YES',
                    'default': row[3]
                })

            # Get PKs
            cursor.execute("""
                SELECT KCU.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU
                    ON TC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME
                WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
                  AND TC.TABLE_NAME = %s
                ORDER BY KCU.ORDINAL_POSITION
            """, (table_name,))
            schema['pk'] = [row[0] for row in cursor.fetchall()]

            # Get FKs
            cursor.execute("""
                SELECT 
                    KCU1.COLUMN_NAME,
                    KCU2.TABLE_NAME,
                    KCU2.COLUMN_NAME
                FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1
                    ON RC.CONSTRAINT_NAME = KCU1.CONSTRAINT_NAME
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU2
                    ON RC.UNIQUE_CONSTRAINT_NAME = KCU2.CONSTRAINT_NAME
                WHERE KCU1.TABLE_NAME = %s
            """, (table_name,))
            for row in cursor.fetchall():
                schema['fks'].append({
                    'column': row[0],
                    'ref_table': row[1],
                    'ref_column': row[2]
                })
                
        cursor.close()
        return schema

    def _map_type_to_saiql(self, source_dialect: str, raw_type: str) -> str:
        """Map source type to SAIQL (target) type using TypeRegistry"""
        # 1. Source -> IR (TypeInfo key)
        type_info = TypeRegistry.map_to_ir(source_dialect, raw_type)
        
        # 2. IR -> Target
        target_dialect = self.target_config.get('type', 'sqlite')
        return TypeRegistry.map_from_ir(target_dialect, type_info)

    def _map_postgres_type(self, pg_type: str) -> str:
        return self._map_type_to_saiql('postgres', pg_type)

    def _map_mysql_type(self, mysql_type: str) -> str:
        return self._map_type_to_saiql('mysql', mysql_type)

    def create_saiql_table(self, table_name: str, schema: Dict[str, Any]):
        """Create table in SAIQL with constraints"""
        if table_name in self.state["completed_tables"]:
            return

        columns_sql = []
        
        # Columns
        for col in schema['columns']:
            # TODO: Handle target dialect quoting differences later
            # Ideally use a dialect emitter, but standard SQL works for most basics
            col_def = f"{self.quote_target_ident(col['name'])} {col['type']}"
            if not col['nullable']:
                col_def += " NOT NULL"
            columns_sql.append(col_def)
            
        # Primary Keys
        if schema['pk']:
            pk_cols = ", ".join([self.quote_target_ident(c) for c in schema['pk']])
            columns_sql.append(f"PRIMARY KEY ({pk_cols})")
            
        # Foreign Keys
        for fk in schema['fks']:
            columns_sql.append(
                f"FOREIGN KEY ({self.quote_target_ident(fk['column'])}) "
                f"REFERENCES {self.quote_target_ident(fk['ref_table'])}({self.quote_target_ident(fk['ref_column'])})"
            )
            
        if 'mssql' in self.target_config.get('type', 'sqlite'):
             # MSSQL: OBJECT_ID requires string literal, not identifier
             create_sql = f"""
                 IF OBJECT_ID('{table_name}', 'U') IS NULL
                 CREATE TABLE {self.quote_target_ident(table_name)} ({', '.join(columns_sql)})
             """
        else:
             create_sql = f"CREATE TABLE IF NOT EXISTS {self.quote_target_ident(table_name)} ({', '.join(columns_sql)})"
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would execute: {create_sql}")
            return

        logger.info(f"Creating table {table_name}...")
        result = self.db_manager.execute_query(create_sql, backend="target")
        
        if not result.success:
            logger.error(f"Failed to create table {table_name}: {result.error_message}")
            if self.clean_on_failure: self.cleanup()
            sys.exit(1)
            
        self.created_tables.append(table_name) # Track for cleanup

        # DEBUG: Verify table existence immediately
        try:
             verify_sql = f"SELECT table_name, table_schema FROM information_schema.tables WHERE table_name = '{table_name}'"
             verify_res = self.db_manager.execute_query(verify_sql, backend="target")
             logger.info(f"DEBUG: Verification query result for {table_name}: {verify_res.data}")
             
             # Debug current schema/db context
             target_type = self.target_config.get('type', 'sqlite')
             schema_sql = None
             
             if 'mssql' in target_type:
                 schema_sql = "SELECT SCHEMA_NAME() AS schema_name"
             elif 'mysql' in target_type:
                 schema_sql = "SELECT DATABASE() AS schema_name"
             elif 'postgres' in target_type:
                 schema_sql = "SELECT current_schema() AS schema_name"
                 
             if schema_sql:
                 try:
                     schema_res = self.db_manager.execute_query(schema_sql, backend="target")
                     logger.info(f"DEBUG: Current schema/db: {schema_res.data}")
                 except Exception as e:
                     logger.warning(f"DEBUG: Could not fetch schema context: {e}")

        except Exception as e:
             logger.error(f"DEBUG: Verification check failed: {e}")

    def _generate_insert_sql(self, table_name: str, schema: Dict[str, Any]) -> str:
        """Helper to generate the INSERT SQL statement."""
        target_type = self.target_config.get('type', 'sqlite')
        param_style = 'qmark' if 'sqlite' in target_type or 'duckdb' in target_type else 'format'
        placeholder = '?' if param_style == 'qmark' else '%s'
        
        placeholders = ', '.join([placeholder] * len(schema['columns']))
        return f"INSERT INTO {self.quote_target_ident(table_name)} VALUES ({placeholders})"

    def migrate_data(self, table_name: str, schema: Dict[str, Any]):
        """Migrate data for a table using transactional batching and checkpoints"""
        # Check if table is already completed
        if table_name in self.state["completed_tables"]:
            logger.info(f"Skipping {table_name} (already completed)")
            return

        logger.info(f"Migrating data for {table_name}...")
        self.state["current_table"] = table_name
        
        # Get total rows first
        total_rows = self._get_row_count(table_name)
        if total_rows == -1: # File adapter, count not readily available
            logger.info(f"Migrating rows for {table_name} (total count unknown for file source)")
        else:
            logger.info(f"Found {total_rows} rows to migrate")
        
        if total_rows == 0:
            self._mark_table_complete(table_name)
            return

        # Update report
        self.report["tables"].append({
            "name": table_name,
            "columns": len(schema['columns']),
            "rows": total_rows if total_rows != -1 else "N/A"
        })
        if total_rows != -1:
            self.report["total_rows"] += total_rows

        if self.dry_run:
            logger.info(f"[DRY RUN] Would migrate {total_rows if total_rows != -1 else 'unknown number of'} rows for {table_name}")
            return

        insert_sql = self._generate_insert_sql(table_name, schema)
        batch_size = 1000
        
        # File Adapter Path
        if self.source_adapter:
            migrated_count = 0
            cols = [c['name'] for c in schema['columns']]
            
            for batch in self.source_adapter.fetch_rows(table_name, batch_size):
                 # Convert dict rows to tuples matching schema order for positional binding
                 operations = [{'sql': insert_sql, 'params': [row.get(c) for c in cols]} for row in batch]
                 
                 result = self.db_manager.execute_transaction(operations, backend="target")
                 
                 if not result.success:
                     logger.error(f"Failed to insert batch for {table_name} at offset {migrated_count}: {result.error_message}")
                     if self.clean_on_failure: self.cleanup()
                     sys.exit(1)
                 
                 migrated_count += len(batch)
                 logger.info(f"  Migrated {migrated_count} rows for {table_name}...")
                 
                 # Update checkpoint
                 self.state["current_offset"] = migrated_count
                 self._save_state()
            
            self._mark_table_complete(table_name)
            return

        # DBAPI Path
        # Determine start offset from state
        start_offset = 0
        if self.state["current_table"] == table_name:
            start_offset = self.state["current_offset"]
            logger.info(f"Resuming {table_name} from offset {start_offset}")

        migrated_count = start_offset
        
        cursor = self.source_conn.cursor()
        cursor.execute(f"SELECT * FROM {self.quote_source_ident(table_name)}")
        
        # Skip to offset if needed
        # Note: This is inefficient for large offsets but generic.
        # Optimizations (LIMIT/OFFSET) are dialect-specific.
        if start_offset > 0:
            logger.info(f"Skipping {start_offset} rows...")
            skipped = 0
            while skipped < start_offset:
                # Skip in chunks
                to_skip = min(10000, start_offset - skipped)
                cursor.fetchmany(to_skip)
                skipped += to_skip
        
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
                
            operations = []
            
            # Prepare batch operations with params
            for row in batch:
                operations.append({
                    'sql': insert_sql,
                    'params': row
                })
            
            # Execute batch in transaction
            result = self.db_manager.execute_transaction(operations, backend="target")
            
            if not result.success:
                logger.error(f"Failed to insert batch at offset {migrated_count}: {result.error_message}")
                if self.clean_on_failure: self.cleanup()
                sys.exit(1)
            else:
                migrated_count += len(batch)
                logger.info(f"Migrated rows {migrated_count - len(batch) + 1} to {migrated_count}")
                
                # Update checkpoint
                self.state["current_table"] = table_name
                self.state["current_offset"] = migrated_count
                self._save_state()

        # Mark table as complete
        self._mark_table_complete(table_name)

    def _mark_table_complete(self, table_name: str):
        if table_name not in self.state["completed_tables"]:
            self.state["completed_tables"].append(table_name)
        self.state["current_table"] = None
        self.state["current_offset"] = 0
        self._save_state()

    def cleanup(self):
        """Cleanup created tables on failure"""
        logger.warning(f"Cleaning up {len(self.created_tables)} tables due to failure...")
        for table in reversed(self.created_tables):
            try:
                self.db_manager.execute_query(f"DROP TABLE IF EXISTS {self.quote_identifier(table)}", backend="target")
                logger.info(f"Dropped table {table}")
            except Exception as e:
                logger.error(f"Failed to drop table {table}: {e}")

    def preflight_check(self, tables: List[str]):
        """Run pre-flight validation checks"""
        logger.info("Running pre-flight checks...")
        
        # 0. Lossy Type Conversion Check
        if self.schema_map:
             for table, info in self.schema_map.items():
                 for col in info['columns']:
                     raw_type = col['type']
                     # Determine source dialect key for registry
                     src = 'postgres' if 'postgres' in self.source_type else \
                           'mysql' if 'mysql' in self.source_type else \
                           'oracle' if 'oracle' in self.source_type else \
                           'sqlite' if 'sqlite' in self.source_type else \
                           'mssql' if 'mssql' in self.source_type else 'unknown'
                           
                     tgt = self.target_config['type']
                     
                     is_lossy, reason = TypeRegistry.is_lossy_conversion(src, raw_type, tgt)
                     if is_lossy:
                         msg = f"LOSSY TYPE in {table}.{col['name']}: {reason}"
                         if msg not in self.report['warnings']:
                             self.report['warnings'].append(msg)
                             logger.warning(msg)
                             
                     # Check for Defaults (Policy: Defer)
                     if col.get('default') is not None:
                         msg = f"DEFERRED: Default value '{col['default']}' for {table}.{col['name']} will be dropped (L1 Migration does not support defaults)."
                         if msg not in self.report['warnings']:
                             self.report['warnings'].append(msg)
                             logger.warning(msg)

        # 1. Circular FK Checks
        dependencies = {}
        for table in tables:
            schema = self.get_schema(table)
            dependencies[table] = set()
            for fk in schema['fks']:
                ref_table = fk['ref_table']
                if ref_table == table:
                     msg = f"Self-referential FK detected: {table}.{fk['column']} -> {ref_table}.{fk['ref_column']}. Data insert order matters."
                     logger.warning(msg)
                     self.report["warnings"].append(msg)

                if ref_table in tables and ref_table != table:
                    dependencies[table].add(ref_table)
        
        # Detect cycles
        for table in tables:
            visited = set()
            path = set()
            def dfs(node):
                visited.add(node)
                path.add(node)
                for neighbor in dependencies.get(node, []):
                    if neighbor in path:
                        msg = f"Circular dependency detected: {node} -> {neighbor}"
                        logger.warning(msg)
                        self.report["warnings"].append(msg)
                    elif neighbor not in visited:
                        dfs(neighbor)
                path.remove(node)
            
            dfs(table)

        # 2. Check for Reserved Words & Collisions
        reserved_words = {'user', 'table', 'select', 'where', 'from', 'order', 'group', 'limit', 'offset', 'index', 'create', 'update', 'delete', 'insert'}
        
        for table in tables:
            if table.lower() in reserved_words:
                 msg = f"Table name '{table}' is a reserved word. Quoting will handle it, but it is risky."
                 logger.warning(msg)
                 self.report["warnings"].append(msg)

        # 3. Check for Identifier Collisions (Case Sensitivity)
        seen_identifiers = {} # lower -> original
        for table in tables:
             lower_name = table.lower()
             if lower_name in seen_identifiers:
                 original = seen_identifiers[lower_name]
                 msg = f"Identifier collision detected: '{table}' vs '{original}'. These will collide on case-insensitive targets."
                 logger.warning(msg)
                 self.report["warnings"].append(msg)
             else:
                 seen_identifiers[lower_name] = table

    def generate_sequence_reset_sql(self, table_name: str, schema: Dict[str, Any]):
        """Generate Post-Migration SQL to reset sequences"""
        if not schema['pk']: return
        
        target_type = self.target_config.get('type', 'sqlite')
        pk_col = schema['pk'][0] # Assume simple PK for MVP
        
        # Only relevant for Integer PKs (heuristic)
        col_type = next((c['type'] for c in schema['columns'] if c['name'] == pk_col), 'TEXT')
        if 'INT' not in col_type.upper():
            return

        sql = ""
        qt = self.quote_identifier(table_name)
        qc = self.quote_identifier(pk_col)

        if 'postgres' in target_type:
             seq_name = f"{table_name}_{pk_col}_seq"
             sql = f"SELECT setval('{seq_name}', (SELECT COALESCE(MAX({qc}), 0) + 1 FROM {qt}), false);"
        
        elif 'mysql' in target_type:
            sql = f"SELECT @m := COALESCE(MAX({qc}), 0) + 1 FROM {qt}; SET @s = CONCAT('ALTER TABLE {qt} AUTO_INCREMENT=', @m); PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;"

        elif 'sqlite' in target_type:
             sql = f"-- SQLite handles AUTOINCREMENT automatically if declared. If manual: UPDATE sqlite_sequence SET seq = (SELECT MAX({qc}) FROM {qt}) WHERE name='{table_name}';"

        if sql:
            self.report["post_migration_sql"].append(f"-- {table_name}: {sql}")

    def _write_ddl_file(self, table_name: str, schema: Dict[str, Any]):
        """Write CREATE TABLE DDL to a file"""
        columns_sql = []
        for col in schema['columns']:
            # Minimal mapping for standard SQL
            col_def = f"{col['name']} {col['type']}"
            if not col['nullable']:
                col_def += " NOT NULL"
            columns_sql.append(col_def)
            
        if schema['pk']:
            pk_cols = ", ".join(schema['pk'])
            columns_sql.append(f"PRIMARY KEY ({pk_cols})")
            
        for fk in schema['fks']:
            columns_sql.append(
                f"FOREIGN KEY ({fk['column']}) REFERENCES {fk['ref_table']}({fk['ref_column']})"
            )
            
        create_sql = f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(columns_sql) + "\n);\n\n"
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        with open(self.output_dir / "schema.sql", "a") as f:
            f.write(create_sql)
            
    def _write_csv_file(self, table_name: str, schema: Dict[str, Any]):
        """Export table data to CSV"""
        data_dir = self.output_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        csv_path = data_dir / f"{table_name}.csv"
        
        logger.info(f"Exporting {table_name} to {csv_path}...")
        
        with open(csv_path, "w", newline='') as f:
            writer = csv.writer(f)
            # Write header
            cols = [c['name'] for c in schema['columns']]
            writer.writerow(cols)
            
            if self.source_adapter:
                # File Source
                for batch in self.source_adapter.fetch_rows(table_name, 1000):
                     # Ensure column order matches header
                     rows = [[row.get(c, None) for c in cols] for row in batch]
                     writer.writerows(rows)
            else:
                # DBAPI Source
                cursor = self.source_conn.cursor()
                quoted_table = self.quote_source_ident(table_name)
                
                query = f"SELECT * FROM {quoted_table}"
                
                # Enforce determinism
                if schema['pk']:
                     pk_cols = ", ".join([self.quote_source_ident(pk) for pk in schema['pk']])
                     query += f" ORDER BY {pk_cols}"
                     
                cursor.execute(query)
                
                while True:
                    rows = cursor.fetchmany(1000)
                    if not rows:
                        break
                    writer.writerows(rows)
                
        self.report["tables"].append({
            "name": table_name,
            "columns": len(schema['columns']),
            "rows": self._get_row_count(table_name),
            "output": str(csv_path)
        })

    def resume_run(self, run_Identifier: str):
        """Resume an existing run"""
        # Try finding the run folder
        potential_path = Path(run_Identifier)
        if potential_path.exists() and potential_path.is_dir():
            self.run_dir = potential_path
        else:
             # Look in default runs location
             runs_dir = self.output_dir / "runs"
             found = list(runs_dir.glob(f"{run_Identifier}*"))
             if not found:
                 raise ValueError(f"Run {run_Identifier} not found in {runs_dir}")
             self.run_dir = found[0]
             
        self.run_id = self.run_dir.name
        
        # Determine output dir from run dir
        self.output_dir = self.run_dir / "output"
        self.checkpoint_file = str(self.run_dir / "checkpoint.json")
        
        # Load state
        self._load_state()
        
        logger.info(f"RESUMING Run: {self.run_id}")
        logger.info(f"Output Directory: {self.output_dir}")
        
        # Logging setup similar to run()
        log_file = self.run_dir / "logs" / "migration.log"
        file_handler = logging.FileHandler(log_file, mode='a')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(file_handler)
        
        self.connect_source()
        
        try:
             tables = self.get_tables()
             # ... continue with migration logic from run() ...
             # Code reuse issue: run() has logic we need. 
             # Refactoring run() to separate initialization from execution.
             self._execute_run(tables)
             
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise e
        finally:
            if hasattr(self, 'source_conn') and self.source_conn:
                self.source_conn.close()
            if self.source_adapter:
                self.source_adapter.close()

    def _execute_run(self, tables):
        """Shared execution logic"""
        logger.info(f"Found tables: {', '.join(tables)}")
        
        # Populate schema map for preflight
        for table in tables:
            self.schema_map[table] = self.get_schema(table)

        # Preflight - skip on resume? No, safer to re-run checks
        self.preflight_check(tables)

        for table in tables:
            schema = self.schema_map[table]
            
            # Mode: FILES or BOTH
            if self.output_mode in ('files', 'both'):
                self._write_ddl_file(table, schema)
                self._write_csv_file(table, schema)
            
            # Mode: DB or BOTH
            if self.output_mode in ('db', 'both'):
                self.create_saiql_table(table, schema)
                self.migrate_data(table, schema)
                self.generate_sequence_reset_sql(table, schema)
                
            # Log Success - get actual row count from report
            if hasattr(self, 'audit_generator'):
                table_info = next((t for t in self.report["tables"] if t["name"] == table), None)
                row_count = table_info["rows"] if table_info and table_info["rows"] != "N/A" else 0
                self.audit_generator.log_conversion(table, rows=row_count, status="Success")

        # Generate Audit Report
        if hasattr(self, 'audit_generator'):
            report_path = self.audit_generator.generate_report()
            logger.info(f"Audit Report generated: {report_path}")
            
        if self.dry_run:
           self.print_report()
        else:
           logger.info("Migration completed successfully! ✨")

    def run(self):
        """Run the migration"""
        # 1. Initialize Run Context
        self.run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        self.run_dir = self.output_dir / "runs" / self.run_id
        
        # Structure: output/runs/run_ID/{input,output,reports,logs}
        (self.run_dir / "input").mkdir(parents=True, exist_ok=True)
        (self.run_dir / "output").mkdir(parents=True, exist_ok=True)
        (self.run_dir / "reports").mkdir(parents=True, exist_ok=True)
        (self.run_dir / "logs").mkdir(parents=True, exist_ok=True)
        
        # Redirect output_dir to run output for artifacts (files mode)
        # We assume files mode writes to self.output_dir
        # We need to preserve the user's intention but encapsulate it.
        # If user passed --output-dir, we use that as the BASE for runs.
        # self.output_dir is currently base. 
        # But _write_csv_file uses self.output_dir. Let's redirect it.
        base_output_dir = self.output_dir
        self.output_dir = self.run_dir / "output"
        
        # Configure File Logging to run folder
        log_file = self.run_dir / "logs" / "migration.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(file_handler)
        logger.info(f"Starting Run: {self.run_id}")
        logger.info(f"Run Directory: {self.run_dir}")

        # Redirect Checkpoint
        self.checkpoint_file = str(self.run_dir / "checkpoint.json")
        
        # Write Manifest
        manifest = {
            "run_id": self.run_id,
            "timestamp": datetime.now().isoformat(),
            "source": self.source_url,
            "target": self.target_config.get('path', 'remote') if hasattr(self, 'target_config') else 'unknown',
            "output_mode": self.output_mode,
            "dry_run": self.dry_run
        }
        with open(self.run_dir / "run_manifest.json", "w") as f:
            json.dump(manifest, f, indent=2)

        # Initialize Audit Generator
        self.audit_generator = AuditBundleGenerator(
            run_dir=self.run_dir,
            source_url=self.source_url,
            target_url=self.target_config.get('path', 'unknown') if hasattr(self, 'target_config') else 'unknown'
        )

        self.connect_source()
        
        try:
            tables = self.get_tables()
            self._execute_run(tables)
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            if self.clean_on_failure: self.cleanup()
            raise e
        finally:
            if hasattr(self, 'source_conn') and self.source_conn:
                self.source_conn.close()
            if self.source_adapter:
                self.source_adapter.close()

    def print_report(self):
        """Print dry run report with capability levels"""
        print("\n" + "="*70)
        print("MIGRATION DRY RUN REPORT")
        print("="*70)
        print(f"Source: {self.source_url} ({self.source_type})")
        print(f"Target Config: {self.target_config.get('type')} ({self.target_config.get('path', 'remote')})")
        print("-" * 70)

        # Capability Level Assessment
        print("\nCAPABILITY LEVEL: L1 (Tables + PK/FK + Indexes + Data)")
        print("  [✓] Tables and columns")
        print("  [✓] Primary keys")
        print("  [✓] Foreign keys")
        print("  [✓] Data migration")
        print("  [✗] Views (L2 - not implemented)")
        print("  [✗] Stored procedures (L3 - not implemented)")
        print("  [✗] Triggers (L4 - not implemented)")

        print("\n" + "-" * 70)
        print("SCHEMA ANALYSIS:")
        print("-" * 70)

        # Conversion summary
        converted = len(self.report["tables"])
        skipped = 0  # Track views, procs, etc in future
        warnings = len(self.report.get("warnings", []))

        print(f"  Converted: {converted} tables")
        print(f"  Skipped: {skipped} objects")
        print(f"  Warnings: {warnings}")

        print("\n" + "-" * 70)
        print("TABLES TO MIGRATE:")
        print("-" * 70)

        for t in self.report["tables"]:
            print(f"  ✓ {t['name']}")
            print(f"      Columns: {t['columns']}")
            print(f"      Rows: {t['rows']:,}")

            if 'type_warnings' in t:
                for warning in t['type_warnings']:
                    print(f"      ⚠️  {warning}")

        if self.report.get("warnings"):
            print("\n" + "-" * 70)
            print("WARNINGS:")
            print("-" * 70)
            for warning in self.report["warnings"]:
                print(f"  ⚠️  {warning}")

        if self.report.get("post_migration_sql"):
            print("\n" + "-" * 70)
            print("POST-MIGRATION ACTIONS (Sequence Resets):")
            print("-" * 70)
            for sql in self.report["post_migration_sql"]:
                print(sql)



        print("\n" + "-" * 70)
        print("SUMMARY:")
        print("-" * 70)
        print(f"  Total Tables: {converted}")
        print(f"  Total Rows: {self.report['total_rows']:,}")
        print(f"  Capability Level: L1 (Schema + Data)")
        print("="*70 + "\n")


def main():
    parser = argparse.ArgumentParser(description="SAIQL Database Migrator")
    parser.add_argument("--source", required=True, help="Source database URL (e.g., postgresql://user:pass@localhost/db)")
    parser.add_argument("--target", help="Target database URL (e.g., postgresql://user:pass@localhost/db)")
    parser.add_argument("--target-dir", help="[Legacy] Target directory for local SAIQL SQLite store")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run and output a report")
    parser.add_argument("--checkpoint-file", default="migration_state.json", help="File to store/resume checkpoint state")
    parser.add_argument("--output-mode", choices=['db', 'files', 'both'], default='db', help="Output mode: db (live), files (csv/sql), or both")
    parser.add_argument("--output-dir", default="./migration_artifacts", help="Directory for output files")
    parser.add_argument("--clean-on-failure", action="store_true", help="Drop created tables if migration fails")
    
    parser.add_argument("--resume-run", help="Resume a specific run ID or path")
    
    args = parser.parse_args()
    
    if args.output_mode in ('db', 'both') and not args.target and not args.target_dir:
        parser.error("Either --target or --target-dir must be specified when output-mode is 'db' or 'both'")
        
    if args.target_dir:
        Path(args.target_dir).mkdir(parents=True, exist_ok=True)
    
    try:
        migrator = DBMigrator(args.source, target_url=args.target, target_dir=args.target_dir, 
                             dry_run=args.dry_run, checkpoint_file=args.checkpoint_file, clean_on_failure=args.clean_on_failure,
                             output_mode=args.output_mode, output_dir=args.output_dir)
                             
        if args.resume_run:
            migrator.resume_run(args.resume_run)
        else:
            migrator.run()
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
