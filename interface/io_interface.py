#!/usr/bin/env python3
"""
SAIQL I/O Interface - Enhanced File and Data Operations

This module provides comprehensive I/O capabilities for SAIQL, including:
- Reading and writing .saiql script files
- Batch processing of query files
- Data import/export in multiple formats (CSV, JSON, Excel, XML)
- Integration with external data sources
- Streaming operations for large datasets
- Configuration file management
- Backup and versioning systems

The I/O interface makes SAIQL practical for real-world applications by providing
enterprise-grade data handling capabilities while maintaining the simplicity
of the symbolic query language.

Author: Apollo & Claude
Version: 1.0.0
Status: Foundation Phase

Usage:
    io_interface = SAIQLIOInterface()
    results = io_interface.execute_script("queries.saiql")
    io_interface.export_data(results, "output.csv", format="csv")
"""

import logging
import os
import json
import csv
import time
from typing import Dict, List, Any, Optional, Union, Iterator, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
import hashlib
import shutil
from datetime import datetime
import tempfile
import zipfile
import gzip
import threading
from contextlib import contextmanager
import re

# Import SAIQL components
try:
    from core.engine import SAIQLEngine, QueryResult, ExecutionContext, ExecutionMode
    from utils.helpers import (read_file, write_file, read_json_file, write_json_file,
                               validate_file_path, format_query_result, measure_time)
except ImportError as e:
    print(f"Warning: Could not import SAIQL components: {e}")
    # Define fallbacks to prevent NameError - interface will fail gracefully at runtime
    SAIQLEngine = None
    QueryResult = None
    ExecutionContext = None
    ExecutionMode = None
    read_file = None
    write_file = None
    read_json_file = None
    write_json_file = None
    validate_file_path = None
    format_query_result = None
    measure_time = None

# Configure logging
logger = logging.getLogger(__name__)


def _escape_sql_value(value: Any) -> str:
    """Escape a value for safe SQL interpolation."""
    if value is None:
        return 'NULL'
    s = str(value)
    # Escape single quotes by doubling them
    return s.replace("'", "''")

class DataFormat(Enum):
    """Supported data formats for import/export"""
    CSV = "csv"
    JSON = "json"
    JSONL = "jsonl"  # JSON Lines
    XML = "xml"
    EXCEL = "xlsx"
    TSV = "tsv"
    PARQUET = "parquet"
    SAIQL = "saiql"
    SQL = "sql"

class CompressionType(Enum):
    """Supported compression types"""
    NONE = "none"
    GZIP = "gzip"
    ZIP = "zip"
    BZIP2 = "bz2"

class StreamingMode(Enum):
    """Streaming modes for large datasets"""
    MEMORY = "memory"      # Load everything into memory
    BATCH = "batch"        # Process in batches
    STREAMING = "streaming" # True streaming, one record at a time

@dataclass
class ImportOptions:
    """Options for data import operations"""
    format: DataFormat
    compression: CompressionType = CompressionType.NONE
    encoding: str = "utf-8"
    delimiter: str = ","
    quote_char: str = '"'
    skip_rows: int = 0
    max_rows: Optional[int] = None
    streaming_mode: StreamingMode = StreamingMode.MEMORY
    batch_size: int = 1000
    validate_data: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ExportOptions:
    """Options for data export operations"""
    format: DataFormat
    compression: CompressionType = CompressionType.NONE
    encoding: str = "utf-8"
    delimiter: str = ","
    quote_char: str = '"'
    include_headers: bool = True
    pretty_format: bool = False
    streaming_mode: StreamingMode = StreamingMode.MEMORY
    batch_size: int = 1000
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ScriptExecution:
    """Results from executing a SAIQL script"""
    script_path: str
    total_queries: int
    successful_queries: int
    failed_queries: int
    execution_time: float
    results: List[QueryResult]
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

class FileWatcher:
    """Watches files for changes and triggers actions"""
    
    def __init__(self, callback: Callable[[str], None]):
        self.callback = callback
        self.watched_files = {}
        self.running = False
        self._thread = None
        self._lock = threading.Lock()
    
    def watch_file(self, file_path: str) -> None:
        """Add file to watch list"""
        with self._lock:
            if Path(file_path).exists():
                stat = Path(file_path).stat()
                self.watched_files[file_path] = stat.st_mtime
    
    def start_watching(self, check_interval: float = 1.0) -> None:
        """Start watching files for changes"""
        if self.running:
            return
        
        self.running = True
        self._thread = threading.Thread(target=self._watch_loop, args=(check_interval,))
        self._thread.daemon = True
        self._thread.start()
        logger.info("File watcher started")
    
    def stop_watching(self) -> None:
        """Stop watching files"""
        self.running = False
        if self._thread:
            self._thread.join(timeout=2.0)
        logger.info("File watcher stopped")
    
    def _watch_loop(self, check_interval: float) -> None:
        """Main watching loop"""
        while self.running:
            try:
                with self._lock:
                    for file_path, last_mtime in list(self.watched_files.items()):
                        if Path(file_path).exists():
                            current_mtime = Path(file_path).stat().st_mtime
                            if current_mtime > last_mtime:
                                self.watched_files[file_path] = current_mtime
                                try:
                                    self.callback(file_path)
                                except Exception as e:
                                    logger.error(f"Error in file watcher callback: {e}")
                        else:
                            # File was deleted
                            del self.watched_files[file_path]
                
                time.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"Error in file watcher loop: {e}")
                time.sleep(check_interval)

class SAIQLIOInterface:
    """
    SAIQL I/O Interface - Enhanced File and Data Operations
    
    Provides comprehensive I/O capabilities for SAIQL including file operations,
    data import/export, batch processing, and external integrations.
    
    Features:
    - Script file execution (.saiql files)
    - Multi-format data import/export (CSV, JSON, Excel, XML, etc.)
    - Streaming operations for large datasets
    - Batch processing with progress tracking
    - File watching and auto-execution
    - Backup and versioning
    - Configuration management
    """
    
    def __init__(self, engine: Optional['SAIQLEngine'] = None,
                 work_directory: Optional[str] = None,
                 config: Optional[Dict[str, Any]] = None):
        """Initialize I/O interface"""

        # Check if SAIQL components are available
        if SAIQLEngine is None:
            raise ImportError(
                "SAIQL core components not available. "
                "Ensure core.engine and utils.helpers are installed."
            )

        # Initialize or use provided engine
        self.engine = engine if engine else SAIQLEngine()
        
        # Set up working directory
        self.work_directory = Path(work_directory) if work_directory else Path.cwd()
        self.work_directory.mkdir(parents=True, exist_ok=True)
        
        # Load configuration
        self.config = config or self._load_default_config()
        
        # Initialize file watcher
        self.file_watcher = FileWatcher(self._on_file_changed)
        
        # Statistics and state
        self.stats = {
            'scripts_executed': 0,
            'files_imported': 0,
            'files_exported': 0,
            'total_records_processed': 0,
            'total_io_time': 0.0
        }
        
        # Thread safety
        self._lock = threading.RLock()
        
        logger.info(f"SAIQL I/O Interface initialized - Working directory: {self.work_directory}")
    
    def _load_default_config(self) -> Dict[str, Any]:
        """Load default I/O configuration"""
        return {
            'script_extensions': ['.saiql', '.sq'],
            'auto_backup': True,
            'backup_directory': 'backups',
            'max_backup_files': 10,
            'streaming_threshold': 10000,  # Records
            'default_batch_size': 1000,
            'compression_threshold': 1024 * 1024,  # 1MB
            'watch_files': False,
            'export_formats': {
                'csv': {'delimiter': ',', 'quote_char': '"'},
                'tsv': {'delimiter': '\t', 'quote_char': '"'},
                'json': {'pretty_format': True},
                'excel': {'sheet_name': 'SAIQL_Results'}
            }
        }
    
    def execute_script(self, script_path: str, 
                      context: Optional[ExecutionContext] = None,
                      variables: Optional[Dict[str, str]] = None) -> ScriptExecution:
        """
        Execute a SAIQL script file
        
        Args:
            script_path: Path to .saiql script file
            context: Execution context (optional)
            variables: Variables for script substitution (optional)
            
        Returns:
            ScriptExecution with results and statistics
        """
        start_time = time.time()
        script_path = str(self.work_directory / script_path)
        
        logger.info(f"Executing SAIQL script: {script_path}")
        
        # Validate script file
        is_valid, error_msg = validate_file_path(script_path, must_exist=True,
                                               allowed_extensions=self.config['script_extensions'])
        
        if not is_valid:
            raise ValueError(f"Invalid script file: {error_msg}")
        
        # Read and parse script
        script_content = read_file(script_path)
        queries = self._parse_script(script_content, variables)
        
        # Execute queries
        results = []
        errors = []
        warnings = []
        successful_queries = 0
        
        for i, query in enumerate(queries):
            try:
                if query.strip():  # Skip empty queries
                    result = self.engine.execute(query, context)
                    results.append(result)
                    
                    if result.success:
                        successful_queries += 1
                    else:
                        errors.append(f"Query {i+1}: {result.error_message}")
                    
                    # Collect warnings
                    warnings.extend(result.warnings)
                    
            except Exception as e:
                error_msg = f"Query {i+1} execution failed: {e}"
                errors.append(error_msg)
                logger.error(error_msg)
        
        execution_time = time.time() - start_time
        
        # Update statistics
        with self._lock:
            self.stats['scripts_executed'] += 1
            self.stats['total_io_time'] += execution_time
        
        # Create backup if enabled
        if self.config.get('auto_backup', True):
            self._create_backup(script_path)
        
        execution_result = ScriptExecution(
            script_path=script_path,
            total_queries=len(queries),
            successful_queries=successful_queries,
            failed_queries=len(queries) - successful_queries,
            execution_time=execution_time,
            results=results,
            errors=errors,
            warnings=warnings,
            metadata={
                'variables_used': variables or {},
                'context': context.to_dict() if context and hasattr(context, 'to_dict') else None
            }
        )
        
        logger.info(f"Script execution completed: {successful_queries}/{len(queries)} queries successful")
        return execution_result
    
    def _parse_script(self, content: str, variables: Optional[Dict[str, str]] = None) -> List[str]:
        """Parse script content into individual queries"""
        
        # Apply variable substitution if provided
        if variables:
            for var_name, var_value in variables.items():
                content = content.replace(f"${{{var_name}}}", var_value)
                content = content.replace(f"${var_name}", var_value)  # Also support $VAR syntax
        
        # Split on semicolons and filter empty queries
        queries = []
        
        # Split by semicolons, but be careful of semicolons in strings
        current_query = ""
        in_string = False
        string_char = None
        
        for char in content:
            if not in_string:
                if char in ['"', "'"]:
                    in_string = True
                    string_char = char
                elif char == ';':
                    # End of query
                    if current_query.strip():
                        queries.append(current_query.strip())
                    current_query = ""
                    continue
            else:
                if char == string_char:
                    in_string = False
                    string_char = None
            
            current_query += char
        
        # Add final query if it doesn't end with semicolon
        if current_query.strip():
            queries.append(current_query.strip())
        
        # Filter out comments and empty lines
        filtered_queries = []
        for query in queries:
            lines = query.split('\n')
            non_comment_lines = []
            
            for line in lines:
                line = line.strip()
                if line and not line.startswith('--') and not line.startswith('#'):
                    non_comment_lines.append(line)
            
            if non_comment_lines:
                filtered_queries.append('\n'.join(non_comment_lines))
        
        return filtered_queries
    
    def import_data(self, file_path: str, table_name: str,
                   options: Optional[ImportOptions] = None) -> Dict[str, Any]:
        """
        Import data from external file into database
        
        Args:
            file_path: Path to data file
            table_name: Target table name
            options: Import options
            
        Returns:
            Import statistics and metadata
        """
        start_time = time.time()
        file_path = str(self.work_directory / file_path)
        
        if not options:
            # Auto-detect format from file extension
            ext = Path(file_path).suffix.lower()
            format_map = {
                '.csv': DataFormat.CSV,
                '.json': DataFormat.JSON,
                '.jsonl': DataFormat.JSONL,
                '.xlsx': DataFormat.EXCEL,
                '.xml': DataFormat.XML,
                '.tsv': DataFormat.TSV
            }
            data_format = format_map.get(ext, DataFormat.CSV)
            options = ImportOptions(format=data_format)
        
        logger.info(f"Importing data from {file_path} to table {table_name}")
        
        # Validate file
        is_valid, error_msg = validate_file_path(file_path, must_exist=True)
        if not is_valid:
            raise ValueError(f"Invalid import file: {error_msg}")
        
        # Import based on format
        if options.format == DataFormat.CSV:
            return self._import_csv(file_path, table_name, options)
        elif options.format == DataFormat.JSON:
            return self._import_json(file_path, table_name, options)
        elif options.format == DataFormat.JSONL:
            return self._import_jsonl(file_path, table_name, options)
        elif options.format == DataFormat.EXCEL:
            return self._import_excel(file_path, table_name, options)
        else:
            raise ValueError(f"Unsupported import format: {options.format}")
    
    def export_data(self, query_or_results: Union[str, List[QueryResult], List[Dict[str, Any]]], 
                   output_path: str,
                   options: Optional[ExportOptions] = None) -> Dict[str, Any]:
        """
        Export data to external file
        
        Args:
            query_or_results: SAIQL query string, QueryResult list, or data list
            output_path: Output file path
            options: Export options
            
        Returns:
            Export statistics and metadata
        """
        start_time = time.time()
        output_path = str(self.work_directory / output_path)
        
        if not options:
            # Auto-detect format from file extension
            ext = Path(output_path).suffix.lower()
            format_map = {
                '.csv': DataFormat.CSV,
                '.json': DataFormat.JSON,
                '.jsonl': DataFormat.JSONL,
                '.xlsx': DataFormat.EXCEL,
                '.xml': DataFormat.XML,
                '.tsv': DataFormat.TSV,
                '.sql': DataFormat.SQL
            }
            data_format = format_map.get(ext, DataFormat.CSV)
            options = ExportOptions(format=data_format)
        
        # Get data to export
        if isinstance(query_or_results, str):
            # Execute query to get data
            result = self.engine.execute(query_or_results)
            if not result.success:
                raise ValueError(f"Query execution failed: {result.error_message}")
            data = result.data
        elif isinstance(query_or_results, list) and query_or_results and isinstance(query_or_results[0], QueryResult):
            # Extract data from QueryResult list
            data = []
            for result in query_or_results:
                data.extend(result.data)
        else:
            # Assume it's already a data list
            data = query_or_results
        
        logger.info(f"Exporting {len(data)} records to {output_path}")
        
        # Create output directory if needed
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Export based on format
        if options.format == DataFormat.CSV:
            return self._export_csv(data, output_path, options)
        elif options.format == DataFormat.JSON:
            return self._export_json(data, output_path, options)
        elif options.format == DataFormat.JSONL:
            return self._export_jsonl(data, output_path, options)
        elif options.format == DataFormat.EXCEL:
            return self._export_excel(data, output_path, options)
        elif options.format == DataFormat.SQL:
            return self._export_sql(data, output_path, options)
        else:
            raise ValueError(f"Unsupported export format: {options.format}")
    
    def _import_csv(self, file_path: str, table_name: str, options: ImportOptions) -> Dict[str, Any]:
        """Import CSV file"""
        records_imported = 0
        
        with open(file_path, 'r', encoding=options.encoding) as f:
            # Skip rows if specified
            for _ in range(options.skip_rows):
                next(f, None)
            
            reader = csv.DictReader(f, delimiter=options.delimiter, quotechar=options.quote_char)
            
            for row_num, row in enumerate(reader):
                if options.max_rows and row_num >= options.max_rows:
                    break
                
                # Insert row into database
                columns = ', '.join(row.keys())
                values = ', '.join([f"'{_escape_sql_value(v)}'" for v in row.values()])

                insert_query = f"+V[{table_name}]::({values})>>oQ"
                result = self.engine.execute(insert_query)
                
                if result.success:
                    records_imported += 1
                else:
                    logger.warning(f"Failed to import row {row_num + 1}: {result.error_message}")
        
        with self._lock:
            self.stats['files_imported'] += 1
            self.stats['total_records_processed'] += records_imported
        
        return {
            'records_imported': records_imported,
            'table_name': table_name,
            'source_file': file_path,
            'format': options.format.value
        }
    
    def _import_json(self, file_path: str, table_name: str, options: ImportOptions) -> Dict[str, Any]:
        """Import JSON file"""
        with open(file_path, 'r', encoding=options.encoding) as f:
            data = json.load(f)
        
        # Handle different JSON structures
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict) and 'data' in data:
            records = data['data']
        else:
            records = [data]
        
        records_imported = 0
        
        for record in records:
            if options.max_rows and records_imported >= options.max_rows:
                break
            
            if isinstance(record, dict):
                columns = ', '.join(record.keys())
                values = ', '.join([f"'{_escape_sql_value(v)}'" for v in record.values()])

                insert_query = f"+V[{table_name}]::({values})>>oQ"
                result = self.engine.execute(insert_query)

                if result.success:
                    records_imported += 1

        with self._lock:
            self.stats['files_imported'] += 1
            self.stats['total_records_processed'] += records_imported

        return {
            'records_imported': records_imported,
            'table_name': table_name,
            'source_file': file_path,
            'format': options.format.value
        }

    def _import_jsonl(self, file_path: str, table_name: str, options: ImportOptions) -> Dict[str, Any]:
        """Import JSON Lines file"""
        records_imported = 0
        
        with open(file_path, 'r', encoding=options.encoding) as f:
            for line_num, line in enumerate(f):
                if options.max_rows and line_num >= options.max_rows:
                    break
                
                if line.strip():
                    try:
                        record = json.loads(line)

                        if isinstance(record, dict):
                            columns = ', '.join(record.keys())
                            values = ', '.join([f"'{_escape_sql_value(v)}'" for v in record.values()])

                            insert_query = f"+V[{table_name}]::({values})>>oQ"
                            result = self.engine.execute(insert_query)

                            if result.success:
                                records_imported += 1

                    except json.JSONDecodeError as e:
                        logger.warning(f"Invalid JSON on line {line_num + 1}: {e}")
        
        with self._lock:
            self.stats['files_imported'] += 1
            self.stats['total_records_processed'] += records_imported
        
        return {
            'records_imported': records_imported,
            'table_name': table_name,
            'source_file': file_path,
            'format': options.format.value
        }
    
    def _import_excel(self, file_path: str, table_name: str, options: ImportOptions) -> Dict[str, Any]:
        """Import Excel file.

        CE Edition: Excel import is not included to minimize dependencies.
        Install openpyxl and implement this method for Excel support.
        """
        logger.warning("Excel import requires openpyxl library (not included in CE)")
        
        return {
            'records_imported': 0,
            'table_name': table_name,
            'source_file': file_path,
            'format': options.format.value,
            'error': 'Excel import not implemented - requires openpyxl'
        }
    
    def _export_csv(self, data: List[Dict[str, Any]], output_path: str, options: ExportOptions) -> Dict[str, Any]:
        """Export to CSV file"""
        if not data:
            return {'records_exported': 0, 'output_file': output_path}
        
        with open(output_path, 'w', newline='', encoding=options.encoding) as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys(), 
                                  delimiter=options.delimiter, 
                                  quotechar=options.quote_char)
            
            if options.include_headers:
                writer.writeheader()
            
            writer.writerows(data)
        
        with self._lock:
            self.stats['files_exported'] += 1
            self.stats['total_records_processed'] += len(data)
        
        return {
            'records_exported': len(data),
            'output_file': output_path,
            'format': options.format.value
        }
    
    def _export_json(self, data: List[Dict[str, Any]], output_path: str, options: ExportOptions) -> Dict[str, Any]:
        """Export to JSON file"""
        with open(output_path, 'w', encoding=options.encoding) as f:
            if options.pretty_format:
                json.dump(data, f, indent=2, ensure_ascii=False)
            else:
                json.dump(data, f, ensure_ascii=False)
        
        with self._lock:
            self.stats['files_exported'] += 1
            self.stats['total_records_processed'] += len(data)
        
        return {
            'records_exported': len(data),
            'output_file': output_path,
            'format': options.format.value
        }
    
    def _export_jsonl(self, data: List[Dict[str, Any]], output_path: str, options: ExportOptions) -> Dict[str, Any]:
        """Export to JSON Lines file"""
        with open(output_path, 'w', encoding=options.encoding) as f:
            for record in data:
                json.dump(record, f, ensure_ascii=False)
                f.write('\n')
        
        with self._lock:
            self.stats['files_exported'] += 1
            self.stats['total_records_processed'] += len(data)
        
        return {
            'records_exported': len(data),
            'output_file': output_path,
            'format': options.format.value
        }
    
    def _export_excel(self, data: List[Dict[str, Any]], output_path: str, options: ExportOptions) -> Dict[str, Any]:
        """Export to Excel file.

        CE Edition: Excel export is not included to minimize dependencies.
        Falls back to CSV export. Install openpyxl for native Excel support.
        """
        logger.warning("Excel export requires openpyxl (not in CE) - exporting as CSV instead")
        
        csv_path = output_path.replace('.xlsx', '.csv')
        return self._export_csv(data, csv_path, ExportOptions(format=DataFormat.CSV))
    
    def _export_sql(self, data: List[Dict[str, Any]], output_path: str, options: ExportOptions) -> Dict[str, Any]:
        """Export as SQL INSERT statements"""
        if not data:
            return {'records_exported': 0, 'output_file': output_path}
        
        table_name = Path(output_path).stem  # Use filename as table name
        
        with open(output_path, 'w', encoding=options.encoding) as f:
            for record in data:
                columns = ', '.join(record.keys())
                values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in record.values()])
                
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values});\n"
                f.write(sql)
        
        with self._lock:
            self.stats['files_exported'] += 1
            self.stats['total_records_processed'] += len(data)
        
        return {
            'records_exported': len(data),
            'output_file': output_path,
            'format': options.format.value,
            'table_name': table_name
        }
    
    def _create_backup(self, file_path: str) -> Optional[str]:
        """Create backup of file"""
        try:
            backup_dir = self.work_directory / self.config['backup_directory']
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            file_path_obj = Path(file_path)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{file_path_obj.stem}_{timestamp}{file_path_obj.suffix}"
            backup_path = backup_dir / backup_name
            
            shutil.copy2(file_path, backup_path)
            
            # Clean up old backups
            self._cleanup_old_backups(backup_dir, file_path_obj.name)
            
            logger.info(f"Created backup: {backup_path}")
            return str(backup_path)
            
        except Exception as e:
            logger.error(f"Failed to create backup for {file_path}: {e}")
            return None
    
    def _cleanup_old_backups(self, backup_dir: Path, base_filename: str) -> None:
        """Clean up old backup files"""
        try:
            max_backups = self.config.get('max_backup_files', 10)
            
            # Find all backup files for this base file
            pattern = f"{Path(base_filename).stem}_*{Path(base_filename).suffix}"
            backups = list(backup_dir.glob(pattern))
            
            # Sort by modification time (newest first)
            backups.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            
            # Remove excess backups
            for backup in backups[max_backups:]:
                backup.unlink()
                logger.debug(f"Removed old backup: {backup}")
                
        except Exception as e:
            logger.error(f"Error cleaning up backups: {e}")
    
    def _on_file_changed(self, file_path: str) -> None:
        """Handle file change events"""
        logger.info(f"File changed detected: {file_path}")
        
        try:
            # Auto-execute if it's a script file
            if any(file_path.endswith(ext) for ext in self.config['script_extensions']):
                logger.info(f"Auto-executing changed script: {file_path}")
                result = self.execute_script(file_path)
                logger.info(f"Auto-execution completed: {result.successful_queries}/{result.total_queries} successful")
        
        except Exception as e:
            logger.error(f"Error in file change handler: {e}")
    
    def start_watching(self, file_paths: List[str]) -> None:
        """Start watching files for changes"""
        for file_path in file_paths:
            full_path = str(self.work_directory / file_path)
            self.file_watcher.watch_file(full_path)
        
        self.file_watcher.start_watching()
        logger.info(f"Started watching {len(file_paths)} files")
    
    def stop_watching(self) -> None:
        """Stop watching files"""
        self.file_watcher.stop_watching()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get I/O interface statistics"""
        with self._lock:
            return {
                'scripts_executed': self.stats['scripts_executed'],
                'files_imported': self.stats['files_imported'],
                'files_exported': self.stats['files_exported'],
                'total_records_processed': self.stats['total_records_processed'],
                'total_io_time': self.stats['total_io_time'],
                'work_directory': str(self.work_directory),
                'backup_directory': str(self.work_directory / self.config['backup_directory']),
                'watching_files': self.file_watcher.running
            }
    
    def cleanup(self) -> None:
        """Clean up resources"""
        self.stop_watching()
        logger.info("SAIQL I/O Interface cleanup completed")

def main():
    """Test the SAIQL I/O Interface"""
    print("SAIQL I/O Interface Test")
    print("=" * 50)
    
    try:
        # Initialize I/O interface
        io_interface = SAIQLIOInterface()
        
        print(f"I/O Interface initialized")
        print(f"Working directory: {io_interface.work_directory}")
        
        # Create test script
        test_script = """
        -- SAIQL Test Script
        *3[users]::name,email>>oQ;
        
        -- Count query
        *COUNT[users]::*>>oQ;
        
        -- Transaction example
        $1;
        $2;
        """
        
        script_path = "test_script.saiql"
        with open(io_interface.work_directory / script_path, 'w') as f:
            f.write(test_script)
        
        print(f"\nCreated test script: {script_path}")
        
        # Execute script
        print(f"Executing script...")
        result = io_interface.execute_script(script_path)
        
        print(f"Script execution results:")
        print(f"  Total queries: {result.total_queries}")
        print(f"  Successful: {result.successful_queries}")
        print(f"  Failed: {result.failed_queries}")
        print(f"  Execution time: {result.execution_time:.4f}s")
        
        if result.errors:
            print(f"  Errors: {len(result.errors)}")
            for error in result.errors[:3]:  # Show first 3 errors
                print(f"    - {error}")
        
        # Test data export
        print(f"\nTesting data export...")
        
        # Create sample data
        sample_data = [
            {'name': 'Alice', 'email': 'alice@example.com', 'age': 30},
            {'name': 'Bob', 'email': 'bob@example.com', 'age': 25},
            {'name': 'Delta', 'email': 'delta@example.com', 'age': 35}
        ]
        
        # Export to CSV
        export_result = io_interface.export_data(sample_data, "test_export.csv")
        print(f"CSV export: {export_result['records_exported']} records")
        
        # Export to JSON
        export_result = io_interface.export_data(sample_data, "test_export.json")
        print(f"JSON export: {export_result['records_exported']} records")
        
        # Show statistics
        print(f"\nI/O Statistics:")
        stats = io_interface.get_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # Cleanup
        io_interface.cleanup()
        print(f"\nI/O Interface test completed successfully!")
        
    except Exception as e:
        print(f"I/O Interface test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
