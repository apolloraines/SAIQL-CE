"""
SAIQL Migration Runner
======================

Handles migration and analysis operations between database backends.

Change Notes (2026-01-20):
- Fixed OracleAdapter import order: moved import before isinstance check
- Moved RoutineArgumentIR import to module top (was inside loop)
"""

import logging
import os
from typing import Dict, Any, List

from core.database_manager import DatabaseManager
from core.schema_ir import RoutineCapability, RoutineIR, RoutineArgumentIR
from core.routine_translator import RoutineTranslator
from core.routine_reporter import RoutineReporter

# Optional Oracle adapter import - may not be available in all deployments
try:
    from extensions.plugins.oracle_adapter import OracleAdapter
    ORACLE_ADAPTER_AVAILABLE = True
except ImportError:
    OracleAdapter = None
    ORACLE_ADAPTER_AVAILABLE = False

logger = logging.getLogger(__name__)


class MigrationRunner:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.db_manager = DatabaseManager(config=config)

        # Normalize routines mode to RoutineCapability enum (config values are strings from JSON)
        mode_value = config.get('routines', {}).get('mode', 'none')
        if isinstance(mode_value, str):
            try:
                self.routines_mode = RoutineCapability(mode_value)
            except ValueError:
                logger.warning(f"Unknown routine mode '{mode_value}', defaulting to NONE")
                self.routines_mode = RoutineCapability.NONE
        elif isinstance(mode_value, RoutineCapability):
            self.routines_mode = mode_value
        else:
            self.routines_mode = RoutineCapability.NONE

        self.routines_out_dir = config.get('routines', {}).get('out_dir', './migration_report')

    def close(self):
        """Close database connections and release resources."""
        if hasattr(self, 'db_manager') and self.db_manager:
            self.db_manager.close_all()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def run(self, source: str, target: str):
        """Run migration or analysis"""
        logger.info(f"Starting migration/analysis: {source} -> {target}")

        # 1. Routine Analysis
        if self.routines_mode != RoutineCapability.NONE:
            self._process_routines(source, target)

    def _process_routines(self, source: str, target: str):
        logger.info(f"Processing Routines in mode: {self.routines_mode.value}")

        # Get Adapters
        # We need direct access to Oracle specific methods
        if source != 'oracle':
            logger.warning("Routine processing only supported for Oracle source in Phase 04")
            return

        if not ORACLE_ADAPTER_AVAILABLE:
            logger.error("OracleAdapter not available - cannot process Oracle routines")
            return

        # Initialize Source
        source_adapter = self.db_manager.adapters.get(source)
        if not source_adapter:
            # Force initialize
            self.db_manager._initialize_backend(source)
            source_adapter = self.db_manager.adapters.get(source)

        # Unwrap if wrapper
        real_adapter = source_adapter.adapter if hasattr(source_adapter, 'adapter') else source_adapter

        if not isinstance(real_adapter, OracleAdapter):
            logger.error("Source adapter is not OracleAdapter, cannot introspect routines")
            return

        # 1. Introspect
        logger.info("Introspecting routines...")
        raw_routines = real_adapter.get_routines()
        logger.info(f"Found {len(raw_routines)} routines")

        # 2. Convert to IR
        ir_list = []
        for r in raw_routines:
            # Reconstruct RoutineIR
            args = [RoutineArgumentIR(
                name=a['name'],
                data_type=a['type'],
                mode=a.get('mode', 'IN'),
                default_value=a.get('default')
            ) for a in r['arguments']]

            ir = RoutineIR(
                name=r['name'],
                arguments=args,
                return_type=r['return_type'],
                body_source=r['body_source'],
                language='PL/SQL'
            )
            ir_list.append(ir)

        # 3. Translate/Analyze
        translator = RoutineTranslator(source_dialect=source, target_dialect=target)
        results = []

        for ir in ir_list:
            res = translator.process(ir, self.routines_mode)
            results.append(res)

        # 4. Report
        report = RoutineReporter.generate_markdown_report(results)

        # write report
        os.makedirs(self.routines_out_dir, exist_ok=True)
        report_path = os.path.join(self.routines_out_dir, "routine_migration_report.md")
        with open(report_path, "w") as f:
            f.write(report)

        # Write SQL artifacts
        sql_path = os.path.join(self.routines_out_dir, "routines.sql")
        with open(sql_path, "w") as f:
            for res in results:
                if res.generated_code:
                    f.write(f"-- {res.routine_name} ({res.outcome})\n")
                    f.write(res.generated_code)
                    f.write("\n\n")

        logger.info(f"Routine report written to {report_path}")
