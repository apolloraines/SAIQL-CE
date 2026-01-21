#!/usr/bin/env python3
"""
SAIQL Translator - Higher-Order Object Translation (L2-L4)

This module provides conservative, provable translation for:
- L2: Views
- L3: Procedures/Functions (subset)
- L4: Triggers/Packages

Key Principles:
- Conservative (no silent semantic changes)
- Deterministic (same input → same output)
- Flagged (explicit capability modes)
- Provable (tests for every claim)
"""

import logging
from enum import Enum
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


class TranslateMode(Enum):
    """Explicit translation capability modes."""
    ANALYZE = "analyze"  # Default: parse + report, NO SQL output
    STUB = "stub"  # Generate stubs (fail loudly for Postgres/Oracle; limitations for MSSQL/MySQL/SQLite)
    SUBSET_TRANSLATE = "subset_translate"  # Translate only proven-safe patterns


class ObjectType(Enum):
    """Higher-order database object types."""
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    PROCEDURE = "procedure"
    FUNCTION = "function"
    TRIGGER = "trigger"
    PACKAGE = "package"


class RiskLevel(Enum):
    """Risk assessment for translation operations."""
    SAFE = "safe"  # Fully supported, proven equivalent
    LOW = "low"  # Minor semantic differences, documented
    MEDIUM = "medium"  # Significant differences, manual review needed
    HIGH = "high"  # Not semantically equivalent, manual rewrite required
    CRITICAL = "critical"  # Unsafe to translate, manual rewrite mandatory


@dataclass
class Warning:
    """Translation warning with deterministic ordering."""
    severity: RiskLevel
    object_name: str
    message: str
    reason: str

    def __lt__(self, other):
        """Deterministic sorting: severity → object_name → message."""
        if self.severity != other.severity:
            return self.severity.value < other.severity.value
        if self.object_name != other.object_name:
            return self.object_name < other.object_name
        return self.message < other.message


@dataclass
class ManualStep:
    """Manual action required for translation."""
    object_name: str
    action: str
    reason: str

    def __lt__(self, other):
        """Deterministic sorting by object name."""
        return self.object_name < other.object_name


@dataclass
class TranslationResult:
    """Result of a translation operation."""
    object_type: ObjectType
    object_name: str
    mode: TranslateMode
    sql_output: Optional[str] = None  # None for analyze mode
    risk_level: RiskLevel = RiskLevel.HIGH
    warnings: List[Warning] = field(default_factory=list)
    manual_steps: List[ManualStep] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class Translator:
    """
    Conservative higher-order object translator.

    Enforces capability boundaries:
    - ANALYZE: No SQL output (default, safest)
    - STUB: Stubs (fail loudly for Postgres/Oracle; limitations for MSSQL/MySQL/SQLite)
    - SUBSET_TRANSLATE: Only proven-safe patterns
    """

    def __init__(
        self,
        mode: TranslateMode = TranslateMode.ANALYZE,
        source_dialect: str = "oracle",
        target_dialect: str = "postgres"
    ):
        self.mode = mode
        self.source_dialect = source_dialect.lower()
        self.target_dialect = target_dialect.lower()
        self.results: List[TranslationResult] = []

    def translate_object(
        self,
        obj_type: ObjectType,
        obj_name: str,
        obj_def: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> TranslationResult:
        """
        Translate a higher-order database object.

        Args:
            obj_type: Type of object (VIEW, TRIGGER, etc.)
            obj_name: Object name
            obj_def: Object definition (DDL)
            metadata: Optional metadata

        Returns:
            TranslationResult with SQL output (if applicable) and warnings
        """
        logger.info(f"Translating {obj_type.value}: {obj_name} (mode={self.mode.value})")

        if self.mode == TranslateMode.ANALYZE:
            result = self._analyze_only(obj_type, obj_name, obj_def, metadata)
        elif self.mode == TranslateMode.STUB:
            result = self._generate_stub(obj_type, obj_name, obj_def, metadata)
        elif self.mode == TranslateMode.SUBSET_TRANSLATE:
            result = self._subset_translate(obj_type, obj_name, obj_def, metadata)
        else:
            raise ValueError(f"Unknown translate mode: {self.mode}")

        self.results.append(result)
        return result

    def _analyze_only(
        self,
        obj_type: ObjectType,
        obj_name: str,
        obj_def: str,
        metadata: Optional[Dict[str, Any]]
    ) -> TranslationResult:
        """
        Analyze mode: Parse and report, NO SQL output.

        This is the default and safest mode.
        """
        result = TranslationResult(
            object_type=obj_type,
            object_name=obj_name,
            mode=TranslateMode.ANALYZE,
            sql_output=None,  # CRITICAL: No SQL output in analyze mode
            risk_level=RiskLevel.HIGH,  # Conservative default
            metadata=metadata or {}
        )

        # Workstream 06.3: Package analysis
        if obj_type == ObjectType.PACKAGE:
            from core.package_analyzer import PackageAnalyzer
            analyzer = PackageAnalyzer(self.source_dialect, self.target_dialect)
            analysis = analyzer.analyze(obj_def, obj_name)

            # Add package analysis to metadata
            result.metadata['package_analysis'] = {
                'has_spec': analysis.has_spec,
                'has_body': analysis.has_body,
                'procedure_count': len(analysis.procedures),
                'function_count': len(analysis.functions),
                'dependencies': analysis.dependencies,
                'complexity_score': analysis.complexity_score
            }

            # Add warnings from package analysis
            for warning in analysis.warnings:
                result.warnings.append(
                    Warning(
                        severity=RiskLevel.HIGH,
                        message=warning,
                        object_name=obj_name,
                        reason='Package analysis detected complexity or Oracle-specific features'
                    )
                )

            # Add manual steps from package analysis
            for step in analysis.manual_steps:
                result.manual_steps.append(
                    ManualStep(
                        object_name=obj_name,
                        action=step,
                        reason='Package requires manual migration'
                    )
                )

            logger.info(f"Analyzed package '{obj_name}' - complexity={analysis.complexity_score}, "
                       f"procedures={len(analysis.procedures)}, functions={len(analysis.functions)}")
        else:
            # Add default manual step for non-package objects in analyze mode
            result.manual_steps.append(
                ManualStep(
                    object_name=obj_name,
                    action=f"Manual review required for {obj_type.value}",
                    reason=f"Object analyzed but not translated (mode=analyze)"
                )
            )

        logger.info(f"Analyzed {obj_type.value} '{obj_name}' - no SQL output (analyze mode)")
        return result

    def _generate_stub(
        self,
        obj_type: ObjectType,
        obj_name: str,
        obj_def: str,
        metadata: Optional[Dict[str, Any]]
    ) -> TranslationResult:
        """
        Stub mode: Generate safe stub that fails loudly if executed.

        Stubs prevent silent semantic drift.
        """
        # Generate stub SQL that fails loudly
        stub_sql = self._create_safe_stub(obj_type, obj_name)

        result = TranslationResult(
            object_type=obj_type,
            object_name=obj_name,
            mode=TranslateMode.STUB,
            sql_output=stub_sql,
            risk_level=RiskLevel.CRITICAL,  # Stubs are not functional
            metadata=metadata or {}
        )

        # Add warning about stub
        result.warnings.append(
            Warning(
                severity=RiskLevel.CRITICAL,
                object_name=obj_name,
                message=f"Generated stub for {obj_type.value} (not functional)",
                reason="Object not in supported translation subset"
            )
        )

        # Add manual step
        result.manual_steps.append(
            ManualStep(
                object_name=obj_name,
                action=f"Manually rewrite {obj_type.value}",
                reason="Stub generated - not semantically equivalent to source"
            )
        )

        logger.info(f"Generated stub for {obj_type.value} '{obj_name}'")
        return result

    def _create_safe_stub(self, obj_type: ObjectType, obj_name: str) -> str:
        """
        Create a stub for unsupported objects.

        Stub behavior depends on object type and target dialect:
        - Views (Postgres/Oracle): Executable stubs that fail loudly (documented behavior)
        - Views (MSSQL): Executable stubs with limitations (may return NULL with ARITHIGNORE ON)
        - Views (MySQL/SQLite): Executable stubs with limitations (may return NULL)
        - Procedures/Functions/Triggers (Postgres): Executable plpgsql stubs that fail loudly
        - Procedures/Functions/Triggers (non-Postgres): Comment-only stubs (no executable body)

        Dialect-specific implementation.
        """
        if obj_type == ObjectType.VIEW:
            # Dialect-specific stub generation
            if self.target_dialect == 'postgres':
                # Postgres: Use function that raises exception (documented behavior: fails loudly)
                return f"""-- STUB: {obj_name}
-- WARNING: This is a non-functional stub generated by SAIQL
-- Manual rewrite required for semantic equivalence

-- Helper function that raises exception
CREATE OR REPLACE FUNCTION {obj_name}_stub_error()
RETURNS TABLE (error_message TEXT) AS $$
BEGIN
    RAISE EXCEPTION 'Manual rewrite required: View "{obj_name}" is a non-functional stub generated by SAIQL';
    RETURN;
END;
$$ LANGUAGE plpgsql;

-- View that calls the error function
CREATE VIEW {obj_name} AS
SELECT * FROM {obj_name}_stub_error();
"""
            elif self.target_dialect == 'oracle':
                # Oracle: Division by zero raises ORA-01476 (documented behavior: fails loudly)
                return f"""-- STUB: {obj_name}
-- WARNING: This is a non-functional stub generated by SAIQL
-- Manual rewrite required for semantic equivalence
CREATE VIEW {obj_name} AS
SELECT
    'Manual rewrite required: View "{obj_name}" is a non-functional stub' AS error_message,
    1/0 AS force_error;
"""
            elif self.target_dialect == 'mssql':
                # MSSQL: Division by zero behavior depends on session settings
                # WARNING: With SET ARITHIGNORE ON, 1/0 returns NULL (does not fail loudly)
                # Only fails loudly with default settings (ARITHIGNORE OFF, ARITHABORT ON)
                return f"""-- STUB: {obj_name}
-- WARNING: This is a non-functional stub generated by SAIQL
-- Manual rewrite required for semantic equivalence
-- LIMITATION: This stub may return NULL instead of raising an error
--             (depends on MSSQL session settings: ARITHIGNORE, ARITHABORT)
CREATE VIEW {obj_name} AS
SELECT
    'Manual rewrite required: View "{obj_name}" is a non-functional stub' AS error_message,
    1/0 AS force_error;
"""
            elif self.target_dialect == 'mysql':
                # MySQL: Division by zero behavior depends on SQL mode
                # WARNING: In default configuration, 1/0 returns NULL (does not fail loudly)
                # Only fails loudly if ERROR_FOR_DIVISION_BY_ZERO is enabled in sql_mode
                return f"""-- STUB: {obj_name}
-- WARNING: This is a non-functional stub generated by SAIQL
-- Manual rewrite required for semantic equivalence
-- LIMITATION: This stub may return NULL instead of raising an error
--             (depends on MySQL sql_mode configuration)
CREATE VIEW {obj_name} AS
SELECT
    'Manual rewrite required: View "{obj_name}" is a non-functional stub' AS error_message,
    1/0 AS force_error;
"""
            elif self.target_dialect == 'sqlite':
                # SQLite: Division by zero returns NULL (does not fail loudly)
                # LIMITATION: SQLite has no reliable "fail loudly" mechanism for views
                return f"""-- STUB: {obj_name}
-- WARNING: This is a non-functional stub generated by SAIQL
-- Manual rewrite required for semantic equivalence
-- LIMITATION: SQLite returns NULL for division by zero (does not fail loudly)
--             This stub will NOT prevent silent breakage
CREATE VIEW {obj_name} AS
SELECT
    'Manual rewrite required: View "{obj_name}" is a non-functional stub' AS error_message,
    1/0 AS force_error;
"""
            else:
                # Generic fallback: Best-effort division by zero
                # WARNING: Behavior varies by dialect
                return f"""-- STUB: {obj_name}
-- WARNING: This is a non-functional stub generated by SAIQL
-- Manual rewrite required for semantic equivalence
-- LIMITATION: Stub failure behavior not verified for this dialect
CREATE VIEW {obj_name} AS
SELECT
    'Manual rewrite required: View "{obj_name}" is a non-functional stub' AS error_message,
    1/0 AS force_error;
"""
        elif obj_type in (ObjectType.PROCEDURE, ObjectType.FUNCTION):
            if self.target_dialect == 'postgres':
                return f"""-- STUB: {obj_name}
-- WARNING: This is a non-functional stub generated by SAIQL
-- Manual rewrite required for semantic equivalence
CREATE OR REPLACE FUNCTION {obj_name}()
RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'Manual rewrite required: {obj_name} is a non-functional stub';
END;
$$ LANGUAGE plpgsql;
"""
            else:
                # Non-Postgres: Comment-only stub (dialect-specific stored procedure syntax varies)
                return f"""-- STUB: {obj_name}
-- WARNING: This is a non-functional stub generated by SAIQL
-- Manual rewrite required for semantic equivalence
-- LIMITATION: Stored procedure/function stubs only supported for Postgres target
-- For {self.target_dialect}, manual rewrite required
"""
        elif obj_type == ObjectType.TRIGGER:
            if self.target_dialect == 'postgres':
                return f"""-- STUB: {obj_name}
-- WARNING: This is a non-functional stub generated by SAIQL
-- Manual rewrite required for semantic equivalence
CREATE OR REPLACE FUNCTION {obj_name}_stub_func()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'Manual rewrite required: {obj_name} is a non-functional stub';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""
            else:
                # Non-Postgres: Comment-only stub (dialect-specific trigger syntax varies)
                return f"""-- STUB: {obj_name}
-- WARNING: This is a non-functional stub generated by SAIQL
-- Manual rewrite required for semantic equivalence
-- LIMITATION: Trigger stubs only supported for Postgres target
-- For {self.target_dialect}, manual rewrite required
"""
        elif obj_type == ObjectType.PACKAGE:
            # Workstream 06.3: Package stubs
            # Packages are Oracle-specific - only comment-only stubs available
            return f"""-- STUB: {obj_name}
-- WARNING: This is a non-functional stub generated by SAIQL
-- Manual rewrite required for semantic equivalence
--
-- PACKAGE MIGRATION NOTES:
-- Oracle packages are dialect-specific and cannot be automatically translated.
-- This package must be manually refactored into target dialect constructs.
--
-- Recommended approach:
-- 1. Extract procedures/functions into separate schema objects
-- 2. Rewrite logic in target dialect syntax
-- 3. Consider target dialect's module/schema organization
--
-- LIMITATION: No automatic package translation available
-- Target dialect: {self.target_dialect}
"""
        else:
            return f"""-- STUB: {obj_name}
-- WARNING: Unsupported object type {obj_type.value}
-- Manual rewrite required
"""

    def _subset_translate(
        self,
        obj_type: ObjectType,
        obj_name: str,
        obj_def: str,
        metadata: Optional[Dict[str, Any]]
    ) -> TranslationResult:
        """
        Subset translate mode: Translate only proven-safe patterns.

        Falls back to stub for unsupported patterns.
        """
        # Check if pattern is in supported subset
        if self._is_supported_pattern(obj_type, obj_def):
            # Translate (implementation in workstream-specific modules)
            result = self._translate_supported(obj_type, obj_name, obj_def, metadata)
        else:
            # Fall back to stub for unsupported patterns
            result = self._generate_stub(obj_type, obj_name, obj_def, metadata)
            result.mode = TranslateMode.SUBSET_TRANSLATE  # Preserve mode intent

        logger.info(f"Subset translate {obj_type.value} '{obj_name}' - risk={result.risk_level.value}")
        return result

    def _is_supported_pattern(self, obj_type: ObjectType, obj_def: str) -> bool:
        """
        Check if object matches a supported translation pattern.

        Conservative: Default to False (unsupported).
        Workstream-specific modules will override with explicit whitelists.
        """
        # Workstream 06.1: View translation
        if obj_type == ObjectType.VIEW:
            from core.view_translator import ViewTranslator
            view_translator = ViewTranslator(self.source_dialect, self.target_dialect)
            return view_translator.is_supported_pattern(obj_def)

        # Workstream 06.2: Trigger translation
        if obj_type == ObjectType.TRIGGER:
            from core.trigger_translator import TriggerTranslator
            trigger_translator = TriggerTranslator(self.source_dialect, self.target_dialect)
            return trigger_translator.is_supported_pattern(obj_def)

        # Default: Nothing is supported (conservative)
        # Workstream 06.3 will add package checks
        return False

    def _translate_supported(
        self,
        obj_type: ObjectType,
        obj_name: str,
        obj_def: str,
        metadata: Optional[Dict[str, Any]]
    ) -> TranslationResult:
        """
        Translate a supported pattern.

        Workstream-specific modules implement actual translation logic.
        """
        # Workstream 06.1: View translation
        if obj_type == ObjectType.VIEW:
            from core.view_translator import ViewTranslator
            view_translator = ViewTranslator(self.source_dialect, self.target_dialect)

            try:
                translated_sql, risk_level = view_translator.translate(obj_name, obj_def)

                result = TranslationResult(
                    object_type=obj_type,
                    object_name=obj_name,
                    mode=TranslateMode.SUBSET_TRANSLATE,
                    sql_output=translated_sql,
                    risk_level=risk_level,
                    metadata=metadata or {}
                )

                # Add warning about unverified syntax (Gemini concession requirement)
                result.warnings.append(
                    Warning(
                        severity=RiskLevel.LOW,
                        object_name=obj_name,
                        message="Translated SQL syntax unverified (no compile-check)",
                        reason="Manual verification required - automated compile-check not implemented"
                    )
                )

                # Add risk-level warning for supported translations
                if risk_level in [RiskLevel.LOW, RiskLevel.MEDIUM]:
                    result.warnings.append(
                        Warning(
                            severity=risk_level,
                            object_name=obj_name,
                            message=f"View translated with {risk_level.value} risk",
                            reason="Manual review recommended"
                        )
                    )

                logger.info(f"Translated view '{obj_name}' - risk={risk_level.value}")
                return result

            except Exception as e:
                logger.error(f"Translation failed for view '{obj_name}': {e}")
                # Fall back to stub on translation error
                return self._generate_stub(obj_type, obj_name, obj_def, metadata)

        # Workstream 06.2: Trigger translation (Postgres-only)
        if obj_type == ObjectType.TRIGGER:
            # Trigger translation only supported for Postgres target
            if self.target_dialect != 'postgres':
                # Fall back to stub for non-Postgres targets
                logger.warning(f"Trigger translation only supported for Postgres target (requested: {self.target_dialect})")
                return self._generate_stub(obj_type, obj_name, obj_def, metadata)

            from core.trigger_translator import TriggerTranslator
            trigger_translator = TriggerTranslator(self.source_dialect, self.target_dialect)

            try:
                translated_sql, risk_level = trigger_translator.translate(obj_name, obj_def)

                result = TranslationResult(
                    object_type=obj_type,
                    object_name=obj_name,
                    mode=TranslateMode.SUBSET_TRANSLATE,
                    sql_output=translated_sql,
                    risk_level=risk_level,
                    metadata=metadata or {}
                )

                # Add warning about unverified syntax (Gemini concession requirement)
                result.warnings.append(
                    Warning(
                        severity=RiskLevel.LOW,
                        object_name=obj_name,
                        message="Translated SQL syntax unverified (no compile-check)",
                        reason="Manual verification required - automated compile-check not implemented"
                    )
                )

                # Add risk-level warning for supported translations
                if risk_level in [RiskLevel.LOW, RiskLevel.MEDIUM]:
                    result.warnings.append(
                        Warning(
                            severity=risk_level,
                            object_name=obj_name,
                            message=f"Trigger translated with {risk_level.value} risk",
                            reason="Manual review recommended"
                        )
                    )

                logger.info(f"Translated trigger '{obj_name}' - risk={risk_level.value}")
                return result

            except Exception as e:
                logger.error(f"Translation failed for trigger '{obj_name}': {e}")
                # Fall back to stub on translation error
                return self._generate_stub(obj_type, obj_name, obj_def, metadata)

        # Placeholder for other object types
        raise NotImplementedError(
            f"Translation not implemented for {obj_type.value}. "
            "This will be added in workstream-specific modules (06.3)."
        )

    def get_report(self) -> Dict[str, Any]:
        """
        Generate deterministic translation report (Workstream 06.4).

        Returns:
            Report dict with counts, warnings, manual steps, risk summary
        """
        from core.report_generator import generate_report

        report = generate_report(self.results, self.mode, self.source_dialect, self.target_dialect)
        return report.to_dict()

    def get_report_text(self) -> str:
        """
        Generate human-readable translation report (Workstream 06.4).

        Returns:
            Formatted text report
        """
        from core.report_generator import generate_report

        report = generate_report(self.results, self.mode, self.source_dialect, self.target_dialect)
        return report.to_text()
