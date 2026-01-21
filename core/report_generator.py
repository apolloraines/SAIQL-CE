"""
Translation report generator (Workstream 06.4).

Generates per-run reports summarizing translation results:
- Object counts (detected/translated/stubbed/skipped)
- Risk score summary
- Manual steps checklist
"""

from typing import List, Dict
from collections import defaultdict
from core.translator import TranslationResult, TranslateMode


class TranslationReport:
    """
    Comprehensive translation report.

    Generated from a list of TranslationResult objects.
    Deterministic: same inputs → same report.
    """

    def __init__(self, results: List[TranslationResult], mode: TranslateMode, source_dialect: str = None, target_dialect: str = None):
        self.results = results
        self.mode = mode
        self.source_dialect = source_dialect
        self.target_dialect = target_dialect
        self._generate_report()

    def _generate_report(self):
        """Generate all report sections."""
        self.counts = self._calculate_counts()
        self.risk_summary = self._calculate_risk_summary()
        self.manual_steps = self._collect_manual_steps()
        self.warnings = self._collect_warnings()

    def _calculate_counts(self) -> Dict[str, int]:
        """
        Calculate object counts by category.

        Categories:
        - detected: Total objects processed
        - translated: Objects successfully translated
        - stubbed: Objects that generated stubs
        - analyzed_only: Objects analyzed without SQL output
        """
        counts = {
            'detected': len(self.results),
            'translated': 0,
            'stubbed': 0,
            'analyzed_only': 0
        }

        # Count by object type
        by_type = defaultdict(int)
        for result in self.results:
            by_type[result.object_type.value] += 1

        counts['by_type'] = dict(sorted(by_type.items()))

        # Categorize by outcome
        for result in self.results:
            if result.sql_output is None:
                counts['analyzed_only'] += 1
            elif 'STUB' in result.sql_output:
                counts['stubbed'] += 1
            else:
                counts['translated'] += 1

        return counts

    def _calculate_risk_summary(self) -> Dict[str, int]:
        """
        Calculate risk score summary.

        Breakdown by risk level (SAFE/LOW/MEDIUM/HIGH/CRITICAL).
        """
        risk_summary = {
            'safe': 0,
            'low': 0,
            'medium': 0,
            'high': 0,
            'critical': 0
        }

        for result in self.results:
            risk_level = result.risk_level.value.lower()
            risk_summary[risk_level] += 1

        return risk_summary

    def _collect_manual_steps(self) -> List[Dict[str, str]]:
        """
        Collect and deduplicate manual steps from all objects.

        Returns list of manual steps, sorted deterministically.
        """
        steps = []
        seen = set()

        # Collect all manual steps
        for result in self.results:
            for step in result.manual_steps:
                # Create unique key for deduplication
                key = f"{step.object_name}|{step.action}"
                if key not in seen:
                    seen.add(key)
                    steps.append({
                        'object': step.object_name,  # Legacy key name for backward compatibility
                        'object_name': step.object_name,
                        'action': step.action,
                        'reason': step.reason
                    })

        # Sort deterministically: by object_name, then action
        steps.sort(key=lambda x: (x['object_name'], x['action']))

        return steps

    def _collect_warnings(self) -> List[Dict[str, str]]:
        """
        Collect all warnings from translation results.

        Returns list of warnings, sorted deterministically.
        """
        warnings = []

        for result in self.results:
            for warning in result.warnings:
                warnings.append({
                    'severity': warning.severity.value if hasattr(warning.severity, 'value') else warning.severity,
                    'object': warning.object_name,  # Legacy key name for backward compatibility
                    'object_name': warning.object_name,
                    'message': warning.message,
                    'reason': warning.reason
                })

        # Sort deterministically: by severity, then object_name, then message
        severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        warnings.sort(key=lambda x: (
            severity_order.get(x['severity'], 99),
            x['object_name'],
            x['message']
        ))

        return warnings

    def to_text(self) -> str:
        """
        Generate human-readable text report.

        Deterministic format for reproducible output.
        """
        lines = []

        # Header
        lines.append("=" * 80)
        lines.append("SAIQL Translation Report")
        lines.append("=" * 80)
        lines.append(f"Mode: {self.mode.value}")
        lines.append("")

        # Object Counts
        lines.append("Object Counts")
        lines.append("-" * 40)
        lines.append(f"  Detected:      {self.counts['detected']}")
        if self.mode == TranslateMode.SUBSET_TRANSLATE:
            lines.append(f"  Translated:    {self.counts['translated']}")
            lines.append(f"  Stubbed:       {self.counts['stubbed']}")
        elif self.mode == TranslateMode.STUB:
            lines.append(f"  Stubbed:       {self.counts['stubbed']}")
        elif self.mode == TranslateMode.ANALYZE:
            lines.append(f"  Analyzed Only: {self.counts['analyzed_only']}")
        lines.append("")

        # Breakdown by type
        if self.counts['by_type']:
            lines.append("  By Type:")
            for obj_type, count in self.counts['by_type'].items():
                lines.append(f"    {obj_type:15} {count}")
            lines.append("")

        # Risk Summary
        lines.append("Risk Summary")
        lines.append("-" * 40)
        for risk_level, count in self.risk_summary.items():
            if count > 0:
                lines.append(f"  {risk_level.upper():10} {count}")
        lines.append("")

        # Warnings
        if self.warnings:
            lines.append("Warnings")
            lines.append("-" * 40)
            for i, warning in enumerate(self.warnings, 1):
                lines.append(f"  {i}. [{warning['severity'].upper()}] {warning['object_name']}")
                lines.append(f"     {warning['message']}")
                if warning['reason']:
                    lines.append(f"     Reason: {warning['reason']}")
                lines.append("")

        # Manual Steps Checklist
        if self.manual_steps:
            lines.append("Manual Steps Checklist")
            lines.append("-" * 40)
            for i, step in enumerate(self.manual_steps, 1):
                lines.append(f"  {i}. {step['object_name']}: {step['action']}")
                if step['reason']:
                    lines.append(f"     Reason: {step['reason']}")
                lines.append("")

        lines.append("=" * 80)
        lines.append("End of Report")
        lines.append("=" * 80)

        return "\n".join(lines)

    def to_dict(self) -> Dict:
        """
        Generate machine-readable dictionary report.

        Useful for JSON export or programmatic consumption.
        """
        report = {
            'mode': self.mode.value,
            'counts': self.counts,
            'risk_summary': self.risk_summary,
            'warnings': self.warnings,
            'manual_steps': self.manual_steps,
            # Legacy fields for backward compatibility
            'total_objects': self.counts['detected'],
            'objects_by_type': self.counts.get('by_type', {})
        }

        # Add dialect info if provided
        if self.source_dialect:
            report['source_dialect'] = self.source_dialect
        if self.target_dialect:
            report['target_dialect'] = self.target_dialect

        return report


def generate_report(results: List[TranslationResult], mode: TranslateMode, source_dialect: str = None, target_dialect: str = None) -> TranslationReport:
    """
    Generate a translation report from results.

    Args:
        results: List of TranslationResult objects
        mode: Translation mode used
        source_dialect: Source database dialect (optional)
        target_dialect: Target database dialect (optional)

    Returns:
        TranslationReport object with summary and checklists
    """
    return TranslationReport(results, mode, source_dialect, target_dialect)
