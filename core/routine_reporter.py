from typing import List
from .routine_translator import TranslationResult

class RoutineReporter:
    @staticmethod
    def generate_markdown_report(results: List[TranslationResult]) -> str:
        lines = []
        lines.append("# Routine Migration Report")
        lines.append("")
        
        # Summary
        lines.append("## Summary")
        lines.append("| Name | Outcome | Warnings | Errors |")
        lines.append("| :--- | :--- | :--- | :--- |")
        
        for res in results:
            warn_count = len(res.warnings)
            err_count = len(res.errors)
            lines.append(f"| `{res.routine_name}` | {res.outcome} | {warn_count} | {err_count} |")
            
        lines.append("")
        
        # Details
        lines.append("## Details")
        for res in results:
            lines.append(f"### {res.routine_name}")
            lines.append(f"- **Outcome**: {res.outcome}")
            lines.append(f"- **Risk Score**: {res.original_ir.risk_score}")
            
            if res.warnings:
                lines.append("**Warnings:**")
                for w in res.warnings:
                    lines.append(f"- [WARN] {w}")
            
            if res.errors:
                lines.append("**Errors:**")
                for e in res.errors:
                    lines.append(f"- [ERR] {e}")
            
            if res.generated_code:
                lines.append("")
                lines.append("```sql")
                lines.append(res.generated_code)
                lines.append("```")
                
            lines.append("---")
            
        return "\n".join(lines)
