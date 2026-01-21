from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
import re

class AuditBundleGenerator:
    """Generates the audit report for a migration run."""
    
    def __init__(self, run_dir: Path, source_url: str, target_url: str):
        self.run_dir = run_dir
        self.reports_dir = run_dir / "reports"
        self.source_url = self._sanitize_url(source_url)
        self.target_url = self._sanitize_url(target_url)
        self.start_time = datetime.now()
        
        # Tracking Data
        self.converted_objects: List[Dict] = []  # {name, rows, status, duration}
        self.warnings: List[str] = []
        self.skipped_objects: List[Dict] = [] # {name, reason}
        self.manual_steps: List[str] = []
        
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    # Known secret parameter names to redact in query strings and DSN formats
    _SECRET_KEYS = {'password', 'passwd', 'pwd', 'secret', 'token', 'api_key',
                    'apikey', 'auth', 'credential', 'credentials', 'key'}

    def _sanitize_url(self, url: str) -> str:
        """Sanitize URL to hide credentials in various formats.

        Handles:
        - Standard URI: schema://user:pass@host
        - Query params: ?password=secret
        - DSN strings: host=localhost password=secret
        """
        if not url:
            return "N/A"
        try:
            # Try parsing as standard URL first
            parsed = urlparse(url)

            if parsed.scheme and parsed.netloc:
                # Standard URL format - redact userinfo and query params
                netloc = parsed.netloc
                if '@' in netloc:
                    # Remove user:pass from netloc
                    netloc = '***:***@' + netloc.split('@')[-1]

                # Redact secret query parameters
                if parsed.query:
                    params = parse_qs(parsed.query, keep_blank_values=True)
                    for key in list(params.keys()):
                        if key.lower() in self._SECRET_KEYS:
                            params[key] = ['***']
                    query = urlencode(params, doseq=True)
                else:
                    query = ''

                return urlunparse((parsed.scheme, netloc, parsed.path,
                                   parsed.params, query, parsed.fragment))

            # Handle DSN-style strings: key=value key2=value2
            # Redact any key that looks like a secret
            result = url
            for key in self._SECRET_KEYS:
                # Match key=value patterns (space or end of string terminated)
                pattern = rf'({key}\s*=\s*)([^\s]+)'
                result = re.sub(pattern, r'\1***', result, flags=re.IGNORECASE)

            # Also redact embedded URIs with userinfo (e.g., CONNECTION_STRING=postgres://user:pass@host)
            # Pattern: scheme://user:pass@host -> scheme://***:***@host
            result = re.sub(
                r'(\w+://)[^/:]+:[^/@]+@',
                r'\1***:***@',
                result
            )

            return result
        except Exception:
            # Fail closed - don't leak potentially sensitive data
            return "[redacted]"

    def log_conversion(self, table: str, rows: int, status: str = "Success", duration_ms: float = 0):
        self.converted_objects.append({
            "table": table,
            "rows": rows,
            "status": status,
            "duration": f"{duration_ms:.2f}ms"
        })

    def log_warning(self, message: str):
        self.warnings.append(message)

    def log_skipped(self, name: str, reason: str):
        self.skipped_objects.append({"name": name, "reason": reason})
        
    def add_manual_step(self, step: str):
        self.manual_steps.append(step)

    def generate_report(self) -> Path:
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        report_path = self.reports_dir / "audit_report.md"
        
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(f"# Migration Audit Report\n\n")
            
            # Summary
            f.write(f"## Summary\n")
            f.write(f"- **Run ID**: {self.run_dir.name}\n")
            f.write(f"- **Date**: {self.start_time.isoformat()}\n")
            f.write(f"- **Duration**: {duration}\n")
            f.write(f"- **Source**: {self.source_url}\n")
            f.write(f"- **Target**: {self.target_url}\n\n")
            
            # Objects
            f.write(f"## Objects Converted\n")
            f.write(f"| Table | Rows | Status | Duration |\n")
            f.write(f"|-------|------|--------|----------|\n")
            for obj in self.converted_objects:
                status_icon = "✅" if obj['status'] == 'Success' else "❌"
                f.write(f"| {obj['table']} | {obj['rows']} | {status_icon} {obj['status']} | {obj['duration']} |\n")
            f.write("\n")
            
            # Warnings
            if self.warnings:
                f.write(f"## Warnings ({len(self.warnings)})\n")
                for w in self.warnings:
                    f.write(f"- ⚠️ {w}\n")
                f.write("\n")
            
            # Skipped
            if self.skipped_objects:
                f.write(f"## Skipped Objects ({len(self.skipped_objects)})\n")
                for s in self.skipped_objects:
                    f.write(f"- **{s['name']}**: {s['reason']}\n")
                f.write("\n")
                
            # Manual Steps
            f.write(f"## Manual Steps Required\n")
            if self.manual_steps:
               for step in self.manual_steps:
                   f.write(f"- [ ] {step}\n")
            else:
               f.write("- [ ] Verify row counts match source\n")
               f.write("- [ ] Check application connectivity\n")
               
        return report_path
