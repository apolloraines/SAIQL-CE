
import re
import json
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

@dataclass
class FirewallDecision:
    action: str  # "ALLOW", "BLOCK", "REDACT"
    reasons: List[str]
    confidence: float
    redactions: Optional[List[Dict[str, Any]]] = None
    modified_text: Optional[str] = None

class SemanticFirewall:
    """
    Semantic Firewall v1

    Provides security guardrails for:
    - Prompt Injection
    - System Prompt Extraction
    - Secret Exfiltration
    - Tool Abuse

    SECURITY: Fails CLOSED - if rules cannot be loaded, all guards BLOCK by default.
    """

    def __init__(self, config_path: Optional[str] = None):
        self.rules = {}
        self.settings = {}
        self._rules_loaded = False  # Track if rules were successfully loaded
        self._load_config(config_path)

    def _load_config(self, config_path: Optional[str]):
        """Load rules from JSON config.

        If loading fails, _rules_loaded remains False and guards will fail-closed (BLOCK).
        """
        if not config_path:
            # Default path relative to this file
            base_dir = Path(__file__).resolve().parent.parent
            config_path = str(base_dir / "config" / "semantic_firewall_rules.json")

        try:
            with open(config_path, "r") as f:
                config = json.load(f)
                self.rules = config.get("rules", {})
                self.settings = config.get("settings", {})
            self._rules_loaded = True
            logger.info(f"Loaded Semantic Firewall rules from {config_path}")
        except Exception as e:
            logger.error(f"Failed to load firewall rules: {e}. Firewall will BLOCK all requests (fail-closed).")
            # Fail-closed: rules not loaded means guards will block
            self.rules = {}
            self.settings = {}
            self._rules_loaded = False

    def _check_patterns(self, text: str, rule_category: str) -> List[Dict[str, Any]]:
        """Check text against regex patterns for a category"""
        matches = []
        if rule_category not in self.rules:
            return matches
            
        for rule in self.rules[rule_category]:
            pattern = rule["pattern"]
            if re.search(pattern, text):
                matches.append(rule)
        return matches

    def pre_prompt_guard(self, user_text: str, context: Optional[Dict] = None) -> FirewallDecision:
        """Guard against malicious input prompts"""
        # Fail-closed: if rules weren't loaded, block everything
        if not self._rules_loaded:
            return FirewallDecision(
                action="BLOCK",
                reasons=["Firewall rules not loaded - fail-closed policy"],
                confidence=1.0
            )

        reasons = []

        # 1. Check Injection
        inj_matches = self._check_patterns(user_text, "injection")
        if inj_matches:
            reasons.extend([m["reason"] for m in inj_matches])
            return FirewallDecision(action="BLOCK", reasons=reasons, confidence=1.0)
            
        # 2. Check System Prompt Extraction
        sys_matches = self._check_patterns(user_text, "system_prompt")
        if sys_matches:
            reasons.extend([m["reason"] for m in sys_matches])
            return FirewallDecision(action="BLOCK", reasons=reasons, confidence=1.0)
            
        # 3. Check Tool Abuse
        tool_matches = self._check_patterns(user_text, "tool_abuse")
        if tool_matches:
            reasons.extend([m["reason"] for m in tool_matches])
            return FirewallDecision(action="BLOCK", reasons=reasons, confidence=1.0)
            
        return FirewallDecision(action="ALLOW", reasons=[], confidence=0.0)

    def scan_text(self, text: str) -> FirewallDecision:
        """Convenience wrapper used in tests/tools."""
        return self.pre_prompt_guard(text)

    def pre_retrieval_guard(self, query_text: str, context: Optional[Dict] = None) -> FirewallDecision:
        """Guard against malicious retrieval queries"""
        # Similar to prompt guard, but maybe stricter or different rules?
        # For now, reuse injection rules as retrieval injection is similar.
        return self.pre_prompt_guard(query_text, context)

    def post_output_guard(self, output_text: str, context: Optional[Dict] = None) -> FirewallDecision:
        """Guard against data leakage in output"""
        # Fail-closed: if rules weren't loaded, block output
        if not self._rules_loaded:
            return FirewallDecision(
                action="BLOCK",
                reasons=["Firewall rules not loaded - fail-closed policy"],
                confidence=1.0
            )

        reasons = []
        redactions = []
        modified_text = output_text

        # Check Secrets
        secret_matches = self._check_patterns(output_text, "secrets")
        if secret_matches:
            for rule in secret_matches:
                reasons.append(rule["reason"])
                # Perform redaction
                replacement = rule.get("replacement", self.settings.get("redaction_placeholder", "[REDACTED]"))
                # Track what was redacted
                redactions.append({
                    "pattern": rule["pattern"],
                    "reason": rule["reason"],
                    "replacement": replacement
                })
                modified_text = re.sub(rule["pattern"], replacement, modified_text)

            return FirewallDecision(
                action="REDACT",
                reasons=reasons,
                confidence=1.0,
                redactions=redactions,
                modified_text=modified_text
            )

        return FirewallDecision(action="ALLOW", reasons=[], confidence=0.0)
