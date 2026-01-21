#!/usr/bin/env python3
"""
SAIQL Community Edition — Edition Checks and Error Codes

This module provides:
- Edition identification
- Feature availability checks
- Deterministic error codes for disabled features
"""

from typing import Set

# Edition constants
EDITION = "ce"
EDITION_NAME = "Community Edition"
VERSION = "1.0.0-ce"

# Error codes (stable, deterministic)
E_CE_NOT_SHIPPED = "E_CE_NOT_SHIPPED"
E_CE_FEATURE_DISABLED = "E_CE_FEATURE_DISABLED"

# Features NOT available in CE
REMOVED_FEATURES: Set[str] = {
    "atlas",
    "qipi",
    "loretokens",
    "lore_core",
    "lore_chunk",
    "imagination",
    "copilot_carl",
    "rag",
    "ocr",
    "vision",
    "training",
}

# Features available in CE
CE_FEATURES: Set[str] = {
    "core_engine",
    "db_adapters",
    "migration",
    "translation",
    "proof_bundles",
    "loretoken_lite",
    "qipi_lite",
    "atlas_ce",      # CE RAG system
    "grounding_ce",  # CE mode grounding (informational)
}


class CENotShippedError(Exception):
    """
    Raised when accessing a feature not shipped in CE.

    This error indicates the feature was intentionally removed
    from Community Edition.
    """
    def __init__(self, feature: str):
        self.feature = feature
        self.code = E_CE_NOT_SHIPPED
        super().__init__(
            f"{E_CE_NOT_SHIPPED}: '{feature}' is not available in "
            f"SAIQL {EDITION_NAME}. This feature requires the full version."
        )


class CEFeatureDisabledError(Exception):
    """
    Raised when accessing a disabled CE feature.

    This error indicates the feature exists but is disabled
    in the current configuration.
    """
    def __init__(self, feature: str, reason: str = ""):
        self.feature = feature
        self.code = E_CE_FEATURE_DISABLED
        msg = f"{E_CE_FEATURE_DISABLED}: '{feature}' is disabled"
        if reason:
            msg += f" ({reason})"
        super().__init__(msg)


def check_feature(feature: str) -> bool:
    """
    Check if a feature is available in CE.

    Args:
        feature: Feature name to check

    Returns:
        True if available

    Raises:
        CENotShippedError: If feature is not shipped in CE
    """
    if feature.lower() in REMOVED_FEATURES:
        raise CENotShippedError(feature)
    return True


def is_ce() -> bool:
    """Return True if running Community Edition."""
    return EDITION == "ce"


def get_edition_info() -> dict:
    """
    Get edition information.

    Returns:
        Dictionary with edition details
    """
    return {
        "edition": EDITION,
        "edition_name": EDITION_NAME,
        "version": VERSION,
        "features_enabled": sorted(CE_FEATURES),
        "features_disabled": sorted(REMOVED_FEATURES),
    }


def edition_banner() -> str:
    """
    Get edition banner for CLI display.

    Returns:
        Formatted banner string
    """
    return f"""SAIQL {VERSION}
Edition: {EDITION_NAME}
Features: Core Engine, DB Adapters, Migration, Translation, Atlas CE, Grounding CE
Disabled: Full Atlas, QIPI, LoreToken (use *-Lite/CE variants)"""


# Export public API
__all__ = [
    'EDITION',
    'EDITION_NAME',
    'VERSION',
    'E_CE_NOT_SHIPPED',
    'E_CE_FEATURE_DISABLED',
    'REMOVED_FEATURES',
    'CE_FEATURES',
    'CENotShippedError',
    'CEFeatureDisabledError',
    'check_feature',
    'is_ce',
    'get_edition_info',
    'edition_banner',
]
