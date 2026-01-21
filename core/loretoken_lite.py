#!/usr/bin/env python3
"""
LoreToken-Lite — CE-safe payload container format

Provides:
- Canonical JSON schema with stable key ordering
- SHA256 hashing with explicit algorithm field
- Pack/unpack for serialization
- Validation against schema

NOT included (CE restrictions):
- Semantic compression
- GPU acceleration
- Proprietary encoders

File extension: .lorelite
"""

import json
import hashlib
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path


# Schema version for compatibility
SCHEMA_VERSION = "1.0"
HASH_ALGO = "sha256"
FILE_EXTENSION = ".lorelite"


class LoreTokenLiteError(Exception):
    """Base exception for LoreToken-Lite operations."""
    pass


class ValidationError(LoreTokenLiteError):
    """Raised when validation fails."""
    pass


class SchemaVersionError(LoreTokenLiteError):
    """Raised when schema version is incompatible."""
    pass


@dataclass
class LoreTokenLite:
    """
    LoreToken-Lite container format.

    Attributes:
        payload: The main data payload (dict)
        metadata: Optional metadata (dict)
        schema_version: Schema version string
        hash_algo: Hash algorithm used
        hash: Computed hash of canonical payload
    """
    payload: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    schema_version: str = SCHEMA_VERSION
    hash_algo: str = HASH_ALGO
    hash: str = ""

    def __post_init__(self):
        """Compute hash after initialization if not set."""
        if not self.hash:
            self.hash = self._compute_hash()

    def _compute_hash(self) -> str:
        """Compute deterministic hash of payload."""
        return stable_hash(self.payload)

    def validate(self) -> bool:
        """
        Validate the token's integrity.

        Returns:
            True if valid

        Raises:
            ValidationError: If validation fails
        """
        # Check schema version
        if self.schema_version != SCHEMA_VERSION:
            raise SchemaVersionError(
                f"Unsupported schema version: {self.schema_version} (expected {SCHEMA_VERSION})"
            )

        # Check hash algorithm
        if self.hash_algo != HASH_ALGO:
            raise ValidationError(
                f"Unsupported hash algorithm: {self.hash_algo} (expected {HASH_ALGO})"
            )

        # Verify hash
        computed = self._compute_hash()
        if self.hash != computed:
            raise ValidationError(
                f"Hash mismatch: stored={self.hash}, computed={computed}"
            )

        # Check payload is dict
        if not isinstance(self.payload, dict):
            raise ValidationError("Payload must be a dictionary")

        return True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "schema_version": self.schema_version,
            "hash_algo": self.hash_algo,
            "hash": self.hash,
            "payload": self.payload,
            "metadata": self.metadata,
        }


def stable_hash(payload: Dict[str, Any]) -> str:
    """
    Compute deterministic SHA256 hash of a payload.

    Uses canonical JSON (sorted keys, no extra whitespace, UTF-8).

    Args:
        payload: Dictionary to hash

    Returns:
        Hex-encoded SHA256 hash
    """
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def pack(payload: Dict[str, Any],
         metadata: Optional[Dict[str, Any]] = None) -> bytes:
    """
    Pack a payload into LoreToken-Lite format.

    Args:
        payload: Data to pack
        metadata: Optional metadata

    Returns:
        UTF-8 encoded canonical JSON bytes
    """
    token = LoreTokenLite(
        payload=payload,
        metadata=metadata or {}
    )

    # Canonical JSON output
    output = json.dumps(
        token.to_dict(),
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=False
    )

    return output.encode('utf-8')


def unpack(data: bytes) -> LoreTokenLite:
    """
    Unpack LoreToken-Lite data.

    Args:
        data: UTF-8 encoded JSON bytes

    Returns:
        LoreTokenLite instance

    Raises:
        ValidationError: If data is invalid
    """
    try:
        obj = json.loads(data.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValidationError(f"Invalid LoreToken-Lite data: {e}")

    # Required fields
    required = ['schema_version', 'hash_algo', 'hash', 'payload']
    for field in required:
        if field not in obj:
            raise ValidationError(f"Missing required field: {field}")

    token = LoreTokenLite(
        payload=obj['payload'],
        metadata=obj.get('metadata', {}),
        schema_version=obj['schema_version'],
        hash_algo=obj['hash_algo'],
        hash=obj['hash']
    )

    # Validate integrity
    token.validate()

    return token


def validate(data: bytes) -> bool:
    """
    Validate LoreToken-Lite data without fully unpacking.

    Args:
        data: UTF-8 encoded JSON bytes

    Returns:
        True if valid

    Raises:
        ValidationError: If validation fails
    """
    token = unpack(data)
    return token.validate()


def save(token: LoreTokenLite, path: Path) -> None:
    """
    Save LoreToken-Lite to file.

    Args:
        token: Token to save
        path: Output path (will add .lorelite extension if missing)
    """
    if not str(path).endswith(FILE_EXTENSION):
        path = Path(str(path) + FILE_EXTENSION)

    data = pack(token.payload, token.metadata)
    path.write_bytes(data)


def load(path: Path) -> LoreTokenLite:
    """
    Load LoreToken-Lite from file.

    Args:
        path: File path

    Returns:
        LoreTokenLite instance
    """
    data = path.read_bytes()
    return unpack(data)


# Export public API
__all__ = [
    'LoreTokenLite',
    'LoreTokenLiteError',
    'ValidationError',
    'SchemaVersionError',
    'stable_hash',
    'pack',
    'unpack',
    'validate',
    'save',
    'load',
    'SCHEMA_VERSION',
    'HASH_ALGO',
    'FILE_EXTENSION',
]
