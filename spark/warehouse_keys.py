"""Stable, domain-separated keys for warehouse dimensions."""

from __future__ import annotations

import hashlib
import hmac


def pseudonymize_identifier(
    entity: str,
    identifier: int | str,
    secret: str,
) -> str:
    """Return a stable HMAC key without exposing the source identifier."""
    if not secret:
        raise ValueError("The HMAC secret must not be empty.")
    message = str(identifier) if entity == "student" else f"{entity}:{identifier}"
    return hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
