"""Normalise chuk-mcp response objects to plain dicts.

chuk-mcp results have historically been Pydantic models (``.model_dump()`` /
``.dict()``). The Rust-backed chuk-mcp (>=0.10) returns PyO3 objects that expose
``.to_dict()`` instead. The transports normalise either form to a dict before
their dict-oriented handling, so both the legacy pure-Python and the modern
Rust-backed chuk-mcp work unchanged.
"""

from __future__ import annotations

from typing import Any

# Pydantic v2's ``model_dump`` then v1's ``dict`` first (real Rust objects have
# neither, so they fall through to ``to_dict``). Order only matters for mocks
# that expose several of these; real chuk-mcp types expose exactly one.
_DICT_METHODS = ("model_dump", "dict", "to_dict")


def to_plain_dict(obj: Any) -> Any:
    """Return ``obj`` as a dict if it is (or can produce) one, else ``obj``."""
    if isinstance(obj, dict):
        return obj
    for name in _DICT_METHODS:
        method = getattr(obj, name, None)
        if callable(method):
            try:
                return method()
            except Exception:  # noqa: BLE001 - fall through to the next form
                continue
    return obj
