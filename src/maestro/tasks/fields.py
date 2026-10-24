"""Typed reads from untrusted JSON-like data; each raises ValueError naming the missing or mistyped field."""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

Fields = Mapping[str, object]


class FieldError(ValueError, TypeError):
    """A missing or mistyped field: a bad value (ValueError) of the wrong type (TypeError)."""


def mapping(value: object, label: str) -> dict[str, object]:
    """`value` as a string-keyed object."""
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in cast(dict[object, object], value)):
        raise FieldError(f'{label} must be a JSON object')
    return cast(dict[str, object], value)


def parse_object(text: str, label: str) -> dict[str, object]:
    """The JSON object encoded in `text`."""
    try:
        decoded = cast(object, json.loads(text))
    except ValueError as error:
        raise FieldError(f'{label} is not JSON') from error
    return mapping(decoded, label)


def text(fields: Fields, key: str) -> str:
    """A string field."""
    value = fields.get(key)
    if not isinstance(value, str):
        raise FieldError(f'{key} must be a string')
    return value


def integer(fields: Fields, key: str) -> int:
    """An integer field; booleans are rejected."""
    value = fields.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise FieldError(f'{key} must be an integer')
    return value


def number(fields: Fields, key: str) -> float:
    """A numeric field; booleans are rejected."""
    value = fields.get(key)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise FieldError(f'{key} must be a number')
    return float(value)


def flag(fields: Fields, key: str) -> bool:
    """A boolean field."""
    value = fields.get(key)
    if not isinstance(value, bool):
        raise FieldError(f'{key} must be a boolean')
    return value


def objects(fields: Fields, key: str) -> list[dict[str, object]]:
    """A list-of-objects field."""
    value = fields.get(key)
    if not isinstance(value, list):
        raise FieldError(f'{key} must be a list')
    return [mapping(item, f'{key}[{index}]') for index, item in enumerate(cast(list[object], value))]


def section(fields: Fields, key: str) -> dict[str, object]:
    """A nested object field; absent means empty."""
    value = fields.get(key, {})
    return mapping(value, key)
