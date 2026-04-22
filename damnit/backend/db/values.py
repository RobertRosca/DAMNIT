"""Typed value encoding/decoding for DAMNIT's run_variables table."""
import io
import struct
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

import numpy as np


class ValueKind(str, Enum):
    null = "null"
    int_ = "int"
    float_ = "float"
    str_ = "str"
    bool_ = "bool"
    complex_ = "complex"
    numpy = "numpy"
    png = "png"
    trendline = "trendline"
    timestamp = "timestamp"
    error = "error"


class SummaryType(str, Enum):
    # Not exhaustive - only types the code currently reasons about.
    timestamp = "timestamp"
    complex = "complex"
    numpy = "numpy"
    trendline = "trendline"


class BlobTypes(Enum):
    """Legacy blob identifier kept for detecting raw PNG/numpy bytes."""
    png = "png"
    numpy = "numpy"
    unknown = "unknown"

    @classmethod
    def identify(cls, blob: bytes):
        if not isinstance(blob, (bytes, bytearray, memoryview)):
            return cls.unknown
        if blob[:8] == b"\x89PNG\r\n\x1a\n":
            return cls.png
        if blob[:6] == b"\x93NUMPY":
            return cls.numpy
        return cls.unknown


@dataclass
class ReducedData:
    """Container for a summary value plus metadata produced by the context runner."""
    value: Any
    max_diff: Optional[float] = None
    summary_method: str = ""
    summary_type: Optional[str] = None
    attributes: Optional[dict] = None


@dataclass
class EncodedValue:
    """A ReducedData after mapping to typed columns of run_variables."""
    kind: ValueKind
    num: Optional[float] = None
    text: Optional[str] = None
    bool_: Optional[bool] = None
    bytes_: Optional[bytes] = None
    summary_type: Optional[str] = None
    summary_method: str = ""
    max_diff: Optional[float] = None
    attributes: Optional[dict] = None


def complex2blob(data: complex) -> bytes:
    return struct.pack("<dd", data.real, data.imag)


def blob2complex(data: bytes) -> complex:
    real, imag = struct.unpack("<dd", data)
    return complex(real, imag)


def numpy2blob(arr: np.ndarray) -> bytes:
    buf = io.BytesIO()
    np.save(buf, arr, allow_pickle=False)
    return buf.getvalue()


def blob2numpy(data: bytes) -> np.ndarray:
    return np.load(io.BytesIO(data), allow_pickle=False)


def _is_png(value) -> bool:
    return isinstance(value, (bytes, bytearray)) and bytes(value[:8]) == b"\x89PNG\r\n\x1a\n"


def encode_value(reduced: ReducedData) -> EncodedValue:
    """Map a ReducedData onto typed columns for run_variables."""
    value = reduced.value
    summary_type = reduced.summary_type or None
    summary_method = reduced.summary_method or ""
    max_diff = reduced.max_diff
    attributes = reduced.attributes if reduced.attributes else None

    # Error path: ReducedData(None, attributes={'error': ..., 'error_cls': ...}).
    if value is None and attributes and "error" in attributes:
        return EncodedValue(
            kind=ValueKind.error,
            text=str(attributes.get("error", "")),
            summary_type=summary_type,
            summary_method=summary_method,
            attributes=attributes,
        )

    if value is None:
        return EncodedValue(
            kind=ValueKind.null,
            summary_type=summary_type,
            summary_method=summary_method,
            max_diff=max_diff,
            attributes=attributes,
        )

    if isinstance(value, bool):
        return EncodedValue(
            kind=ValueKind.bool_, bool_=bool(value),
            summary_type=summary_type, summary_method=summary_method,
            max_diff=max_diff, attributes=attributes,
        )

    if isinstance(value, int) and not isinstance(value, bool):
        return EncodedValue(
            kind=ValueKind.int_, num=float(value),
            summary_type=summary_type, summary_method=summary_method,
            max_diff=max_diff, attributes=attributes,
        )

    if isinstance(value, float):
        return EncodedValue(
            kind=ValueKind.float_, num=float(value),
            summary_type=summary_type, summary_method=summary_method,
            max_diff=max_diff, attributes=attributes,
        )

    if isinstance(value, complex):
        return EncodedValue(
            kind=ValueKind.complex_, bytes_=complex2blob(value),
            summary_type=summary_type or SummaryType.complex.value,
            summary_method=summary_method, max_diff=max_diff, attributes=attributes,
        )

    if isinstance(value, np.ndarray):
        if value.dtype.hasobject:
            raise TypeError("Unsupported array dtype for database storage")
        kind = ValueKind.trendline if summary_type == "trendline" else ValueKind.numpy
        return EncodedValue(
            kind=kind, bytes_=numpy2blob(value),
            summary_type=summary_type or SummaryType.numpy.value,
            summary_method=summary_method, max_diff=max_diff, attributes=attributes,
        )

    if isinstance(value, (bytes, bytearray)) and _is_png(value):
        return EncodedValue(
            kind=ValueKind.png, bytes_=bytes(value),
            summary_type=summary_type, summary_method=summary_method,
            max_diff=max_diff, attributes=attributes,
        )

    if isinstance(value, datetime):
        return EncodedValue(
            kind=ValueKind.timestamp, num=value.timestamp(),
            summary_type=summary_type or SummaryType.timestamp.value,
            summary_method=summary_method, max_diff=max_diff, attributes=attributes,
        )

    if isinstance(value, str):
        return EncodedValue(
            kind=ValueKind.str_, text=value,
            summary_type=summary_type, summary_method=summary_method,
            max_diff=max_diff, attributes=attributes,
        )

    raise TypeError(f"Unsupported type for database: {type(value).__name__}")


def decode_value(
    kind: str,
    *,
    num: Optional[float] = None,
    text: Optional[str] = None,
    bool_: Optional[bool] = None,
    bytes_: Optional[bytes] = None,
    summary_type: Optional[str] = None,
) -> Any:
    """Inverse of :func:`encode_value`. Returns a Python-native value."""
    k = ValueKind(kind) if not isinstance(kind, ValueKind) else kind

    if k is ValueKind.null:
        return None
    if k is ValueKind.bool_:
        return bool_
    if k is ValueKind.int_:
        return int(num) if num is not None else None
    if k is ValueKind.float_:
        return num
    if k is ValueKind.str_ or k is ValueKind.error:
        return text
    if k is ValueKind.complex_:
        return blob2complex(bytes(bytes_))
    if k is ValueKind.numpy or k is ValueKind.trendline:
        return blob2numpy(bytes(bytes_))
    if k is ValueKind.png:
        return bytes(bytes_)
    if k is ValueKind.timestamp:
        if num is None:
            return None
        return datetime.fromtimestamp(num, tz=timezone.utc)
    raise ValueError(f"Unknown value kind {k!r}")
