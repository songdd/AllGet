"""Bencode encoder/decoder for BitTorrent protocol."""

from typing import Any, Union


class BencodeError(Exception):
    """Error during bencode operations."""
    pass


def decode(data: bytes, start: int = 0) -> tuple[Any, int]:
    """Decode a bencoded value starting at offset, return (value, next_offset)."""
    if start >= len(data):
        raise BencodeError("Unexpected end of data")

    ch = chr(data[start])

    if ch == 'i':
        # Integer: i<number>e
        end = data.index(b'e', start)
        num_str = data[start + 1:end]
        if not num_str:
            raise BencodeError("Empty integer")
        if num_str == b'-0':
            raise BencodeError("Invalid integer: -0")
        if num_str[0:1] == b'0' and len(num_str) > 1:
            raise BencodeError("Leading zero in integer")
        if num_str[0:1] == b'-' and num_str[1:2] == b'0' and len(num_str) > 2:
            raise BencodeError("Leading zero in negative integer")
        return int(num_str), end + 1

    elif ch == 'l':
        # List: l<values>e
        result = []
        pos = start + 1
        while pos < len(data) and data[pos:pos + 1] != b'e':
            value, pos = decode(data, pos)
            result.append(value)
        if pos >= len(data):
            raise BencodeError("Unterminated list")
        return result, pos + 1

    elif ch == 'd':
        # Dictionary: d<keyvaluepairs>e
        result = {}
        pos = start + 1
        while pos < len(data) and data[pos:pos + 1] != b'e':
            key, pos = decode(data, pos)
            if not isinstance(key, bytes):
                raise BencodeError(f"Dictionary key must be bytes, got {type(key)}")
            value, pos = decode(data, pos)
            result[key] = value
        if pos >= len(data):
            raise BencodeError("Unterminated dictionary")
        return result, pos + 1

    elif ch.isdigit():
        # String: <length>:<bytes>
        colon = data.index(b':', start)
        length = int(data[start:colon])
        end = colon + 1 + length
        if end > len(data):
            raise BencodeError("String extends past data")
        return data[colon + 1:end], end

    else:
        raise BencodeError(f"Unexpected character: {ch!r} at offset {start}")


def decode_all(data: bytes) -> Any:
    """Decode a complete bencoded value, checking no trailing data."""
    value, end = decode(data)
    if end != len(data):
        raise BencodeError(f"Trailing data after offset {end}")
    return value


def encode(value: Union[int, bytes, str, list, dict]) -> bytes:
    """Encode a value to bencoded bytes."""
    if isinstance(value, int):
        return b'i' + str(value).encode() + b'e'
    elif isinstance(value, bytes):
        return str(len(value)).encode() + b':' + value
    elif isinstance(value, str):
        encoded = value.encode('utf-8')
        return str(len(encoded)).encode() + b':' + encoded
    elif isinstance(value, list):
        parts = [b'l']
        for item in value:
            parts.append(encode(item))
        parts.append(b'e')
        return b''.join(parts)
    elif isinstance(value, dict):
        parts = [b'd']
        for key in sorted(value.keys(), key=lambda k: k if isinstance(k, bytes) else k.encode('utf-8')):
            parts.append(encode(key))
            parts.append(encode(value[key]))
        parts.append(b'e')
        return b''.join(parts)
    else:
        raise BencodeError(f"Cannot encode type: {type(value)}")

