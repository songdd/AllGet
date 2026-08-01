"""Torrent metainfo file parsing."""

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import unquote

from .bencode import decode_all, decode


@dataclass
class TorrentFile:
    """A single file within a torrent."""
    path: str          # Relative path within torrent
    length: int        # File size in bytes


@dataclass
class TorrentMeta:
    """Parsed torrent metainfo."""
    info_hash: bytes                     # SHA1 hash of info dict
    name: str                            # Torrent name
    piece_length: int                    # Size of each piece
    pieces: bytes                        # Concatenated 20-byte SHA1 hashes
    total_size: int                      # Total size in bytes
    files: list[TorrentFile] = field(default_factory=list)
    announce: str = ""                   # Primary tracker URL
    announce_list: list[list[str]] = field(default_factory=list)  # Tiered trackers
    comment: str = ""
    created_by: str = ""
    info_data: bytes = b""               # Raw info dict bytes, for hash verification

    @property
    def piece_count(self) -> int:
        return len(self.pieces) // 20

    @property
    def is_single_file(self) -> bool:
        return len(self.files) == 1

    def piece_hashes(self) -> list[bytes]:
        """Return list of piece hashes (20 bytes each)."""
        return [self.pieces[i:i + 20] for i in range(0, len(self.pieces), 20)]

    def piece_index_for_offset(self, offset: int) -> int:
        """Return the piece index that contains the given byte offset."""
        idx = offset // self.piece_length
        return min(idx, self.piece_count - 1)

    def piece_size(self, index: int) -> int:
        """Return the size of piece at index (last piece may be shorter)."""
        if index < self.piece_count - 1:
            return self.piece_length
        remainder = self.total_size - (self.piece_count - 1) * self.piece_length
        return remainder

    def file_for_offset(self, offset: int) -> tuple[TorrentFile, int]:
        """Find which file contains offset, return (file, offset_within_file)."""
        current = 0
        for f in self.files:
            if offset < current + f.length:
                return f, offset - current
            current += f.length
        raise ValueError(f"Offset {offset} beyond total size {self.total_size}")


def parse_torrent_file(path: str) -> TorrentMeta:
    """Parse a .torrent file from disk."""
    with open(path, 'rb') as f:
        return parse_torrent_data(f.read())


def parse_torrent_data(data: bytes) -> TorrentMeta:
    """Parse torrent metainfo from raw bytes."""
    root = decode_all(data)
    if not isinstance(root, dict):
        raise ValueError("Invalid torrent file: root must be a dictionary")

    info = root.get(b'info')
    if not isinstance(info, dict):
        raise ValueError("Invalid torrent file: missing or invalid 'info' dict")

    # Compute info_hash from the raw info dict bytes
    from .bencode import encode
    info_data = encode(info)
    info_hash = hashlib.sha1(info_data).digest()

    # Parse name
    name = info.get(b'name', b'unknown')
    if isinstance(name, bytes):
        try:
            name = name.decode('utf-8')
        except UnicodeDecodeError:
            name = name.decode('utf-8', errors='replace')

    piece_length = info.get(b'piece length', 0)
    pieces = info.get(b'pieces', b'')

    files: list[TorrentFile] = []
    total_size = 0

    if b'files' in info:
        for file_entry in info[b'files']:
            if not isinstance(file_entry, dict):
                continue
            length = file_entry.get(b'length', 0)
            path_parts = file_entry.get(b'path', [])
            if isinstance(path_parts, list):
                file_path = '/'.join(
                    p.decode('utf-8', errors='replace') if isinstance(p, bytes) else str(p)
                    for p in path_parts
                )
            else:
                file_path = 'unknown'
            files.append(TorrentFile(path=f"{name}/{file_path}", length=length))
            total_size += length
    else:
        length = info.get(b'length', 0)
        files.append(TorrentFile(path=name, length=length))
        total_size = length

    # Announce
    announce = root.get(b'announce', b'')
    if isinstance(announce, bytes):
        announce = announce.decode('utf-8', errors='replace')

    announce_list: list[list[str]] = []
    raw_al = root.get(b'announce-list')
    if isinstance(raw_al, list):
        for tier in raw_al:
            if isinstance(tier, list):
                announce_list.append([
                    t.decode('utf-8', errors='replace') if isinstance(t, bytes) else str(t)
                    for t in tier
                ])

    comment = root.get(b'comment', b'')
    if isinstance(comment, bytes):
        comment = comment.decode('utf-8', errors='replace')

    created_by = root.get(b'created by', b'')
    if isinstance(created_by, bytes):
        created_by = created_by.decode('utf-8', errors='replace')

    return TorrentMeta(
        info_hash=info_hash,
        name=name,
        piece_length=piece_length,
        pieces=pieces,
        total_size=total_size,
        files=files,
        announce=announce,
        announce_list=announce_list,
        comment=comment,
        created_by=created_by,
        info_data=info_data,
    )


def parse_magnet_link(uri: str) -> tuple[bytes, str, list[list[str]]]:
    """Parse a magnet link, return (info_hash, display_name, trackers).

    Supports magnet:?xt=urn:btih:<hash>&dn=<name>&tr=<tracker>
    """
    from urllib.parse import parse_qs, urlparse

    parsed = urlparse(uri)
    if parsed.scheme != 'magnet':
        raise ValueError(f"Not a magnet link: {uri[:60]}")

    params = parse_qs(parsed.query)

    xt = params.get('xt', [None])[0]
    if not xt:
        raise ValueError("Magnet link missing xt parameter")

    # Format: urn:btih:<hex_hash>
    hash_str = xt.split(':')[-1].lower()
    if len(hash_str) == 40:
        info_hash = bytes.fromhex(hash_str)
    elif len(hash_str) == 32:
        import base64
        info_hash = base64.b32decode(hash_str.upper())
    else:
        raise ValueError(f"Invalid info hash length: {len(hash_str)}")

    dn = params.get('dn', [''])[0]

    trackers = []
    tr_list = params.get('tr', [])
    if tr_list:
        trackers.append(tr_list)

    return info_hash, dn, trackers

