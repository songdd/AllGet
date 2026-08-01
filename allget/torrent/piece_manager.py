"""Piece selection, verification, and storage management."""
import asyncio, hashlib, logging, os
from typing import Optional, Callable
from .metainfo import TorrentMeta

logger = logging.getLogger(__name__)

class PieceManager:
    def __init__(self, meta, download_dir, progress_callback=None):
        self.meta = meta
        self.download_dir = download_dir
        self.progress_callback = progress_callback
        self.piece_count = meta.piece_count
        self._piece_hashes = meta.piece_hashes()
        self._verified_pieces: set[int] = set()
        self._pending_blocks: dict = {}
        self._piece_buffers: dict = {}
        self._piece_requested: set[int] = set()
        self.total_bytes = 0
        self.downloaded_bytes = 0
        self.verified_bytes = 0
        self._lock = asyncio.Lock()
        self._resume_file = os.path.join(download_dir, f".{meta.name}.resume")
        self._load_resume()

    def _load_resume(self):
        if os.path.exists(self._resume_file):
            try:
                with open(self._resume_file, "rb") as f:
                    data = f.read()
                for i in range(0, len(data), 4):
                    idx = int.from_bytes(data[i:i+4], "big")
                    if idx < self.piece_count:
                        self._verified_pieces.add(idx)
                        sz = self.meta.piece_size(idx)
                        self.verified_bytes += sz
                        self.downloaded_bytes += sz
                        self.total_bytes += sz
            except Exception:
                pass

    def _save_resume(self):
        try:
            data = b"".join(idx.to_bytes(4, "big") for idx in sorted(self._verified_pieces))
            with open(self._resume_file, "wb") as f:
                f.write(data)
        except Exception:
            pass

    @property
    def is_complete(self):
        return len(self._verified_pieces) == self.piece_count

    @property
    def progress(self):
        if self.meta.total_size == 0:
            return 0.0
        return self.verified_bytes / self.meta.total_size

    def missing_pieces(self):
        return set(range(self.piece_count)) - self._verified_pieces

    def available_pieces(self, peer_bitfield):
        missing = self.missing_pieces()
        available = set()
        for idx in missing:
            byte_idx = idx // 8
            bit_idx = 7 - (idx % 8)
            if byte_idx < len(peer_bitfield) and (peer_bitfield[byte_idx] & (1 << bit_idx)):
                available.add(idx)
        return available

    def select_piece(self, peer_bitfield):
        available = self.available_pieces(peer_bitfield)
        if not available:
            return None
        unrequested = available - self._piece_requested
        if unrequested:
            return min(unrequested)
        return min(available)

    async def add_block(self, piece_index, begin, block):
        async with self._lock:
            if piece_index in self._verified_pieces:
                return False
            if piece_index not in self._piece_buffers:
                self._piece_buffers[piece_index] = bytearray(self.meta.piece_size(piece_index))
            buf = self._piece_buffers[piece_index]
            end = begin + len(block)
            if end > len(buf):
                return False
            buf[begin:end] = block
            self._pending_blocks[(piece_index, begin)] = bytearray(block)
            self.downloaded_bytes += len(block)
            piece_size = self.meta.piece_size(piece_index)
            filled = sum(len(b) for (pi, _), b in self._pending_blocks.items() if pi == piece_index)
            if filled >= piece_size:
                if await self._verify_piece(piece_index):
                    return True
            if self.progress_callback:
                self.progress_callback(self.progress, self.downloaded_bytes, self.meta.total_size)
            return False

    async def _verify_piece(self, piece_index):
        buf = self._piece_buffers.get(piece_index)
        if buf is None:
            return False
        computed = hashlib.sha1(bytes(buf)).digest()
        expected = self._piece_hashes[piece_index]
        if computed == expected:
            self._verified_pieces.add(piece_index)
            self.verified_bytes += len(buf)
            await self._write_piece(piece_index)
            self._save_resume()
            self._piece_buffers.pop(piece_index, None)
            keys = [k for k in self._pending_blocks if k[0] == piece_index]
            for k in keys:
                self._pending_blocks.pop(k, None)
            self._piece_requested.discard(piece_index)
            if self.progress_callback:
                self.progress_callback(self.progress, self.downloaded_bytes, self.meta.total_size)
            return True
        else:
            logger.warning(f"Piece {piece_index} hash mismatch")
            self._piece_buffers.pop(piece_index, None)
            keys = [k for k in self._pending_blocks if k[0] == piece_index]
            for k in keys:
                self._pending_blocks.pop(k, None)
            self._piece_requested.discard(piece_index)
            return False

    async def _write_piece(self, piece_index):
        buf = self._piece_buffers.get(piece_index)
        if buf is None:
            return
        piece_offset = piece_index * self.meta.piece_length
        piece_data = bytes(buf)
        if self.meta.is_single_file:
            filepath = os.path.join(self.download_dir, self.meta.files[0].path)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "r+b") as f:
                f.seek(piece_offset)
                f.write(piece_data)
        else:
            remaining = piece_data
            offset = piece_offset
            while remaining:
                file_info, file_offset = self.meta.file_for_offset(offset)
                filepath = os.path.join(self.download_dir, file_info.path)
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                write_len = min(len(remaining), file_info.length - file_offset)
                with open(filepath, "r+b") as f:
                    f.seek(file_offset)
                    f.write(remaining[:write_len])
                remaining = remaining[write_len:]
                offset += write_len

    async def preallocate_files(self):
        """Pre-allocate files, then hash-check any existing data."""
        for fi in self.meta.files:
            filepath = os.path.join(self.download_dir, fi.path)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            if not os.path.exists(filepath):
                with open(filepath, "wb") as f:
                    f.seek(fi.length - 1)
                    f.write(b"\x00")

    async def verify_existing_data(self):
        """Scan existing files and verify pieces against known hashes.
        Call this after preallocation to detect already-downloaded data."""
        if self.meta.total_size == 0:
            return 0

        # Check if all files exist with correct sizes
        for fi in self.meta.files:
            filepath = os.path.join(self.download_dir, fi.path)
            if not os.path.exists(filepath):
                return 0
            if os.path.getsize(filepath) != fi.length:
                return 0

        logger.info(f"All files exist with correct sizes, running hash check...")
        verified_count = 0

        for piece_idx in range(self.piece_count):
            if piece_idx in self._verified_pieces:
                continue

            piece_size = self.meta.piece_size(piece_idx)
            piece_data = bytearray(piece_size)
            piece_offset = piece_idx * self.meta.piece_length

            # Read piece data from existing files
            try:
                remaining = piece_size
                offset = piece_offset
                read_pos = 0
                while remaining > 0:
                    file_info, file_offset = self.meta.file_for_offset(offset)
                    filepath = os.path.join(self.download_dir, file_info.path)
                    to_read = min(remaining, file_info.length - file_offset)
                    with open(filepath, "rb") as f:
                        f.seek(file_offset)
                        chunk = f.read(to_read)
                    piece_data[read_pos:read_pos+len(chunk)] = chunk
                    read_pos += len(chunk)
                    remaining -= len(chunk)
                    offset += len(chunk)

                computed = hashlib.sha1(bytes(piece_data)).digest()
                expected = self._piece_hashes[piece_idx]

                if computed == expected:
                    self._verified_pieces.add(piece_idx)
                    self.verified_bytes += piece_size
                    self.downloaded_bytes += piece_size
                    verified_count += 1
            except Exception as e:
                logger.warning(f"Hash check piece {piece_idx} error: {e}")
                break

        if verified_count > 0:
            self._save_resume()
            total = self.meta.total_size
            pct = self.verified_bytes / total * 100 if total > 0 else 0
            logger.info(f"Hash check: {verified_count}/{self.piece_count} pieces verified ({pct:.1f}%)")
            if self.progress_callback:
                self.progress_callback(self.progress, self.downloaded_bytes, self.meta.total_size)

        return verified_count

    def cleanup(self):
        if os.path.exists(self._resume_file):
            try:
                os.remove(self._resume_file)
            except Exception:
                pass