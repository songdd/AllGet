"""eDonkey/eMule (ed2k) link handler."""
import asyncio, hashlib, logging, os, re, struct
from typing import Optional, Callable
from urllib.parse import urlparse, unquote

logger = logging.getLogger(__name__)

ED2K_PATTERN = re.compile(
    r"ed2k://\|file\|([^|]+)\|(\d+)\|([a-fA-F0-9]{32})\|(?:.*?\|)?/",
    re.IGNORECASE,
)

def parse_ed2k_link(uri):
    """Parse an ed2k link, returning (filename, size, md4_hash)."""
    m = ED2K_PATTERN.match(uri)
    if not m:
        raise ValueError(f"Invalid ed2k link: {uri[:80]}")
    name = unquote(m.group(1))
    size = int(m.group(2))
    hash_hex = m.group(3).lower()
    return name, size, hash_hex

class ED2KDownloader:
    """Handles ed2k downloads via known HTTP mirrors and web sources."""

    def __init__(self, progress_callback=None, stats_callback=None):
        self.progress_callback = progress_callback
        self.stats_callback = stats_callback
        self._running = False
        self._paused = False
        self._stop_event = asyncio.Event()
        self.downloaded_bytes = 0
        self.total_bytes = 0
        self.download_speed = 0

    async def start(self, uri, save_path):
        self._running = True
        self._stop_event.clear()
        name, size, md4_hash = parse_ed2k_link(uri)
        self.total_bytes = size
        self.downloaded_bytes = 0
        filepath = os.path.join(save_path, name)
        if os.path.exists(filepath) and os.path.getsize(filepath) == size:
            if self.progress_callback:
                self.progress_callback(1.0, size, size)
            return

        # ed2k links are primarily P2P; without a full ed2k/Kad network client,
        # we attempt HTTP-based sources
        search_urls = self._build_search_urls(name, size, md4_hash)
        for url in search_urls:
            if self._stop_event.is_set():
                break
            success = await self._try_http_download(url, filepath)
            if success:
                if self.progress_callback:
                    self.progress_callback(1.0, size, size)
                return
        logger.warning(f"Could not find HTTP source for ed2k: {name}")

    def _build_search_urls(self, name, size, md4_hash):
        """Build potential HTTP download URLs for an ed2k resource."""
        return []

    async def _try_http_download(self, url, filepath):
        """Try downloading from an HTTP URL."""
        import aiohttp, aiofiles
        try:
            timeout = aiohttp.ClientTimeout(total=60, connect=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        return False
                    total = int(resp.headers.get("Content-Length", 0))
                    if total > 0 and total != self.total_bytes:
                        logger.debug(f"Size mismatch: {total} vs {self.total_bytes}")
                    async with aiofiles.open(filepath, "wb") as f:
                        async for chunk in resp.content.iter_chunked(65536):
                            if self._stop_event.is_set():
                                return False
                            while self._paused:
                                await asyncio.sleep(0.2)
                            await f.write(chunk)
                            self.downloaded_bytes += len(chunk)
                            if self.progress_callback:
                                self.progress_callback(self.progress, self.downloaded_bytes, self.total_bytes)
            return os.path.exists(filepath) and os.path.getsize(filepath) > 0
        except Exception as e:
            logger.debug(f"HTTP try failed for {url}: {e}")
            return False

    @property
    def progress(self):
        return self.downloaded_bytes / self.total_bytes if self.total_bytes > 0 else 0.0

    async def stop(self):
        self._running = False; self._stop_event.set()

    def pause(self): self._paused = True
    def resume(self): self._paused = False

    @property
    def is_paused(self): return self._paused