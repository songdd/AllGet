"""HTTP/HTTPS download engine with resume support."""
import asyncio, aiohttp, aiofiles, logging, os, time
from typing import Optional, Callable

logger = logging.getLogger(__name__)

class HTTPDownloader:
    def __init__(self, progress_callback=None, stats_callback=None):
        self.progress_callback = progress_callback
        self.stats_callback = stats_callback
        self._running = False
        self._paused = False
        self._stop_event = asyncio.Event()
        self._task = None
        self.downloaded_bytes = 0
        self.total_bytes = 0
        self.download_speed = 0
        self._last_bytes = 0
        self._last_time = time.time()
        self._error = None

    async def start(self, url, save_path, filename=None, headers=None):
        self._running = True
        self._stop_event.clear()
        self.downloaded_bytes = 0
        self.total_bytes = 0
        self.download_speed = 0
        self._last_bytes = 0
        self._last_time = time.time()
        self._error = None
        self._task = asyncio.create_task(self._download(url, save_path, filename, headers))
        await self._task
        if self._error:
            raise self._error

    async def _download(self, url, save_path, filename, headers=None):
        os.makedirs(save_path, exist_ok=True)
        hdrs = headers or {}
        hdrs.setdefault("User-Agent", "AllGet/1.0")
        filepath = os.path.join(save_path, filename or self._extract_filename(url))
        partfile = filepath + ".part"
        existing_size = 0
        if os.path.exists(partfile):
            existing_size = os.path.getsize(partfile)
            hdrs["Range"] = f"bytes={existing_size}-"
        timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_read=30)
        connector = aiohttp.TCPConnector(limit=1, force_close=True)
        try:
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                async with session.get(url, headers=hdrs) as resp:
                    if resp.status not in (200, 206):
                        self._error = Exception(f"HTTP {resp.status} for {url}")
                        return
                    if "Content-Range" in resp.headers:
                        range_val = resp.headers["Content-Range"]
                        total = int(range_val.split("/")[-1])
                    elif "Content-Length" in resp.headers:
                        total = existing_size + int(resp.headers["Content-Length"])
                    else:
                        total = 0
                    self.total_bytes = total
                    self.downloaded_bytes = existing_size
                    mode = "ab" if existing_size > 0 else "wb"
                    async with aiofiles.open(partfile, mode) as f:
                        async for chunk in resp.content.iter_chunked(65536):
                            if self._stop_event.is_set():
                                return
                            while self._paused and not self._stop_event.is_set():
                                await asyncio.sleep(0.2)
                            if self._stop_event.is_set():
                                return
                            await f.write(chunk)
                            self.downloaded_bytes += len(chunk)
                            self._update_speed()
                            if self.progress_callback:
                                self.progress_callback(
                                    self.progress, self.downloaded_bytes, self.total_bytes
                                )
            if not self._stop_event.is_set() and os.path.exists(partfile):
                if os.path.getsize(partfile) > 0:
                    try:
                        os.replace(partfile, filepath)
                    except OSError:
                        pass
        except asyncio.CancelledError:
            self._running = False
        except Exception as e:
            self._error = e
            logger.error(f"Download error: {e}")

    def _update_speed(self):
        now = time.time(); elapsed = now - self._last_time
        if elapsed >= 1.0:
            self.download_speed = (self.downloaded_bytes - self._last_bytes) / elapsed
            self._last_bytes = self.downloaded_bytes; self._last_time = now
            if self.stats_callback:
                self.stats_callback(self.download_speed, self.downloaded_bytes, self.total_bytes)

    def _extract_filename(self, url):
        from urllib.parse import urlparse, unquote
        path = urlparse(url).path
        name = path.rstrip("/").rsplit("/", 1)[-1] if "/" in path else "download"
        return unquote(name) if name else "download"

    @property
    def progress(self):
        return self.downloaded_bytes / self.total_bytes if self.total_bytes > 0 else 0.0

    async def stop(self):
        self._running = False
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def pause(self): self._paused = True
    def resume(self): self._paused = False

    @property
    def is_paused(self): return self._paused