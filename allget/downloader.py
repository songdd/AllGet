"""Unified download manager orchestrating all download types."""
import asyncio, logging, os, time, uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable

from .parsers import detect_link_type, LinkType, extract_filename_from_url
from .http_downloader import HTTPDownloader
from .ed2k import ED2KDownloader

logger = logging.getLogger(__name__)

class TaskStatus(Enum):
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"

@dataclass
class DownloadTask:
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    url: str = ""
    link_type: LinkType = LinkType.UNKNOWN
    filename: str = ""
    save_path: str = ""
    status: TaskStatus = TaskStatus.QUEUED
    total_bytes: int = 0
    downloaded_bytes: int = 0
    download_speed: float = 0.0
    progress: float = 0.0
    error: str = ""
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    _downloader: Optional[object] = field(default=None, repr=False)

class DownloadManager:
    def __init__(self, default_save_path=None):
        self.default_save_path = default_save_path or os.path.join(os.getcwd(), "downloads")
        self.tasks: dict[str, DownloadTask] = {}
        self._http_downloader: Optional[HTTPDownloader] = None
        self.active_downloads: dict[str, asyncio.Task] = {}
        self._callback = None

    def set_callback(self, callback):
        self._callback = callback

    def add_task(self, url, save_path=None, filename=None):
        link_type = detect_link_type(url)
        if link_type == LinkType.UNKNOWN:
            raise ValueError(f"Unrecognized link type: {url[:100]}")
        sp = save_path or self.default_save_path
        fn = filename or extract_filename_from_url(url)
        task = DownloadTask(url=url, link_type=link_type, filename=fn, save_path=sp)
        self.tasks[task.id] = task
        if self._callback:
            self._callback(task)
        return task

    async def start_task(self, task_id):
        task = self.tasks.get(task_id)
        if not task:
            return
        if task_id in self.active_downloads:
            return
        coro = self._run_task(task)
        t = asyncio.create_task(coro)
        self.active_downloads[task_id] = t

    async def _run_task(self, task):
        task.status = TaskStatus.DOWNLOADING
        self._notify(task)
        try:
            if task.link_type in (LinkType.HTTP, LinkType.HTTPS, LinkType.TORRENT):
                if task.link_type == LinkType.TORRENT:
                    # Download .torrent file first, then start torrent
                    task.status = TaskStatus.DOWNLOADING
                    self._notify(task)
                await self._download_http(task)
            elif task.link_type == LinkType.MAGNET:
                await self._download_magnet(task)
            elif task.link_type == LinkType.ED2K:
                await self._download_ed2k(task)
            if task.status != TaskStatus.PAUSED:
                task.status = TaskStatus.COMPLETED
                task.completed_at = time.time()
        except Exception as e:
            logger.error(f"Task {task.id} failed: {e}")
            task.status = TaskStatus.FAILED
            task.error = str(e)
        self._notify(task)

    async def _download_http(self, task):
        dl = HTTPDownloader(
            progress_callback=lambda p, d, t: self._on_progress(task, dl, d, t),
            stats_callback=None,
        )
        task._downloader = dl
        await dl.start(task.url, task.save_path, task.filename)

    async def _download_magnet(self, task):
        """Start magnet download using torrent client."""
        from .torrent.client import TorrentClient
        client = TorrentClient(
            task.save_path,
            progress_callback=lambda p, d, t: self._on_progress(task, client, d, t),
            stats_callback=lambda speed, d, t: self._on_stats(task, speed, d, t),
        )
        task._downloader = client
        await client.start(magnet_uri=task.url)

    async def _download_ed2k(self, task):
        dl = ED2KDownloader(
            progress_callback=lambda p, d, t: self._on_progress(task, dl, d, t),
        )
        task._downloader = dl
        await dl.start(task.url, task.save_path)

    def _on_progress(self, task, downloader, downloaded, total):
        task.downloaded_bytes = downloaded
        task.total_bytes = total or task.total_bytes
        task.progress = downloaded / total if total > 0 else 0.0
        task.download_speed = getattr(downloader, "download_speed", 0)
        self._notify(task)

    def _on_stats(self, task, speed, downloaded, total):
        task.download_speed = speed
        task.downloaded_bytes = downloaded
        task.total_bytes = total or task.total_bytes
        task.progress = downloaded / total if total > 0 else 0.0
        self._notify(task)

    def _notify(self, task):
        if self._callback:
            self._callback(task)

    async def pause_task(self, task_id):
        task = self.tasks.get(task_id)
        if task and task._downloader:
            task._downloader.pause()
            task.status = TaskStatus.PAUSED
            self._notify(task)

    async def resume_task(self, task_id):
        task = self.tasks.get(task_id)
        if task and task._downloader:
            task._downloader.resume()
            task.status = TaskStatus.DOWNLOADING
            self._notify(task)

    async def stop_task(self, task_id):
        task = self.tasks.get(task_id)
        if task and task._downloader:
            await task._downloader.stop()
            task.status = TaskStatus.STOPPED
            self._notify(task)
        if task_id in self.active_downloads:
            self.active_downloads[task_id].cancel()
            del self.active_downloads[task_id]

    async def add_peer_to_task(self, task_id, ip, port):
        """Manually add a peer to a running torrent task."""
        task = self.tasks.get(task_id)
        if not task:
            return False
        if task._downloader and hasattr(task._downloader, "add_peer"):
            return await task._downloader.add_peer(ip, int(port))
        return False

    async def delete_task(self, task_id):
        await self.stop_task(task_id)
        self.tasks.pop(task_id, None)

    def get_task(self, task_id):
        return self.tasks.get(task_id)

    def get_all_tasks(self):
        return list(self.tasks.values())

    def get_active_count(self):
        return sum(1 for t in self.tasks.values() if t.status == TaskStatus.DOWNLOADING)

    async def start_torrent_from_file(self, file_path, save_path=None):
        """Start a torrent download from a .torrent file."""
        sp = save_path or self.default_save_path
        task_id = uuid.uuid4().hex[:12]
        task = DownloadTask(
            id=task_id, url=file_path, link_type=LinkType.MAGNET,
            filename=os.path.basename(file_path), save_path=sp,
            status=TaskStatus.DOWNLOADING,
        )
        self.tasks[task_id] = task
        self._notify(task)
        from .torrent.client import TorrentClient
        client = TorrentClient(
            sp,
            progress_callback=lambda p, d, t: self._on_progress(task, client, d, t),
            stats_callback=lambda speed, d, t: self._on_stats(task, speed, d, t),
        )
        task._downloader = client
        try:
            await client.start(torrent_path=file_path)
            task.status = TaskStatus.COMPLETED
            task.completed_at = time.time()
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error = str(e)
        self._notify(task)
        return task