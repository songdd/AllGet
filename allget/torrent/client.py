"""Torrent download client with DHT + metadata exchange support."""
import asyncio, hashlib, logging, os, time
from .metainfo import TorrentMeta, parse_torrent_data, parse_torrent_file, parse_magnet_link
from .tracker import TrackerClient
from .peer import PeerManager, BLOCK_SIZE, METADATA_BLOCK_SIZE
from .piece_manager import PieceManager
from .dht import DHTClient
from .peer_server import PeerServer

logger = logging.getLogger(__name__)

DEFAULT_TRACKERS = [
    "http://tracker.opentrackr.org:1337/announce",
    "http://open.tracker.cl:1337/announce",
    "https://tracker.tamersunion.org:443/announce",
    "http://tracker.bt4g.com:2095/announce",
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.tracker.cl:1337/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://tracker.torrent.eu.org:451/announce",
]

class TorrentClient:
    def __init__(self, download_dir, progress_callback=None, stats_callback=None, status_callback=None):
        self.download_dir = download_dir
        self.progress_callback = progress_callback
        self.stats_callback = stats_callback
        self.status_callback = status_callback
        self._running = False
        self._paused = False
        self._stop_event = asyncio.Event()
        self.meta = None
        self.tracker = None
        self.peer_mgr = None
        self.piece_mgr = None
        self.dht = None
        self.download_speed = 0
        self._last_bytes = 0
        self._last_time = time.time()
        self._file_already_complete = False
        self._peer_server = None

    async def start(self, data=None, torrent_path=None, magnet_uri=None):
        """Entry point with guaranteed cleanup."""
        try:
            self._running = True
            self._stop_event.clear()
            if torrent_path:
                self.meta = parse_torrent_file(torrent_path)
            elif data:
                self.meta = parse_torrent_data(data)
            elif magnet_uri:
                self.meta = await self._resolve_magnet(magnet_uri)
            else:
                raise ValueError("No torrent source provided")
            if self.meta is None:
                raise ValueError("Failed to resolve torrent metadata")

            self._set_status(f"Downloading: {self.meta.name}")
            self.tracker = TrackerClient(self.meta)
            self.piece_mgr = PieceManager(self.meta, self.download_dir, self._on_progress)

            if self.meta.total_size > 0:
                await self.piece_mgr.preallocate_files()
                verified = await self.piece_mgr.verify_existing_data()
                if verified > 0:
                    self._set_status(f"Hash check: {verified}/{self.piece_mgr.piece_count} pieces OK")
                if self.piece_mgr.is_complete:
                    self._file_already_complete = True
                    self._set_status("File already complete on disk!")

            self.peer_mgr = PeerManager(self.meta.info_hash, self.tracker.peer_id, max(self.meta.piece_count, 1))

            # Start DHT if not already running (magnet path starts it earlier)
            if not self.dht:
                self.dht = DHTClient()
                try:
                    await self.dht.start()
                except Exception as e:
                    logger.warning(f"DHT start failed: {e}")
                    self.dht = None

            # Start BT listen server for incoming peer connections
            logger.info(f"Starting PeerServer on port 6882...")
            self._peer_server = PeerServer(self.meta, self.piece_mgr, self.download_dir)
            await self._peer_server.start()
            logger.info(f"PeerServer started on port {self._peer_server.listen_port}")
            self._set_status(f"BT listening on port {self._peer_server.listen_port}")

            await self._download_loop()
        finally:
            if self.peer_mgr:
                await self.peer_mgr.close_all()
            if self.tracker:
                await self.tracker.close()
            if self._peer_server:
                await self._peer_server.stop()
            if self.dht:
                await self.dht.stop()

    async def _resolve_magnet(self, uri):
        info_hash, name, trackers = parse_magnet_link(uri)
        display_name = name or info_hash.hex()[:12]
        self._set_status(f"Resolving metadata for {display_name}")
        self.dht = DHTClient()
        try:
            await self.dht.start()
        except Exception as e:
            logger.warning(f"DHT start failed: {e}")
            self.dht = None
        tracker_urls = list(trackers[0]) if trackers and trackers[0] else list(DEFAULT_TRACKERS)
        all_peers = set()
        minimal_meta = TorrentMeta(
            info_hash=info_hash, name=display_name,
            piece_length=0, pieces=b'', total_size=0,
            announce_list=trackers if trackers else [DEFAULT_TRACKERS],
        )
        tracker = TrackerClient(minimal_meta)
        peer_mgr = PeerManager(info_hash, tracker.peer_id, 1)
        self._metadata_buffer = bytearray()
        self._metadata_size = 0
        try:
            for url in tracker_urls:
                if self._stop_event.is_set(): break
                resp = await tracker.announce(url, left=0, event="started")
                if resp and resp.peers:
                    for p in resp.peers:
                        all_peers.add((p.ip, p.port))
                    await peer_mgr.add_peers([(p.ip, p.port) for p in resp.peers])
                    self._set_status(f"Tracker: {len(resp.peers)} peers from {url[:50]}")
        except Exception as e:
            logger.warning(f"Tracker announce error: {e}")
        metadata = await self._fetch_metadata(peer_mgr, tracker, tracker_urls, all_peers, info_hash)
        await peer_mgr.close_all()
        await tracker.close()
        if metadata:
            return parse_torrent_data(bytes(metadata))
        self._set_status(f"Metadata fetch failed for {display_name}")
        return None

    async def _fetch_metadata(self, peer_mgr, tracker, tracker_urls, extra_peers, info_hash):
        metadata_pieces = {}
        total_pieces = 0
        last_dht_search = 0
        for attempt in range(120):
            if self._stop_event.is_set():
                return None
            if self.dht and time.time() - last_dht_search > 3.0 and len(peer_mgr.connections) < 10:
                last_dht_search = time.time()
                try:
                    dht_peers = await asyncio.wait_for(self.dht.get_peers(info_hash, timeout=3.0), timeout=4.0)
                    extra_peers.update(dht_peers)
                    if dht_peers:
                        new_conns = await peer_mgr.add_peers(list(dht_peers))
                        self._set_status(f"DHT: {len(dht_peers)} peers, {len(new_conns)} connected")
                except (asyncio.TimeoutError, Exception):
                    pass
            if attempt % 10 == 0:
                for url in tracker_urls:
                    try:
                        resp = await tracker.announce(url, left=0, event="")
                        if resp and resp.peers:
                            await peer_mgr.add_peers([(p.ip, p.port) for p in resp.peers])
                    except Exception:
                        pass
            for (ip, port), conn in list(peer_mgr.connections.items()):
                if not conn._ext_handshake_received:
                    if conn.reader is not None:
                        asyncio.create_task(conn.read_messages())
                    continue
                ut_id = conn.get_ext_msg_id('ut_metadata')
                if ut_id is None: continue
                meta_size = conn.peer_extensions.get(b'metadata_size', 0)
                if meta_size and meta_size > 0 and self._metadata_size == 0:
                    self._metadata_size = meta_size
                    total_pieces = (meta_size + METADATA_BLOCK_SIZE - 1) // METADATA_BLOCK_SIZE
                    self._set_status(f"Metadata: {meta_size} bytes, {total_pieces} pieces")
                    self._metadata_buffer = bytearray(meta_size)
                if total_pieces == 0: continue
                for piece in range(total_pieces):
                    if piece not in metadata_pieces:
                        await conn.send_metadata_request(piece)
                try:
                    piece, data, mtotal = await asyncio.wait_for(conn.metadata_queue.get(), timeout=1.0)
                    if piece not in metadata_pieces:
                        metadata_pieces[piece] = data
                        offset = piece * METADATA_BLOCK_SIZE
                        end = min(offset + len(data), self._metadata_size)
                        self._metadata_buffer[offset:end] = data[:end-offset]
                        if total_pieces > 0:
                            self._set_status(f"Metadata: {len(metadata_pieces)}/{total_pieces} pieces")
                        if total_pieces > 0 and len(metadata_pieces) >= total_pieces:
                            try:
                                from .bencode import decode_all
                                info = decode_all(bytes(self._metadata_buffer))
                                if isinstance(info, dict) and b'name' in info:
                                    name = info.get(b'name', b'').decode('utf-8', errors='replace')
                                    self._set_status(f"Metadata resolved: {name}")
                                    return bytes(self._metadata_buffer)
                            except Exception: pass
                except asyncio.TimeoutError: pass
            await asyncio.sleep(0.5)
        return None

    async def _download_loop(self):
        urls = self._get_tracker_urls_for(self.meta)

        if self._file_already_complete and self.dht:
            self._set_status("Already complete, announcing to DHT...")
            self.dht.register_downloaded(self.meta.info_hash, self.meta.total_size)
            await self.dht.announce_to_dht(self.meta.info_hash)
            self._set_status("Seed announced to DHT. Keeping server running.")
            self._set_status(f"Seed mode active on port {self._peer_server.listen_port}. Waiting for peers...")
            while self._running and not self._stop_event.is_set():
                await asyncio.sleep(1)
            return

        self._set_status("Connecting to peers...")
        for url in urls:
            resp = await self.tracker.announce(url, left=self.meta.total_size, event="started")
            if resp and resp.peers:
                self._set_status(f"Tracker: {len(resp.peers)} peers")
                await self.peer_mgr.add_peers([(p.ip, p.port) for p in resp.peers])
        if self.dht:
            try:
                dht_peers = await asyncio.wait_for(self.dht.get_peers(self.meta.info_hash, timeout=10.0), timeout=12.0)
                if dht_peers:
                    await self.peer_mgr.add_peers(dht_peers)
                    self._set_status(f"DHT: {len(dht_peers)} peers")
            except (asyncio.TimeoutError, Exception): pass

        peer_refresh_count = 0
        while self._running and not self._stop_event.is_set():
            if self._paused:
                await asyncio.sleep(0.5); continue
            if self.piece_mgr and self.piece_mgr.is_complete:
                self._set_status("Complete!")
                if self.dht:
                    self.dht.register_downloaded(self.meta.info_hash, self.meta.total_size)
                    asyncio.create_task(self.dht.announce_to_dht(self.meta.info_hash))
                break
            await self._process_peers()
            peer_refresh_count += 1
            if len(self.peer_mgr.connections) < 5 and peer_refresh_count % 10 == 0:
                left = self.meta.total_size - (self.piece_mgr.verified_bytes if self.piece_mgr else 0)
                for url in urls:
                    try:
                        resp = await self.tracker.announce(url, left=left, event="")
                        if resp and resp.peers:
                            await self.peer_mgr.add_peers([(p.ip, p.port) for p in resp.peers])
                    except Exception: pass
                if self.dht:
                    try:
                        dht_peers = await asyncio.wait_for(self.dht.get_peers(self.meta.info_hash, timeout=5.0), timeout=6.0)
                        if dht_peers: await self.peer_mgr.add_peers(dht_peers)
                    except (asyncio.TimeoutError, Exception): pass
            await asyncio.sleep(0.1)

        if self.piece_mgr and self.piece_mgr.is_complete:
            for url in urls:
                try: await self.tracker.announce(url, left=0, event="completed")
                except Exception: pass
            self.piece_mgr.cleanup()

    async def _process_peers(self):
        for conn in list(self.peer_mgr.connections.values()):
            if conn.reader is not None:
                asyncio.create_task(self._handle_peer(conn))

    async def _handle_peer(self, conn):
        if not conn.am_interested:
            await conn.send_interested(); await conn.send_unchoke()
        if self.piece_mgr is None or conn.peer_choking:
            return
        piece_idx = self.piece_mgr.select_piece(conn.peer_bitfield)
        if piece_idx is None: return
        self.piece_mgr._piece_requested.add(piece_idx)
        piece_size = self.piece_mgr.meta.piece_size(piece_idx)
        for offset in range(0, piece_size, BLOCK_SIZE):
            block_len = min(BLOCK_SIZE, piece_size - offset)
            await conn.send_request(piece_idx, offset, block_len)
            try:
                pidx, begin, block = await asyncio.wait_for(conn.piece_queue.get(), timeout=10)
                if await self.piece_mgr.add_block(pidx, begin, block): break
            except asyncio.TimeoutError: break
        now = time.time(); elapsed = now - self._last_time
        if elapsed >= 1.0 and self.piece_mgr:
            self.download_speed = (self.piece_mgr.downloaded_bytes - self._last_bytes) / elapsed
            self._last_bytes = self.piece_mgr.downloaded_bytes; self._last_time = now
            if self.stats_callback:
                self.stats_callback(self.download_speed, self.piece_mgr.verified_bytes, self.meta.total_size)

    def _on_progress(self, progress, downloaded, total):
        if self.progress_callback:
            self.progress_callback(progress, downloaded, total)

    def _set_status(self, msg):
        logger.info(msg)
        if self.status_callback: self.status_callback(msg)

    def _get_tracker_urls_for(self, meta):
        urls = []
        if meta.announce: urls.append(meta.announce)
        for tier in meta.announce_list: urls.extend(tier)
        return urls if urls else DEFAULT_TRACKERS

    async def add_peer(self, ip, port):
        """Manually add a peer to connect to."""
        if not self.peer_mgr:
            logger.warning(f"add_peer: peer_mgr is None")
            return False
        logger.info(f"add_peer: connecting to {ip}:{port}...")
        conn = await self.peer_mgr.add_peer(ip, int(port))
        if conn:
            self._set_status(f"Connected to manual peer {ip}:{port}")
            asyncio.create_task(conn.read_messages())
            return True
        logger.warning(f"add_peer: connection to {ip}:{port} failed")
        return False

    async def stop(self):
        self._running = False; self._stop_event.set()

    def pause(self): self._paused = True
    def resume(self): self._paused = False

    @property
    def is_paused(self): return self._paused

    @property
    def progress(self):
        return self.piece_mgr.progress if self.piece_mgr else 0.0

    @property
    def downloaded_bytes(self):
        return self.piece_mgr.downloaded_bytes if self.piece_mgr else 0

    @property
    def total_bytes(self):
        return self.meta.total_size if self.meta else 0