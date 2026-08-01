"""BitTorrent tracker communication (HTTP and UDP)."""

import asyncio, hashlib, logging, random, socket, struct, time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse
import aiohttp
from .metainfo import TorrentMeta

logger = logging.getLogger(__name__)
PEER_ID_PREFIX = b"-AG01"


@dataclass
class PeerInfo:
    ip: str
    port: int
    peer_id: Optional[bytes] = None


@dataclass
class TrackerResponse:
    interval: int
    complete: int
    incomplete: int
    peers: list[PeerInfo]
    min_interval: int = 0
    tracker_id: str = ""


def _generate_peer_id():
    suffix = ''.join(random.choice('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ') for _ in range(16))
    return (PEER_ID_PREFIX + suffix.encode())[:20]


def _generate_key():
    return random.randint(0, 0x7FFFFFFF)


class _UDPTrackerProtocol(asyncio.DatagramProtocol):
    """Protocol for receiving UDP tracker responses."""

    def __init__(self):
        super().__init__()
        self.transport = None
        self.future = asyncio.get_event_loop().create_future()
        self._closed = False

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        if not self.future.done():
            self.future.set_result((data, addr))

    def error_received(self, exc):
        if not self.future.done() and not self._closed:
            self.future.set_exception(exc)

    def connection_lost(self, exc):
        # Only set exception if not already intentionally closed
        if not self.future.done() and not self._closed:
            err = exc or ConnectionError("Connection closed")
            self.future.set_exception(err)


class TrackerClient:
    """Communicates with BitTorrent trackers via HTTP and UDP."""

    def __init__(self, meta):
        self.meta = meta
        self.peer_id = _generate_peer_id()
        self.key = _generate_key()
        self._session = None

    async def _get_session(self):
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=15)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def announce(self, tracker_url, uploaded=0, downloaded=0, left=0, event="started"):
        parsed = urlparse(tracker_url)
        if parsed.scheme in ('http', 'https'):
            return await self._announce_http(tracker_url, uploaded, downloaded, left, event)
        elif parsed.scheme == 'udp':
            return await self._announce_udp(tracker_url, uploaded, downloaded, left, event)
        else:
            logger.warning(f"Unsupported tracker protocol: {parsed.scheme}")
            return None

    async def _announce_http(self, url, uploaded, downloaded, left, event):
        params = {
            'info_hash': self.meta.info_hash,
            'peer_id': self.peer_id,
            'port': 6881,
            'uploaded': str(uploaded),
            'downloaded': str(downloaded),
            'left': str(left),
            'compact': '1',
            'event': event,
            'key': str(self.key),
        }
        try:
            session = await self._get_session()
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.warning(f"Tracker {url} returned {resp.status}")
                    return None
                data = await resp.read()
                return self._parse_http_response(data)
        except Exception as e:
            logger.warning(f"Tracker {url} error: {e}")
            return None

    def _parse_http_response(self, data):
        from .bencode import decode_all
        try:
            resp = decode_all(data)
        except Exception as e:
            logger.warning(f"Failed to decode tracker response: {e}")
            return None
        if not isinstance(resp, dict):
            return None
        failure = resp.get(b'failure reason')
        if failure:
            msg = failure.decode('utf-8', errors='replace') if isinstance(failure, bytes) else str(failure)
            logger.warning(f"Tracker failure: {msg}")
            return None
        interval = resp.get(b'interval', 1800)
        min_interval = resp.get(b'min interval', interval)
        complete = resp.get(b'complete', 0)
        incomplete = resp.get(b'incomplete', 0)
        peers = []
        peers_data = resp.get(b'peers')
        if isinstance(peers_data, bytes):
            for i in range(0, len(peers_data), 6):
                chunk = peers_data[i:i+6]
                if len(chunk) == 6:
                    ip = '.'.join(str(b) for b in chunk[:4])
                    port = struct.unpack('>H', chunk[4:6])[0]
                    peers.append(PeerInfo(ip=ip, port=port))
        elif isinstance(peers_data, list):
            for peer_dict in peers_data:
                if isinstance(peer_dict, dict):
                    ip_bytes = peer_dict.get(b'ip', b'')
                    ip = ip_bytes.decode('utf-8', errors='replace') if isinstance(ip_bytes, bytes) else str(ip_bytes)
                    port = peer_dict.get(b'port', 0)
                    pid = peer_dict.get(b'peer id')
                    peers.append(PeerInfo(ip=ip, port=port, peer_id=pid))
        return TrackerResponse(
            interval=interval, min_interval=min_interval,
            complete=complete, incomplete=incomplete, peers=peers,
        )

    async def _announce_udp(self, url, uploaded, downloaded, left, event):
        parsed = urlparse(url)
        host = parsed.hostname
        port = parsed.port or 80
        loop = asyncio.get_event_loop()

        conn_id = 0x41727101980
        transaction_id = random.randint(0, 0x7FFFFFFF)
        connect_req = struct.pack('>QII', conn_id, 0, transaction_id)

        protocol = None
        try:
            _, protocol = await loop.create_datagram_endpoint(
                lambda: _UDPTrackerProtocol(),
                remote_addr=(host, port),
            )
            protocol.transport.sendto(connect_req)

            try:
                data, _ = await asyncio.wait_for(asyncio.shield(protocol.future), timeout=10.0)
            except asyncio.TimeoutError:
                self._close_protocol(protocol)
                return None

            if len(data) < 16:
                self._close_protocol(protocol)
                return None

            action, tx_id, new_conn_id = struct.unpack('>IIQ', data[:16])
            if tx_id != transaction_id or action != 0:
                self._close_protocol(protocol)
                return None

            event_map = {'started': 2, 'stopped': 3, 'completed': 1, '': 0}
            ev = event_map.get(event, 0)
            transaction_id2 = random.randint(0, 0x7FFFFFFF)

            announce_req = struct.pack(
                '>QQQ20s20sQQQIIIiH',
                new_conn_id, 1, transaction_id2,
                self.meta.info_hash, self.peer_id,
                downloaded, left, uploaded,
                ev, 0, self.key, -1, port,
            )

            protocol.future = loop.create_future()
            protocol.transport.sendto(announce_req)

            try:
                data, _ = await asyncio.wait_for(asyncio.shield(protocol.future), timeout=10.0)
            except asyncio.TimeoutError:
                self._close_protocol(protocol)
                return None

            self._close_protocol(protocol)

            if len(data) < 20:
                return None

            action_resp, tx_id_resp = struct.unpack('>II', data[:8])
            if tx_id_resp != transaction_id2 or action_resp != 1:
                return None

            interval, leechers, seeders = struct.unpack('>III', data[8:20])

            peers_data = data[20:]
            peers = []
            for i in range(0, len(peers_data), 6):
                chunk = peers_data[i:i+6]
                if len(chunk) == 6:
                    ip = '.'.join(str(b) for b in chunk[:4])
                    port_val = struct.unpack('>H', chunk[4:6])[0]
                    peers.append(PeerInfo(ip=ip, port=port_val))

            return TrackerResponse(
                interval=interval, complete=seeders,
                incomplete=leechers, peers=peers,
            )

        except Exception as e:
            logger.warning(f"UDP tracker {url} error: {e}")
            if protocol:
                self._close_protocol(protocol)
            return None

    @staticmethod
    def _close_protocol(protocol):
        protocol._closed = True
        if not protocol.future.done():
            protocol.future.cancel()
        try:
            protocol.transport.close()
        except Exception:
            pass