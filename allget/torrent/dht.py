"""Mainline DHT (BEP 5) implementation for peer discovery without trackers."""

import asyncio, hashlib, logging, os, random, socket, struct, time
from collections import OrderedDict
from typing import Optional

from .bencode import decode_all, encode

logger = logging.getLogger(__name__)

BOOTSTRAP_NODES = [
    ("dht.libtorrent.org", 25401),
    ("dht.transmissionbt.com", 6881),
    ("router.bittorrent.com", 6881),
    ("router.utorrent.com", 6881),
    ("dht.aelitis.com", 6881),
    ("67.215.246.10", 6881),
    ("82.221.103.244", 6881),
    ("87.98.162.88", 6881),
]

K = 8
ALPHA = 3
QUERY_TIMEOUT = 5.0
REFRESH_INTERVAL = 15 * 60
TOKEN_SECRET = os.urandom(8)


class DHTNode:
    __slots__ = ('id', 'ip', 'port', 'last_seen')

    def __init__(self, node_id, ip, port):
        self.id = node_id
        self.ip = ip
        self.port = port
        self.last_seen = time.time()

    def compact(self):
        ip_bytes = socket.inet_aton(self.ip)
        port_bytes = struct.pack('>H', self.port)
        return self.id + ip_bytes + port_bytes

    @staticmethod
    def from_compact(data):
        nodes = []
        for i in range(0, len(data), 26):
            chunk = data[i:i+26]
            if len(chunk) == 26:
                nid = chunk[:20]
                ip = socket.inet_ntoa(chunk[20:24])
                port = struct.unpack('>H', chunk[24:26])[0]
                nodes.append(DHTNode(nid, ip, port))
        return nodes

    def __repr__(self):
        return f"DHTNode({self.ip}:{self.port})"


class KRPCError(Exception):
    pass


class RoutingTable:
    def __init__(self, node_id):
        self.node_id = node_id
        self.buckets = [OrderedDict() for _ in range(160)]

    def _bucket_index(self, target_id):
        xor = int.from_bytes(bytes(a ^ b for a, b in zip(self.node_id, target_id)), 'big')
        if xor == 0: return 0
        return 159 - xor.bit_length() + 1

    def add_node(self, node):
        if node.id == self.node_id: return
        idx = self._bucket_index(node.id)
        idx = max(0, min(159, idx))
        bucket = self.buckets[idx]
        node.last_seen = time.time()
        if node.id in bucket:
            bucket.move_to_end(node.id)
        elif len(bucket) < K:
            bucket[node.id] = node

    def find_closest(self, target_id, count=K):
        idx = self._bucket_index(target_id)
        candidates = []
        for i in range(160):
            for offset in (i, -i):
                bidx = idx + offset
                if offset == 0: bidx = idx
                if 0 <= bidx < 160:
                    for node in self.buckets[bidx].values():
                        dist = int.from_bytes(bytes(a ^ b for a, b in zip(node.id, target_id)), 'big')
                        candidates.append((dist, node))
        candidates.sort(key=lambda x: x[0])
        return [n for _, n in candidates[:count]]

    @property
    def total_nodes(self):
        return sum(len(b) for b in self.buckets)


class DHTClient:
    def __init__(self, port=6881):
        self.node_id = hashlib.sha1(os.urandom(20)).digest()
        self.port = port
        self.routing_table = RoutingTable(self.node_id)
        self._transport = None
        self._running = False
        self._pending = {}
        self._peers_found = []
        self._peer_found_event = asyncio.Event()
        # Store our own peer info for files we have
        self._seeding_info_hashes: dict[bytes, int] = {}  # info_hash -> total_size
        self._seeding_tokens: dict[bytes, dict[tuple, bytes]] = {}  # info_hash -> {addr: token}

    async def start(self):
        loop = asyncio.get_event_loop()
        self._transport, _ = await loop.create_datagram_endpoint(
            lambda: self._make_protocol(), local_addr=('0.0.0.0', self.port)
        )
        self._running = True
        logger.info(f"DHT started on port {self.port}, node_id: {self.node_id.hex()[:12]}")
        await self._bootstrap()

    def _make_protocol(self):
        protocol = _DHTProtocol()
        protocol.client = self
        return protocol

    async def _bootstrap(self):
        logger.info("DHT bootstrapping...")
        for ip, port in BOOTSTRAP_NODES:
            try:
                resp = await self._query(ip, port, 'find_node', {'id': self.node_id, 'target': self.node_id})
                if resp and b'nodes' in resp:
                    nodes_raw = resp[b'nodes']
                    if isinstance(nodes_raw, bytes):
                        for node in DHTNode.from_compact(nodes_raw):
                            self.routing_table.add_node(node)
                    logger.info(f"DHT bootstrapped from {ip}:{port}, table: {self.routing_table.total_nodes} nodes")
                    if self.routing_table.total_nodes >= K:
                        break
            except Exception as e:
                logger.debug(f"DHT bootstrap {ip}:{port} failed: {e}")
        asyncio.create_task(self._refresh_loop())

    async def _refresh_loop(self):
        while self._running:
            await asyncio.sleep(REFRESH_INTERVAL)
            if self._running and self.routing_table.total_nodes < 100:
                await self._bootstrap()

    def register_downloaded(self, info_hash, total_size):
        """Register a completed download so we serve it to DHT peers."""
        self._seeding_info_hashes[info_hash] = total_size
        logger.info(f"DHT registered seed for {info_hash.hex()[:12]}")

    async def announce_to_dht(self, info_hash, listen_port=6881):
        """Announce to DHT that we have this file."""
        if info_hash not in self._seeding_info_hashes:
            return
        logger.info(f"DHT announcing seed for {info_hash.hex()[:12]}")
        closest = self.routing_table.find_closest(info_hash, K)
        if not closest:
            # Use bootstrap nodes if routing table is empty
            for ip, port in BOOTSTRAP_NODES[:4]:
                closest.append(DHTNode(self.node_id, ip, port))
        for node in closest:
            try:
                resp = await self._query(node.ip, node.port, 'get_peers', {'id': self.node_id, 'info_hash': info_hash})
                if resp and b'token' in resp:
                    token = resp[b'token']
                    await self._query(node.ip, node.port, 'announce_peer', {
                        'id': self.node_id,
                        'info_hash': info_hash,
                        'port': listen_port,
                        'token': token,
                        'implied_port': 0,
                    })
            except Exception:
                pass

    async def get_peers(self, info_hash, timeout=30.0):
        self._peers_found = []
        self._peer_found_event.clear()
        logger.info(f"DHT searching peers for {info_hash.hex()[:12]}")
        asyncio.create_task(self._peer_lookup(info_hash))
        try:
            await asyncio.wait_for(self._peer_found_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        logger.info(f"DHT found {len(self._peers_found)} peers for {info_hash.hex()[:12]}")
        return list(self._peers_found)

    async def _peer_lookup(self, info_hash):
        closest = self.routing_table.find_closest(info_hash, ALPHA)
        if not closest:
            for ip, port in BOOTSTRAP_NODES[:4]:
                closest.append(DHTNode(self.node_id, ip, port))
        queried = set()
        while closest and len(self._peers_found) < 50:
            batch = []
            for node in closest:
                nid = node.id
                if nid not in queried:
                    batch.append(node)
                    queried.add(nid)
                if len(batch) >= ALPHA:
                    break
            if not batch:
                break
            tasks = []
            for node in batch:
                tasks.append(self._query(node.ip, node.port, 'get_peers', {'id': self.node_id, 'info_hash': info_hash}))
            results = await asyncio.gather(*tasks, return_exceptions=True)
            new_nodes = []
            for node, result in zip(batch, results):
                if isinstance(result, Exception) or result is None:
                    continue
                # Store token for potential announce_peer later
                if b'token' in result:
                    token = result[b'token']
                    if info_hash not in self._seeding_tokens:
                        self._seeding_tokens[info_hash] = {}
                    self._seeding_tokens[info_hash][(node.ip, node.port)] = token
                if b'values' in result:
                    values = result[b'values']
                    if isinstance(values, list):
                        for v in values:
                            if isinstance(v, bytes) and len(v) == 6:
                                ip = socket.inet_ntoa(v[:4])
                                port = struct.unpack('>H', v[4:6])[0]
                                key = (ip, port)
                                if key not in self._peers_found:
                                    self._peers_found.append(key)
                    if len(self._peers_found) >= 8:
                        self._peer_found_event.set()
                if b'nodes' in result:
                    nodes_raw = result[b'nodes']
                    if isinstance(nodes_raw, bytes):
                        for n in DHTNode.from_compact(nodes_raw):
                            self.routing_table.add_node(n)
                            new_nodes.append(n)
                self.routing_table.add_node(node)
            new_distances = []
            for n in new_nodes:
                if n.id not in queried:
                    dist = int.from_bytes(bytes(a ^ b for a, b in zip(n.id, info_hash)), 'big')
                    new_distances.append((dist, n))
            new_distances.sort(key=lambda x: x[0])
            closest = [n for _, n in new_distances[:ALPHA]]
            if not closest:
                closest = [n for n in self.routing_table.find_closest(info_hash, ALPHA) if n.id not in queried]
        self._peer_found_event.set()

    async def _query(self, ip, port, method, args, timeout=QUERY_TIMEOUT):
        if self._transport is None:
            raise KRPCError("DHT not started")
        txn_id = hashlib.sha1(os.urandom(20)).digest()[:2]
        message = {b't': txn_id, b'y': b'q', b'q': method.encode(), b'a': {k.encode() if isinstance(k, str) else k: v for k, v in args.items()}}
        payload = encode(message)
        addr = (ip, port)
        future = asyncio.get_event_loop().create_future()
        self._pending[txn_id] = future
        self._transport.sendto(payload, addr)
        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            return None
        finally:
            self._pending.pop(txn_id, None)

    def handle_response(self, data, addr):
        try:
            msg = decode_all(data)
        except Exception:
            return
        if not isinstance(msg, dict):
            return
        if msg.get(b'y') == b'r':
            txn_id = msg.get(b't')
            if txn_id and txn_id in self._pending:
                future = self._pending[txn_id]
                if not future.done():
                    future.set_result(msg.get(b'r', {}))
        elif msg.get(b'y') == b'q':
            self._handle_query(msg, addr)

    def _handle_query(self, msg, addr):
        q = msg.get(b'q', b'')
        a = msg.get(b'a', {})
        txn_id = msg.get(b't')
        if not isinstance(a, dict):
            return

        # Add sender to routing table
        sender_id = a.get(b'id')
        if sender_id and isinstance(sender_id, bytes) and len(sender_id) == 20:
            self.routing_table.add_node(DHTNode(sender_id, addr[0], addr[1]))

        if q == b'ping':
            self._send_response(addr, txn_id, {b'id': self.node_id})

        elif q == b'find_node':
            target = a.get(b'target', self.node_id)
            closest = self.routing_table.find_closest(target, K)
            nodes_data = b''.join(n.compact() for n in closest)
            self._send_response(addr, txn_id, {b'id': self.node_id, b'nodes': nodes_data})

        elif q == b'get_peers':
            info_hash = a.get(b'info_hash', b'')
            token = self._make_token(addr)
            closest = self.routing_table.find_closest(info_hash, K)
            nodes_data = b''.join(n.compact() for n in closest)

            # If we have this file, include our peer info in values
            values = []
            if info_hash in self._seeding_info_hashes:
                our_ip = addr[0] if addr[0] != '127.0.0.1' else self._get_own_ip()
                values.append(socket.inet_aton(our_ip) + struct.pack('>H', self.port))
                logger.debug(f"DHT serving peer for {info_hash.hex()[:12]} to {addr}")

            self._send_response(addr, txn_id, {
                b'id': self.node_id,
                b'token': token,
                b'nodes': nodes_data,
                b'values': values,
            })

        elif q == b'announce_peer':
            token = a.get(b'token', b'')
            expected = self._make_token(addr)
            if token == expected and b'info_hash' in a:
                info_hash = a[b'info_hash']
                port_val = a.get(b'port', 6881)
                # Store this as a known seed
                key = (addr[0], port_val)
                logger.debug(f"DHT received announce_peer for {info_hash.hex()[:12]} from {addr[0]}:{port_val}")
                self._send_response(addr, txn_id, {b'id': self.node_id})

    def _send_response(self, addr, txn_id, response):
        msg = {b't': txn_id, b'y': b'r', b'r': response}
        try:
            self._transport.sendto(encode(msg), addr)
        except Exception:
            pass

    def _make_token(self, addr):
        raw = f"{addr[0]}:{addr[1]}:{TOKEN_SECRET.hex()}".encode()
        return hashlib.sha1(raw).digest()[:8]

    def _get_own_ip(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return '127.0.0.1'

    async def stop(self):
        self._running = False
        if self._transport:
            self._transport.close()
            self._transport = None
        for future in self._pending.values():
            if not future.done():
                future.cancel()
        self._pending.clear()


class _DHTProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        super().__init__()
        self.client = None

    def connection_made(self, transport):
        pass

    def datagram_received(self, data, addr):
        if self.client:
            self.client.handle_response(data, addr)

    def error_received(self, exc):
        logger.debug(f"DHT protocol error: {exc}")

    def connection_lost(self, exc):
        pass