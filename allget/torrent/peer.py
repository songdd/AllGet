"""BitTorrent peer wire protocol with extension support."""
import asyncio, hashlib, logging, struct
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional

logger = logging.getLogger(__name__)


class MessageID(IntEnum):
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7
    CANCEL = 8
    PORT = 9


# BEP 10: Extension protocol
EXTENDED_MESSAGE = 20
# Reserved bit for LTEP (libtorrent extension protocol): bit 20 from the right (0-indexed)
# In the reserved bytes (offset 20-27 in handshake), we set the 43rd bit from right
# The 8 reserved bytes are at positions 20-27. Bit 20 from the right = bit 4 of byte 5 (counting from right)
# Actually: bit 20 -> byte 5 (20//8=2? No: 20: bytes[5] (indexing from start) or bytes[2] (from right))
# Let me use the standard approach: reserved[5] |= 0x10  (bit 20 from left/most-significant)
# The standard says: "Bit 20 of the Reserved field (byte 5, bit 0x10)"
# In handshake: 1 byte pstrlen + 19 bytes pstr + 8 bytes reserved
# Byte 5 of reserved (0-indexed): reserved[5] |= 0x10
EXTENSION_BIT_BYTE = 5
EXTENSION_BIT_MASK = 0x10

BLOCK_SIZE = 16384
METADATA_BLOCK_SIZE = 16384  # 16KB per metadata piece per BEP 9


class ExtMessageID(IntEnum):
    HANDSHAKE = 0
    UT_METADATA = 1  # will be assigned dynamically


@dataclass
class PeerConnection:
    ip: str
    port: int
    info_hash: bytes
    peer_id: bytes
    piece_count: int

    am_choking: bool = True
    am_interested: bool = False
    peer_choking: bool = True
    peer_interested: bool = False
    peer_bitfield: bytearray = field(default_factory=bytearray)

    reader: Optional[asyncio.StreamReader] = None
    writer: Optional[asyncio.StreamWriter] = None
    piece_queue: asyncio.Queue = field(default_factory=asyncio.Queue)

    # Extension support
    extensions_enabled: bool = True
    peer_extensions: dict = field(default_factory=dict)
    ext_message_ids: dict[str, int] = field(default_factory=dict)
    _ext_handshake_sent: bool = False
    _ext_handshake_received: bool = False

    # Metadata exchange
    metadata_queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    _ut_metadata_msg_id: Optional[int] = None

    def has_piece(self, index):
        if not self.peer_bitfield:
            return False
        byte_idx = index // 8
        bit_idx = 7 - (index % 8)
        if byte_idx >= len(self.peer_bitfield):
            return False
        return bool(self.peer_bitfield[byte_idx] & (1 << bit_idx))

    def supports_extensions(self):
        return self.peer_extensions is not None and len(self.peer_extensions) > 0

    def get_ext_msg_id(self, name):
        m = self.peer_extensions.get('m', {})
        return m.get(name)

    async def connect(self, timeout=10.0):
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.ip, self.port), timeout=timeout
            )
        except (asyncio.TimeoutError, OSError, ConnectionError) as e:
            logger.debug(f"Connect to {self.ip}:{self.port} failed: {e}")
            return False

        handshake = self._build_handshake()
        self.writer.write(handshake)
        await self.writer.drain()

        try:
            response = await asyncio.wait_for(self.reader.readexactly(68), timeout=timeout)
        except (asyncio.TimeoutError, asyncio.IncompleteReadError):
            await self.close()
            return False

        if not self._verify_handshake(response):
            await self.close()
            return False

        # Check if peer supports extensions by inspecting reserved byte
        reserved = response[20:28]
        peer_supports_ext = bool(reserved[EXTENSION_BIT_BYTE] & EXTENSION_BIT_MASK) if len(reserved) > EXTENSION_BIT_BYTE else False

        if peer_supports_ext and self.extensions_enabled:
            await self._send_extended_handshake()

        return True

    def _build_handshake(self):
        pstr = b"BitTorrent protocol"
        reserved = bytearray(8)
        if self.extensions_enabled:
            reserved[EXTENSION_BIT_BYTE] |= EXTENSION_BIT_MASK
        return bytes([len(pstr)]) + pstr + bytes(reserved) + self.info_hash + self.peer_id

    def _verify_handshake(self, data):
        if len(data) != 68:
            return False
        pstrlen = data[0]
        if pstrlen != 19:
            return False
        received_hash = data[28:48]
        return received_hash == self.info_hash

    async def _send_extended_handshake(self):
        """Send BEP 10 extended handshake."""
        from .bencode import encode
        ext_handshake = {
            'm': {
                'ut_metadata': 1,
            },
            'metadata_size': 0,  # Will be set by client if known
            'v': 'AllGet 1.0',
        }
        payload = bytes([0]) + encode(ext_handshake)
        await self.send_message(EXTENDED_MESSAGE, payload)
        self._ext_handshake_sent = True

    async def send_message(self, msg_id, payload=b""):
        if self.writer is None:
            return
        length = 1 + len(payload)
        msg = struct.pack('>IB', length, msg_id) + payload
        try:
            self.writer.write(msg)
            await self.writer.drain()
        except (OSError, ConnectionError):
            pass

    async def send_request(self, index, begin, length):
        payload = struct.pack('>III', index, begin, length)
        await self.send_message(MessageID.REQUEST, payload)

    async def send_interested(self):
        self.am_interested = True
        await self.send_message(MessageID.INTERESTED)

    async def send_unchoke(self):
        self.am_choking = False
        await self.send_message(MessageID.UNCHOKE)

    async def send_have(self, index):
        payload = struct.pack('>I', index)
        await self.send_message(MessageID.HAVE, payload)

    async def send_metadata_request(self, piece):
        """Request a metadata piece via ut_metadata."""
        ut_id = self.get_ext_msg_id('ut_metadata')
        if ut_id is None:
            return
        from .bencode import encode
        msg = encode({b'msg_type': 0, b'piece': piece})
        payload = bytes([ut_id]) + msg
        await self.send_message(EXTENDED_MESSAGE, payload)

    async def read_messages(self):
        if self.reader is None:
            return
        try:
            while True:
                raw_len = await self.reader.readexactly(4)
                length = struct.unpack('>I', raw_len)[0]
                if length == 0:
                    continue
                body = await self.reader.readexactly(length)
                msg_id = body[0]
                payload = body[1:] if length > 1 else b""

                if msg_id == MessageID.CHOKE:
                    self.peer_choking = True
                elif msg_id == MessageID.UNCHOKE:
                    self.peer_choking = False
                elif msg_id == MessageID.INTERESTED:
                    self.peer_interested = True
                elif msg_id == MessageID.NOT_INTERESTED:
                    self.peer_interested = False
                elif msg_id == MessageID.HAVE:
                    index = struct.unpack('>I', payload)[0]
                    self._set_have(index)
                elif msg_id == MessageID.BITFIELD:
                    self.peer_bitfield = bytearray(payload)
                elif msg_id == MessageID.PIECE:
                    piece_index = struct.unpack('>I', payload[:4])[0]
                    begin = struct.unpack('>I', payload[4:8])[0]
                    block = payload[8:]
                    await self.piece_queue.put((piece_index, begin, block))
                elif msg_id == EXTENDED_MESSAGE:
                    await self._handle_extended(payload)
                elif msg_id == MessageID.CANCEL:
                    pass

        except (asyncio.IncompleteReadError, OSError, ConnectionError):
            pass

    async def _handle_extended(self, payload):
        """Handle BEP 10 extended message."""
        if len(payload) < 1:
            return
        ext_id = payload[0]
        ext_data = payload[1:]

        from .bencode import decode_all
        try:
            decoded = decode_all(ext_data)
        except Exception:
            return

        if not isinstance(decoded, dict):
            return

        if ext_id == 0:
            # Extended handshake
            self.peer_extensions = decoded
            self._ext_handshake_received = True
            logger.debug(f"Peer {self.ip}:{self.port} extensions: {decoded.get(b'm', {})}")
        else:
            # ut_metadata or other extension
            ut_id = self.get_ext_msg_id('ut_metadata')
            if ut_id is not None and ext_id == ut_id:
                msg_type = decoded.get(b'msg_type')
                piece = decoded.get(b'piece', 0)
                if msg_type == 1:
                    # data
                    data = decoded.get(b'data', b'')
                    total_size = decoded.get(b'total_size', 0)
                    await self.metadata_queue.put((piece, data, total_size))
                elif msg_type == 2:
                    # reject
                    logger.debug(f"Metadata piece {piece} rejected by {self.ip}:{self.port}")

    def _set_have(self, index):
        byte_idx = index // 8
        bit_idx = 7 - (index % 8)
        if byte_idx >= len(self.peer_bitfield):
            needed = byte_idx + 1 - len(self.peer_bitfield)
            self.peer_bitfield.extend(b'\x00' * needed)
        self.peer_bitfield[byte_idx] |= (1 << bit_idx)

    async def close(self):
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except (OSError, ConnectionError):
                pass
            self.writer = None
        self.reader = None


class PeerManager:
    def __init__(self, info_hash, peer_id, piece_count, max_peers=50):
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.piece_count = piece_count
        self.max_peers = max_peers
        self.connections = {}
        self._next_peer_id = 0

    async def add_peer(self, ip, port):
        key = (ip, port)
        if key in self.connections:
            return self.connections[key]
        if len(self.connections) >= self.max_peers:
            return None
        conn = PeerConnection(
            ip=ip, port=port, info_hash=self.info_hash,
            peer_id=f"-AG01-{self._next_peer_id:012d}".encode(),
            piece_count=self.piece_count,
        )
        self._next_peer_id += 1
        if await conn.connect():
            self.connections[key] = conn
            return conn
        return None

    async def add_peers(self, peers):
        tasks = [self.add_peer(ip, port) for ip, port in peers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if isinstance(r, PeerConnection)]

    async def close_all(self):
        for conn in list(self.connections.values()):
            await conn.close()
        self.connections.clear()