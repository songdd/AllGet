"""BT TCP listen server - accepts incoming peer connections and serves pieces."""
import asyncio, hashlib, logging, struct, os
from .metainfo import TorrentMeta
from .peer import BLOCK_SIZE, MessageID, EXTENDED_MESSAGE, EXTENSION_BIT_BYTE, EXTENSION_BIT_MASK
from .bencode import encode

logger = logging.getLogger(__name__)

class _BTServerProtocol(asyncio.Protocol):
    """Protocol handler for BT peer connections - works reliably on Windows."""
    def __init__(self, server):
        self._server = server
        self._transport = None
        self._buffer = bytearray()
        self._handshake_done = False

    def connection_made(self, transport):
        self._transport = transport
        addr = transport.get_extra_info("peername")
        logger.info(f"Incoming BT connection from {addr[0]}:{addr[1]}")
        self._buffer = bytearray()

    def data_received(self, data):
        self._buffer.extend(data)
        if not self._handshake_done and len(self._buffer) >= 68:
            hs = bytes(self._buffer[:68])
            self._buffer = self._buffer[68:]
            self._handshake_done = True
            asyncio.ensure_future(self._server._handle_handshake(self._transport, hs))
        elif self._handshake_done:
            # Handle post-handshake messages
            asyncio.ensure_future(self._server._handle_messages(self._transport, bytes(self._buffer)))
            self._buffer = bytearray()

    def connection_lost(self, exc):
        addr = self._transport.get_extra_info("peername") if self._transport else ("?", 0)
        logger.debug(f"BT connection closed from {addr[0]}:{addr[1]}")


class PeerServer:
    """Listens for incoming BT peer connections and serves file pieces."""

    def __init__(self, meta, piece_mgr, download_dir, listen_host="0.0.0.0", listen_port=6882):
        self.meta = meta
        self.piece_mgr = piece_mgr
        self.download_dir = download_dir
        self.host = listen_host
        self.port = listen_port
        self._server = None
        self._running = False
        self._peer_id = b"-AG01-SEED0000001"

    async def start(self):
        self._running = True
        loop = asyncio.get_event_loop()
        factory = lambda: _BTServerProtocol(self)
        try:
            self._server = await loop.create_server(factory, self.host, self.port)
            logger.info(f"BT listen server on {self.host}:{self.port}")
        except OSError:
            logger.warning(f"Port {self.port} busy, trying random...")
            self._server = await loop.create_server(factory, self.host, 0)
            addr = self._server.sockets[0].getsockname()
            self.port = addr[1]
            logger.info(f"BT listen server on {self.host}:{self.port}")

    async def _handle_handshake(self, transport, handshake):
        addr = transport.get_extra_info("peername")
        logger.info(f"Handshake from {addr[0]}:{addr[1]}: pstrlen={handshake[0]}, hash={handshake[28:48].hex()[:12]}...")
        if handshake[0] != 19:
            logger.warning(f"Bad pstrlen={handshake[0]} from {addr[0]}:{addr[1]}")
            transport.close(); return
        received_hash = handshake[28:48]
        if received_hash != self.meta.info_hash:
            logger.warning(f"Info_hash mismatch: got {received_hash.hex()[:12]}, expected {self.meta.info_hash.hex()[:12]}")
            transport.close(); return
        try:
            pstr = b"BitTorrent protocol"
            reserved = bytearray(8)
            reserved[EXTENSION_BIT_BYTE] |= EXTENSION_BIT_MASK
            our_hs = bytes([len(pstr)]) + pstr + bytes(reserved) + self.meta.info_hash + self._peer_id
            transport.write(our_hs)
            bf = self._make_bitfield()
            if bf:
                transport.write(struct.pack(">IB", 1 + len(bf), MessageID.BITFIELD) + bf)
            transport.write(struct.pack(">IB", 1, MessageID.UNCHOKE))
            logger.info(f"Handshake OK, sent bitfield+unchoke to {addr[0]}:{addr[1]}")
        except Exception as e:
            logger.warning(f"Send error to {addr[0]}:{addr[1]}: {e}")
            transport.close()

    async def _handle_messages(self, transport, data):
        addr = transport.get_extra_info("peername")
        pos = 0
        while pos + 4 <= len(data):
            length = struct.unpack(">I", data[pos:pos+4])[0]
            if pos + 4 + length > len(data):
                break
            body = data[pos+4:pos+4+length]
            pos += 4 + length
            if length == 0: continue
            msg_id = body[0]
            payload = body[1:] if length > 1 else b""
            if msg_id == MessageID.REQUEST and len(payload) >= 12:
                await self._on_request(transport, payload)
            elif msg_id == EXTENDED_MESSAGE:
                await self._on_extended(transport, payload)

    async def _on_request(self, transport, payload):
        index, begin, length = struct.unpack(">III", payload[:12])
        if not self.piece_mgr or index not in self.piece_mgr._verified_pieces:
            return
        poff = index * self.meta.piece_length + begin
        to_read = min(length, BLOCK_SIZE)
        try:
            if self.meta.is_single_file:
                fp = os.path.join(self.download_dir, self.meta.files[0].path)
                with open(fp, "rb") as f:
                    f.seek(poff); data = f.read(to_read)
            else:
                data = bytearray(); rem = to_read; off = poff
                while rem > 0:
                    fi, foff = self.meta.file_for_offset(off)
                    fp = os.path.join(self.download_dir, fi.path)
                    rd = min(rem, fi.length - foff)
                    with open(fp, "rb") as f:
                        f.seek(foff); data.extend(f.read(rd))
                    rem -= rd; off += rd
                data = bytes(data)
        except Exception:
            return
        resp = struct.pack(">II", index, begin) + data
        msg = struct.pack(">IB", 1 + len(resp), MessageID.PIECE) + resp
        try:
            transport.write(msg)
        except Exception:
            pass

    async def _on_extended(self, transport, payload):
        if len(payload) < 1: return
        ext_id = payload[0]
        try: decoded = decode(payload[1:])
        except Exception: return
        if not isinstance(decoded, dict): return
        if ext_id == 0:
            resp = {b"m": {b"ut_metadata": 1}, b"v": b"AllGet 1.0"}
            if self.meta.info_data:
                resp[b"metadata_size"] = len(self.meta.info_data)
            msg = struct.pack(">IB", 2 + len(encode(resp)), EXTENDED_MESSAGE) + bytes([0]) + encode(resp)
            try: transport.write(msg)
            except Exception: pass
        else:
            if decoded.get(b"msg_type") == 0 and self.meta.info_data:
                piece = decoded.get(b"piece", 0)
                bs = 16384; off = piece * bs
                chunk = self.meta.info_data[off:off+bs]
                resp = encode({b"msg_type": 1, b"piece": piece, b"total_size": len(self.meta.info_data), b"data": chunk})
                msg = struct.pack(">IB", 2 + len(resp), EXTENDED_MESSAGE) + bytes([1]) + resp
                try: transport.write(msg)
                except Exception: pass

    def _make_bitfield(self):
        if not self.piece_mgr or self.piece_mgr.piece_count == 0:
            return b""
        bc = (self.piece_mgr.piece_count + 7) // 8
        bf = bytearray(bc)
        for idx in range(self.piece_mgr.piece_count):
            if idx in self.piece_mgr._verified_pieces:
                bi = idx // 8; bf[bi] |= (1 << (7 - (idx % 8)))
        return bytes(bf)

    @property
    def listen_port(self):
        return self.port

    async def stop(self):
        self._running = False
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
