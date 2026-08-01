"""BT TCP listen server - accepts incoming peer connections and serves pieces."""
import asyncio, hashlib, logging, struct, os
from .metainfo import TorrentMeta
from .peer import BLOCK_SIZE, MessageID, EXTENDED_MESSAGE, EXTENSION_BIT_BYTE, EXTENSION_BIT_MASK
from .bencode import encode

logger = logging.getLogger(__name__)

class PeerServer:
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
        try:
            self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
            logger.info(f"BT listen server on {self.host}:{self.port}")
        except OSError:
            logger.warning(f"Port {self.port} busy, trying random...")
            self._server = await asyncio.start_server(self._handle_client, self.host, 0)
            addr = self._server.sockets[0].getsockname()
            self.port = addr[1]
            logger.info(f"BT listen server on {self.host}:{self.port}")

    async def _handle_client(self, reader, writer):
        addr = writer.get_extra_info("peername")
        logger.info(f"Incoming BT connection from {addr[0]}:{addr[1]}")
        try:
            # Read handshake - loop to handle partial reads
            handshake = b""
            while len(handshake) < 68:
                chunk = await reader.read(68 - len(handshake))
                if not chunk:
                    raise asyncio.IncompleteReadError(handshake, 68)
                handshake += chunk
            logger.info(f"Handshake from {addr[0]}:{addr[1]}: pstrlen={handshake[0]}, hash={handshake[28:48].hex()[:12]}...")
            if handshake[0] != 19:
                logger.warning(f"Bad pstrlen={handshake[0]}")
                writer.close(); return
            received_hash = handshake[28:48]
            if received_hash != self.meta.info_hash:
                logger.warning(f"Info_hash mismatch: got {received_hash.hex()[:12]}, expected {self.meta.info_hash.hex()[:12]}")
                writer.close(); return

            # Send our handshake + bitfield + unchoke
            pstr = b"BitTorrent protocol"
            reserved = bytearray(8)
            reserved[EXTENSION_BIT_BYTE] |= EXTENSION_BIT_MASK
            our = bytes([len(pstr)]) + pstr + bytes(reserved) + self.meta.info_hash + self._peer_id
            try:
                sock = writer.get_extra_info('socket')
                sock.setsockopt(6, 1, 1)  # TCP_NODELAY
                transport = writer.transport
                transport.write(our)
                bf = self._make_bitfield()
                if bf:
                    transport.write(struct.pack(">IB", 1 + len(bf), MessageID.BITFIELD) + bf)
                transport.write(struct.pack(">IB", 1, MessageID.UNCHOKE))
                await asyncio.sleep(0.1)
            except Exception:
                writer.write(our)
                bf = self._make_bitfield()
                if bf:
                    writer.write(struct.pack(">IB", 1 + len(bf), MessageID.BITFIELD) + bf)
                writer.write(struct.pack(">IB", 1, MessageID.UNCHOKE))
                await writer.drain()
            logger.info(f"Handshake OK, sent to {addr[0]}:{addr[1]}")

            # Handle messages loop
            while self._running:
                try:
                    raw_len = b""
                    while len(raw_len) < 4:
                        chunk = await reader.read(4 - len(raw_len))
                        if not chunk: raise asyncio.IncompleteReadError(raw_len, 4)
                        raw_len += chunk
                    length = struct.unpack(">I", raw_len)[0]
                    if length == 0: continue
                    body = b""
                    while len(body) < length:
                        chunk = await reader.read(length - len(body))
                        if not chunk: raise asyncio.IncompleteReadError(body, length)
                        body += chunk
                    msg_id = body[0]
                    payload = body[1:] if length > 1 else b""
                    if msg_id == MessageID.REQUEST:
                        await self._on_request(writer, payload)
                    elif msg_id == EXTENDED_MESSAGE:
                        await self._on_extended(writer, payload)
                except (asyncio.IncompleteReadError, ConnectionError, OSError):
                    break
        except (asyncio.IncompleteReadError, ConnectionError, OSError) as e:
            logger.info(f"BT connection closed from {addr[0]}:{addr[1]}: {type(e).__name__}")
        except Exception as e:
            logger.warning(f"BT unexpected error from {addr[0]}:{addr[1]}: {type(e).__name__}: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _on_request(self, writer, payload):
        if len(payload) < 12: return
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
        transport = writer.transport
        transport.write(struct.pack(">IB", 1 + 12 + len(data), MessageID.PIECE) + struct.pack(">II", index, begin) + data)
        await asyncio.sleep(0.1)

    async def _on_extended(self, writer, payload):
        if len(payload) < 1: return
        ext_id = payload[0]
        try: decoded = decode(payload[1:])
        except Exception: return
        if not isinstance(decoded, dict): return
        if ext_id == 0:
            resp = {b"m": {b"ut_metadata": 1}, b"v": b"AllGet 1.0"}
            if self.meta.info_data:
                resp[b"metadata_size"] = len(self.meta.info_data)
            writer.write(struct.pack(">IB", 2 + len(encode(resp)), EXTENDED_MESSAGE) + bytes([0]) + encode(resp))
            await writer.drain()
        else:
            if decoded.get(b"msg_type") == 0 and self.meta.info_data:
                piece = decoded.get(b"piece", 0)
                bs = 16384; off = piece * bs
                chunk = self.meta.info_data[off:off+bs]
                resp = encode({b"msg_type": 1, b"piece": piece, b"total_size": len(self.meta.info_data), b"data": chunk})
                writer.write(struct.pack(">IB", 2 + len(resp), EXTENDED_MESSAGE) + bytes([1]) + resp)
                await writer.drain()

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
