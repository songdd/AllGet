# AllGet

Universal download manager supporting HTTP/HTTPS, Magnet links, BitTorrent (.torrent), and eD2k links. Self-contained BT protocol implementation — no external libraries needed for torrenting.

## Features

| Protocol | Capabilities |
|----------|------------|
| HTTP/HTTPS | Resumable downloads (Range headers), auto filename extraction, .part temp files |
| Magnet | Full Mainline DHT (BEP 5) peer discovery, metadata exchange (BEP 9/10) |
| Torrent (.torrent) | Bencode parser, HTTP + UDP tracker, peer wire protocol, piece verification (SHA1), resume file, TCP server for seeding |
| eD2k | Link parsing, hash extraction, HTTP source fallback |

- Web UI with real-time WebSocket progress updates
- Pause / Resume / Stop / Delete for all download types
- Create `.torrent` files from any file → get magnet links
- Manual peer connection for LAN testing (bypass tracker/DHT)
- Auto hash-check: detects already-downloaded files and seeds them
- Clipboard auto-paste for download links

## Quick Start

```powershell
pip install -r requirements.txt
python -m uvicorn allget.main:app --host 0.0.0.0 --port 8590
```

Open `http://localhost:8590`.

## Supported Link Formats

```
https://example.com/file.zip
magnet:?xt=urn:btih:8feb2e1f334765c2e8488fef9ba14aa2f0fadf5b&dn=test
ed2k://|file|movie.avi|99999999|ABCDEF1234567890ABCDEF1234567890|/
(Upload .torrent file via the Torrent button)
```

## Create Torrent from File

```powershell
python -m allget.torrent.mk_torrent downloads\myfile.zip

# Output:
#   Torrent: downloads\myfile.zip.torrent
#   Magnet:  magnet:?xt=urn:btih:abc123...&dn=myfile.zip
```

## Two-Machine LAN Verification

No tracker, no DHT needed. Direct TCP peer connection.

**Machine A (seed):**

```powershell
python -m allget.torrent.mk_torrent downloads\myfile
python -m uvicorn allget.main:app --host 0.0.0.0 --port 8590
```

1. Open `http://<A_IP>:8590`
2. Click **Torrent** button → select `downloads\myfile.torrent`
3. Hash check passes → `Seed mode active on port 6882`
4. Note the magnet link from mk_torrent output

**Machine B (downloader):**

```powershell
python -m uvicorn allget.main:app --host 0.0.0.0 --port 8590
```

1. Copy `myfile.torrent` to B (network share / USB / A's HTTP)
2. Click **Torrent** → select `myfile.torrent`
3. In the task card, fill `<A_IP>:6882` → click **Connect**
4. B connects to A via BT wire protocol, downloads piece by piece with SHA1 verification

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/tasks` | List all download tasks |
| POST | `/api/tasks` | Create task (form: `url`) |
| POST | `/api/tasks/upload-torrent` | Upload .torrent file to start |
| POST | `/api/tasks/{id}/pause` | Pause task |
| POST | `/api/tasks/{id}/resume` | Resume task |
| POST | `/api/tasks/{id}/stop` | Stop task |
| DELETE | `/api/tasks/{id}` | Delete task |
| POST | `/api/tasks/{id}/peers` | Add manual peer (form: `ip`, `port`) |
| WS | `/ws` | Real-time task updates |

## Architecture

```
allget/
  main.py              FastAPI app, REST + WebSocket
  downloader.py         Task queue, orchestration, manual peer API
  parsers.py            Link type detection
  http_downloader.py    HTTP engine with Range resume
  ed2k.py              eD2k link parser
  torrent/
    bencode.py          Bencode encoder/decoder
    metainfo.py         .torrent file + magnet link parser
    tracker.py          HTTP + UDP tracker (quote_from_bytes encoding)
    peer.py             Peer wire protocol + extension handshake (BEP 10)
    peer_server.py      TCP listen server for seeding + piece serving
    piece_manager.py    Piece selection (rarest-first), SHA1 verify, hash-check, resume
    client.py           Download orchestrator with DHT + metadata exchange
    dht.py              Mainline DHT (BEP 5) — KRPC, K-buckets, get_peers, announce_peer
    mk_torrent.py       Create .torrent files from any file/folder
static/
  index.html            Single-page UI
  style.css             Dark theme
  app.js                WebSocket client, task cards, manual peer UI
```

## Requirements

- Python 3.10+
- `fastapi`, `uvicorn`, `aiohttp`, `aiofiles`, `python-multipart`