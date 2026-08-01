# AllGet

Universal download manager supporting HTTP/HTTPS, Magnet links, BitTorrent (.torrent), and eD2k links. Built with Python + FastAPI backend and vanilla JS frontend.

## Features

| Protocol | Capabilities |
|----------|------------|
| **HTTP/HTTPS** | Resumable downloads (Range headers), auto filename extraction, .part temp files |
| **Magnet** | Full Mainline DHT (BEP 5) peer discovery, metadata exchange (BEP 9/10), public tracker fallback |
| **Torrent (.torrent)** | Bencode parser, HTTP + UDP tracker, peer wire protocol, piece verification (SHA1), resume file |
| **eD2k** | Link parsing, hash extraction, HTTP source fallback |

- Web UI with real-time WebSocket progress updates
- Pause / Resume / Stop / Delete for all download types
- Upload `.torrent` files directly in browser
- Create `.torrent` files from any file → get magnet links for testing
- Clipboard auto-paste for download links
- Dark theme UI

## Quick Start

```powershell
cd D:\code\AllGet
pip install -r requirements.txt
python -m uvicorn allget.main:app --host 0.0.0.0 --port 8590
```

Open `http://localhost:8590` in browser.

## Create Torrent from File (for testing)

```powershell
# Generate .torrent file + magnet link for any file
python -m allget.torrent.mk_torrent downloads\myfile.zip

# Output:
#   Torrent: downloads\myfile.zip.torrent
#   Magnet:  magnet:?xt=urn:btih:abc123...&dn=myfile.zip
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/tasks` | List all download tasks |
| POST | `/api/tasks` | Create task (form: `url`, `save_path`, `filename`) |
| POST | `/api/tasks/upload-torrent` | Upload .torrent file to start download |
| POST | `/api/tasks/{id}/pause` | Pause task |
| POST | `/api/tasks/{id}/resume` | Resume task |
| POST | `/api/tasks/{id}/stop` | Stop task |
| DELETE | `/api/tasks/{id}` | Delete task |
| POST | `/api/torrent/make` | Upload file → return magnet link |
| WS | `/ws` | WebSocket for real-time task updates |

## Architecture

```
allget/
  main.py              FastAPI app, REST + WebSocket, lifespan
  downloader.py         Unified task queue & orchestration
  parsers.py            Link type detection (HTTP/Magnet/Torrent/eD2k)
  http_downloader.py    HTTP/HTTPS engine with Range resume
  ed2k.py              eD2k link parser & HTTP source downloader
  torrent/
    bencode.py          Bencode encoder/decoder
    metainfo.py         .torrent file & magnet link parser
    tracker.py          HTTP + UDP tracker communication
    peer.py             Peer wire protocol + extension handshake
    piece_manager.py    Piece selection (rarest-first), SHA1 verify, file I/O
    client.py           Download orchestrator with DHT + metadata exchange
    dht.py              Mainline DHT (BEP 5) — KRPC, routing table, get_peers
    mk_torrent.py       Create .torrent files & magnet links from any file
static/
  index.html            Single-page UI
  style.css             Dark theme
  app.js                WebSocket client, task cards, clipboard paste
```

## DHT Verification (two-machine test)

1. **Machine A** — create a test `.torrent`, download the file via AllGet, keep it running (it auto-announces to DHT as a seed)
2. **Machine B** — paste the same magnet link, AllGet discovers Machine A via DHT and downloads

Both machines need only `http://localhost:8590` accessible from each other. No central tracker required.

## Requirements

- Python 3.10+
- `fastapi`, `uvicorn`, `aiohttp`, `aiofiles`, `python-multipart`