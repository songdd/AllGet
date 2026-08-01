"""AllGet - FastAPI application entry point."""
import asyncio, json, logging, os, sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, Form
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse

from .parsers import detect_link_type, LinkType
from .downloader import DownloadManager, TaskStatus

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("allget")

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
DEFAULT_DOWNLOAD_DIR = str(Path(os.getcwd()) / "downloads")

manager = DownloadManager(default_save_path=DEFAULT_DOWNLOAD_DIR)
ws_clients: list[WebSocket] = []

def broadcast(message):
    """Send a JSON message to all connected WebSocket clients."""
    dead = []
    for ws in ws_clients:
        try:
            asyncio.create_task(ws.send_json(message))
        except Exception:
            dead.append(ws)
    for ws in dead:
        if ws in ws_clients:
            ws_clients.remove(ws)

def on_task_update(task):
    """Callback from download manager."""
    data = {
        "type": "task_update",
        "task": {
            "id": task.id,
            "url": task.url[:120],
            "link_type": task.link_type.value,
            "filename": task.filename,
            "save_path": task.save_path,
            "status": task.status.value,
            "total_bytes": task.total_bytes,
            "downloaded_bytes": task.downloaded_bytes,
            "download_speed": task.download_speed,
            "progress": task.progress,
            "error": task.error,
        },
    }
    broadcast(data)

manager.set_callback(on_task_update)

@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(DEFAULT_DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(STATIC_DIR, exist_ok=True)
    logger.info(f"Download directory: {DEFAULT_DOWNLOAD_DIR}")
    yield

app = FastAPI(title="AllGet", lifespan=lifespan)

@app.get("/api/tasks")
async def list_tasks():
    return {"tasks": [task_to_dict(t) for t in manager.get_all_tasks()]}

@app.post("/api/tasks")
async def create_task(url: str = Form(""), save_path: str = Form(""), filename: str = Form("")):
    if not url.strip():
        return JSONResponse({"error": "URL is required"}, status_code=400)
    try:
        task = manager.add_task(
            url.strip(),
            save_path=save_path.strip() or None,
            filename=filename.strip() or None,
        )
        asyncio.create_task(manager.start_task(task.id))
        return {"task": task_to_dict(task)}
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)

@app.post("/api/tasks/upload-torrent")
async def upload_torrent(file: UploadFile = File(...), save_path: str = Form("")):
    if not file.filename or not file.filename.endswith(".torrent"):
        return JSONResponse({"error": "Invalid torrent file"}, status_code=400)
    data = await file.read()
    sp = save_path.strip() or DEFAULT_DOWNLOAD_DIR
    # Parse torrent to get name and create task
    from .torrent.metainfo import parse_torrent_data
    try:
        meta = parse_torrent_data(data)
        task_id = __import__("uuid").uuid4().hex[:12]
        from .downloader import DownloadTask
        task = DownloadTask(
            id=task_id, url=file.filename, link_type=LinkType.MAGNET,
            filename=meta.name, save_path=sp,
            total_bytes=meta.total_size,
        )
        manager.tasks[task_id] = task
        on_task_update(task)
        # Save torrent and start
        torrent_path = os.path.join(sp, f".{task_id}.torrent")
        with open(torrent_path, "wb") as f:
            f.write(data)
        asyncio.create_task(manager.start_torrent_from_file(torrent_path, sp))
        return {"task": task_to_dict(task)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

@app.post("/api/tasks/{task_id}/pause")
async def pause_task(task_id: str):
    await manager.pause_task(task_id)
    return {"status": "ok"}

@app.post("/api/tasks/{task_id}/resume")
async def resume_task(task_id: str):
    await manager.resume_task(task_id)
    return {"status": "ok"}

@app.post("/api/tasks/{task_id}/stop")
async def stop_task(task_id: str):
    await manager.stop_task(task_id)
    return {"status": "ok"}

@app.delete("/api/tasks/{task_id}")
async def delete_task(task_id: str):
    await manager.delete_task(task_id)
    return {"status": "ok"}

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.append(ws)
    # Send current state
    for task in manager.get_all_tasks():
        await ws.send_json({"type": "task_update", "task": task_to_dict(task)})
    try:
        while True:
            data = await ws.receive_text()
            # Clients can send ping/pong or commands
            msg = json.loads(data)
            if msg.get("type") == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if ws in ws_clients:
            ws_clients.remove(ws)

@app.post("/api/torrent/make")
async def make_torrent(file: UploadFile = File(...)):
    import tempfile
    from .torrent.mk_torrent import create_torrent
    try:
        suffix = os.path.splitext(file.filename or "test")[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name
        torrent_dir = os.path.join(DEFAULT_DOWNLOAD_DIR, "torrents")
        os.makedirs(torrent_dir, exist_ok=True)
        out_path = os.path.join(torrent_dir, (file.filename or "test") + ".torrent")
        output, magnet, info_hash, total_size = create_torrent(tmp_path, out_path)
        try: os.unlink(tmp_path)
        except Exception: pass
        return {"torrent_path": output, "magnet": magnet, "info_hash": info_hash, "size": total_size}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

def task_to_dict(task):
    return {
        "id": task.id,
        "url": task.url[:200],
        "link_type": task.link_type.value,
        "filename": task.filename,
        "save_path": task.save_path,
        "status": task.status.value,
        "total_bytes": task.total_bytes,
        "downloaded_bytes": task.downloaded_bytes,
        "download_speed": task.download_speed,
        "progress": task.progress,
        "error": task.error,
        "created_at": task.created_at,
        "completed_at": task.completed_at,
    }

if STATIC_DIR.exists():
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")

def main():
    import uvicorn
    uvicorn.run("allget.main:app", host="0.0.0.0", port=8590, reload=False, log_level="info")

if __name__ == "__main__":
    main()