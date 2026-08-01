"""Link type detection and URL parsing."""
import re
from enum import Enum
from urllib.parse import urlparse

class LinkType(Enum):
    HTTP = "http"
    HTTPS = "https"
    MAGNET = "magnet"
    TORRENT = "torrent"
    ED2K = "ed2k"
    UNKNOWN = "unknown"

MAGNET_PATTERN = re.compile(r"^magnet:\?xt=urn:btih:[a-fA-F0-9]{32,40}", re.IGNORECASE)
ED2K_PATTERN = re.compile(r"^ed2k://\|file\|", re.IGNORECASE)

def detect_link_type(uri):
    """Detect the type of a download link."""
    uri = uri.strip()
    if not uri:
        return LinkType.UNKNOWN
    parsed = urlparse(uri)
    if parsed.scheme in ("http", "https"):
        # Check if it looks like a torrent URL (ends with .torrent)
        path = parsed.path.lower()
        if path.endswith(".torrent"):
            return LinkType.TORRENT
        return LinkType.HTTPS if parsed.scheme == "https" else LinkType.HTTP
    if parsed.scheme == "magnet":
        return LinkType.MAGNET
    if parsed.scheme == "ed2k" or ED2K_PATTERN.match(uri):
        return LinkType.ED2K
    # Check if raw magnet
    if MAGNET_PATTERN.match(uri):
        return LinkType.MAGNET
    return LinkType.UNKNOWN

def extract_filename_from_url(url):
    """Extract a reasonable filename from a URL."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    if "/" in path:
        name = path.rsplit("/", 1)[-1]
    else:
        name = path or "download"
    from urllib.parse import unquote
    name = unquote(name)
    return name if name else "download"