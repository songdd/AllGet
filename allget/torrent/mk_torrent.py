"""Create .torrent files and derive magnet links for testing."""
import hashlib, os, sys, argparse
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from allget.torrent.bencode import encode

PIECE_LENGTH = 256 * 1024  # 256 KiB pieces


def create_torrent(file_path, output_path=None, tracker="http://tracker.opentrackr.org:1337/announce",
                   piece_length=PIECE_LENGTH, private=False, comment="AllGet test torrent"):
    """Create a .torrent file from a single file or directory."""
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    info = {}
    pieces = b""
    total_size = 0

    if file_path.is_file():
        # Single file
        info[b'name'] = file_path.name.encode('utf-8')
        info[b'length'] = file_path.stat().st_size
        total_size = file_path.stat().st_size
        pieces = _hash_file(file_path, piece_length)
    else:
        # Directory
        info[b'name'] = file_path.name.encode('utf-8')
        files_list = []
        all_files = sorted(f for f in file_path.rglob('*') if f.is_file())
        for f in all_files:
            rel = f.relative_to(file_path)
            # BEP 3 uses list of path components
            path_parts = [p.encode('utf-8') for p in rel.parts]
            size = f.stat().st_size
            files_list.append({b'length': size, b'path': path_parts})
            total_size += size
        info[b'files'] = files_list
        # Hash all file data concatenated
        pieces = _hash_directory(file_path, piece_length)

    info[b'piece length'] = piece_length
    info[b'pieces'] = pieces

    if private:
        info[b'private'] = 1

    torrent = {
        b'announce': tracker.encode('utf-8'),
        b'comment': comment.encode('utf-8'),
        b'created by': b'AllGet',
        b'creation date': int(__import__('time').time()),
        b'info': info,
    }

    torrent_data = encode(torrent)

    if output_path is None:
        output_path = str(file_path) + ".torrent"
    else:
        output_path = str(output_path)

    with open(output_path, 'wb') as f:
        f.write(torrent_data)

    # Compute info_hash
    info_data = encode(info)
    info_hash = hashlib.sha1(info_data).hexdigest()
    magnet = f"magnet:?xt=urn:btih:{info_hash}&dn={file_path.name}"

    return output_path, magnet, info_hash, total_size


def _hash_file(file_path, piece_length):
    """Hash a single file into pieces."""
    pieces = []
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(piece_length)
            if not chunk:
                break
            pieces.append(hashlib.sha1(chunk).digest())
    return b''.join(pieces)


def _hash_directory(dir_path, piece_length):
    """Hash a directory (all files concatenated in order)."""
    all_files = sorted(f for f in dir_path.rglob('*') if f.is_file())
    pieces = []
    buf = b""
    total = 0

    for f in all_files:
        total += f.stat().st_size
        with open(f, 'rb') as fh:
            data = fh.read()
            buf += data
            # Process complete pieces from buffer
            while len(buf) >= piece_length:
                pieces.append(hashlib.sha1(buf[:piece_length]).digest())
                buf = buf[piece_length:]

    # Last partial piece
    if buf:
        pieces.append(hashlib.sha1(buf).digest())

    return b''.join(pieces)


def get_magnet_from_torrent(torrent_path):
    """Extract magnet link from an existing .torrent file."""
    from allget.torrent.metainfo import parse_torrent_file
    meta = parse_torrent_file(str(torrent_path))
    magnet = f"magnet:?xt=urn:btih:{meta.info_hash.hex()}&dn={meta.name}"
    if meta.announce_list:
        for tier in meta.announce_list:
            for tr in tier:
                magnet += f"&tr={tr}"
    elif meta.announce:
        magnet += f"&tr={meta.announce}"
    return magnet


def main():
    parser = argparse.ArgumentParser(description="Create .torrent file and generate magnet link")
    parser.add_argument("path", help="File or directory to create torrent for")
    parser.add_argument("-o", "--output", help="Output .torrent file path")
    parser.add_argument("-t", "--tracker", default="http://tracker.opentrackr.org:1337/announce")
    parser.add_argument("-p", "--piece-length", type=int, default=PIECE_LENGTH,
                        help=f"Piece length in bytes (default: {PIECE_LENGTH})")
    parser.add_argument("--private", action="store_true", help="Mark as private torrent")
    parser.add_argument("--from-torrent", help="Extract magnet link from existing .torrent file")

    args = parser.parse_args()

    if args.from_torrent:
        magnet = get_magnet_from_torrent(args.from_torrent)
        print(magnet)
        return

    output, magnet, info_hash, total_size = create_torrent(
        args.path, args.output, args.tracker, args.piece_length, args.private
    )
    print(f"Torrent: {output}")
    print(f"Info Hash: {info_hash}")
    print(f"Size: {total_size:,} bytes ({total_size/1024/1024:.1f} MB)")
    print(f"Magnet: {magnet}")


if __name__ == "__main__":
    main()