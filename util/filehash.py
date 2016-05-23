import hashlib
from os.path import getsize
BLOCK_SIZE = 65536


def get_file_hash(path: str) -> str:
    hasher = hashlib.md5()
    hasher.update(path.encode('utf-8', errors='replace'))
    file_size = getsize(path)
    if file_size > 0:
        with open(path, 'rb') as f:
            buf = f.read(min(BLOCK_SIZE, file_size))
            while len(buf) > 0:
                hasher.update(buf)
                file_size -= len(buf)
                buf = f.read(min(BLOCK_SIZE, file_size))
    return hasher.hexdigest()
