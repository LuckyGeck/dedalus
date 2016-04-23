import hashlib

BLOCK_SIZE = 65536


def get_file_hash(path: str) -> str:
    hasher = hashlib.md5()
    with open(path, 'rb') as f:
        buf = f.read(BLOCK_SIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(BLOCK_SIZE)
    return hasher.hexdigest()
