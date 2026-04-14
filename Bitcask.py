import os
import struct

# Each record on disk:  [key_len: 4 bytes][val_len: 4 bytes][key][value]
# A delete (tombstone) uses val_len = -1  (stored as unsigned max via struct trick)
# keydir maps:  key  ->  (offset, val_size)

TOMBSTONE = b"\x00__TOMBSTONE__\x00"
HEADER = struct.Struct(">II")   # two unsigned ints: key_len, val_len (4 bytes each)

# ---------- low-level record helpers ----------

def _encode(key: str, value: bytes) -> bytes:
    k = key.encode()
    return HEADER.pack(len(k), len(value)) + k + value

def _read_record(f, offset: int):
    """Return (key, value_bytes) from file at offset. value is None for tombstone."""
    f.seek(offset)
    header = f.read(HEADER.size)
    if len(header) < HEADER.size:
        return None, None
    key_len, val_len = HEADER.unpack(header)
    key = f.read(key_len).decode()
    value = f.read(val_len)
    return key, None if value == TOMBSTONE else value

# ---------- startup: rebuild keydir from log ----------

def open_db(path: str) -> dict:
    """
    Returns keydir dict.
    Replays the whole log to rebuild the in-memory index.
    """
    keydir = {}
    if not os.path.exists(path):
        return keydir
    with open(path, "rb") as f:
        offset = 0
        while True:
            header = f.read(HEADER.size)
            if len(header) < HEADER.size:
                break
            key_len, val_len = HEADER.unpack(header)
            key = f.read(key_len).decode()
            value = f.read(val_len)
            record_size = HEADER.size + key_len + val_len
            if value == TOMBSTONE:
                keydir.pop(key, None)          # deleted key
            else:
                keydir[key] = (offset, val_len)
            offset += record_size
    return keydir

# ---------- the three commands ----------

def db_set(path: str, keydir: dict, key: str, value: str):
    """Append a new record and update the keydir."""
    data = _encode(key, value.encode())
    with open(path, "ab") as f:
        offset = f.tell()
        f.write(data)
    val_len = len(value.encode())
    keydir[key] = (offset + HEADER.size + len(key.encode()), val_len)


def db_get(path: str, keydir: dict, key: str) -> str | None:
    """Seek directly to offset — no scan."""
    if key not in keydir:
        return None
    offset, val_len = keydir[key]
    with open(path, "rb") as f:
        f.seek(offset)
        return f.read(val_len).decode()


def db_delete(path: str, keydir: dict, key: str):
    """Append a tombstone record and remove from keydir."""
    if key not in keydir:
        return
    data = _encode(key, TOMBSTONE)
    with open(path, "ab") as f:
        f.write(data)
    del keydir[key]


DB = "mydb.log"

keydir = open_db(DB)

db_set(DB, keydir, "name", "burhan")
db_set(DB, keydir, "city", "lahore")
#db_set(DB, keydir, "name", "ali")     # overwrites — old record still on disk

print(db_get(DB, keydir, "name"))     # ali
print(db_get(DB, keydir, "city"))     # lahore

db_delete(DB, keydir, "city")
print(db_get(DB, keydir, "city"))     # None

# If you restart Python, open_db replays the log and rebuilds keydir
keydir2 = open_db(DB)
print(db_get(DB, keydir2, "name")) 