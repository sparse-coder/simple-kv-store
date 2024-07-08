import io
from binascii import crc32
from typing import IO


class HashMap:
    def __init__(self) -> None:
        self._items = dict()

    def get(self, key: bytes) -> int:
        return self._items[key]

    def insert(self, key: bytes, value: int):
        self._items[key] = value

    def remove(self, key: bytes):
        self._items.pop(key)


class KVStore:
    __slots__ = ["_f", "_index"]
    byte_order = "little"

    def __init__(self, file: IO, index: HashMap) -> None:
        self._f = file
        self._index = index

    def load(self):
        f = self._f
        f.seek(0, io.SEEK_SET)
        while True:
            pos = f.seek(0, io.SEEK_CUR)
            try:
                maybe_kv = self._process_record(f)
                self._index.insert(maybe_kv[0], pos)
            except EOFError:
                break

    def get(self, key: str):
        k = key.encode()
        try:
            pos = self._index.get(k)
            kv = self._get_at(pos)
            return bytes(kv[1]).decode()
        except KeyError:
            raise KeyError(key)

    def delete(self, key: str):
        try:
            # self._index.remove(key.encode())
            self.insert_or_update(key, "")
        except KeyError:
            raise KeyError(key)

    def insert_or_update(self, key: str, value: str):
        k = key.encode()
        v = value.encode()
        idx = self._insert_but_ignore_index(k, v)
        self._index.insert(k, idx)

    def _get_at(self, pos: int):
        f = self._f
        f.seek(pos, io.SEEK_SET)
        kv = self._process_record(f)
        # reset to end
        f.seek(0, io.SEEK_END)
        return kv

    def _process_record(self, f: IO):
        N_BYTES = 4
        eof = f.peek(N_BYTES)
        if eof == b"":
            raise EOFError()
        saved_checksum = f.read(N_BYTES)
        saved_checksum = int.from_bytes(saved_checksum, byteorder=self.byte_order)
        key_len = f.read(N_BYTES)
        key_len = int.from_bytes(key_len, byteorder=self.byte_order)
        value_len = f.read(N_BYTES)
        value_len = int.from_bytes(value_len, byteorder=self.byte_order)

        data_len = key_len + value_len
        data = f.read(data_len)
        assert len(data) == data_len
        checksum = crc32(data)

        if checksum != saved_checksum:
            raise RuntimeError("data corruption encountered")
        key = data[0:key_len]
        value = data[key_len:]

        return (key, value)

    def _insert_but_ignore_index(self, key: bytes, value: bytes):
        f = self._f
        N_BYTES = 4
        key_len = len(key)
        value_len = len(value)

        tmp = bytearray()
        for byte in key:
            tmp.append(byte)
        for byte in value:
            tmp.append(byte)

        checksum = crc32(tmp)

        # make sure we are writing at end
        current = f.seek(0, io.SEEK_END)
        # f.seek(0, io.SEEK_END)
        f.write(checksum.to_bytes(N_BYTES, byteorder=self.byte_order))
        f.write(key_len.to_bytes(N_BYTES, byteorder=self.byte_order))
        f.write(value_len.to_bytes(N_BYTES, byteorder=self.byte_order))
        f.write(tmp)
        return current

    @classmethod
    def open(cls, path):
        file = open(path, mode="ab+")
        return KVStore(file, HashMap())
