"""
Мінімальний клієнт Redis (протокол RESP2) на чистому Python - див. пояснення в pgwire.py.
Підтримує команди, pipeline (кілька команд за один round-trip), AUTH та SELECT <db>.
"""
import socket


class RedisError(Exception):
    pass


def _encode(args) -> bytes:
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(b), b))
    return b"".join(out)


class Redis:
    def __init__(self, host="127.0.0.1", port=6379, password=None, db=None, timeout=300):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.f = self.sock.makefile("rb", buffering=262144)
        if password:
            self.cmd("AUTH", password)
        if db is not None:
            self.cmd("SELECT", db)

    def _read(self):
        line = self.f.readline()
        if not line:
            raise ConnectionError("Redis closed the connection")
        tag, rest = line[:1], line[1:-2]
        if tag == b"+":
            return rest.decode()
        if tag == b":":
            return int(rest)
        if tag == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = self.f.read(n + 2)
            return data[:-2]
        if tag == b"*":
            n = int(rest)
            if n < 0:
                return None
            return [self._read() for _ in range(n)]
        if tag == b"-":
            return RedisError(rest.decode())
        raise RedisError(f"unexpected reply {line!r}")

    def cmd(self, *args):
        self.sock.sendall(_encode(args))
        r = self._read()
        if isinstance(r, RedisError):
            raise r
        return r

    def pipeline(self, commands):
        """commands: список кортежів-команд. Повертає список відповідей (один round-trip)."""
        self.sock.sendall(b"".join(_encode(c) for c in commands))
        replies = [self._read() for _ in commands]
        for r in replies:
            if isinstance(r, RedisError):
                raise r
        return replies

    def close(self):
        self.sock.close()
