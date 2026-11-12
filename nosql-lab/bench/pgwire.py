"""
Мінімальний клієнт PostgreSQL на чистому Python (лише стандартна бібліотека).

Навіщо власний клієнт, а не psycopg2?
  * бенчмарк не потребує жодних pip-пакетів (однаково працює в Linux/WSL/Windows);
  * Redis-клієнт (resp.py) теж написаний вручну в тому самому стилі, тож обидві БД
    опитуються через однаково "тонкий" код: різниця в якості драйверів не впливає на порівняння.

Підтримується: протокол 3.0, автентифікація trust / cleartext / md5 / SCRAM-SHA-256
(типова для PostgreSQL 14+), простий запит (Query) та розширений протокол
з іменованими підготовленими запитами (Parse/Bind/Execute) - так само працюють
драйвери в режимі prepared statements.
"""
import base64
import hashlib
import hmac
import os
import socket
import struct


class PGError(Exception):
    pass


def _msg(tag: bytes, body: bytes) -> bytes:
    return tag + struct.pack("!I", len(body) + 4) + body


def _parse_error(body: bytes) -> str:
    fields = {}
    for part in body.split(b"\0"):
        if part:
            fields[chr(part[0])] = part[1:].decode("utf-8", "replace")
    return f"{fields.get('S', 'ERROR')} {fields.get('C', '')}: {fields.get('M', '')}"


def _datarow(body: bytes):
    (n,) = struct.unpack_from("!H", body, 0)
    off = 2
    row = []
    for _ in range(n):
        (ln,) = struct.unpack_from("!i", body, off)
        off += 4
        if ln < 0:
            row.append(None)
        else:
            row.append(body[off:off + ln].decode("utf-8"))
            off += ln
    return row


class PG:
    def __init__(self, host="127.0.0.1", port=5432, user="postgres", password="",
                 database="postgres", timeout=120):
        self.password = password
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = bytearray()
        self.last_tag = ""
        self._startup(user, database)

    # ------------------------------------------------------------ транспорт
    def _fill(self, need: int):
        while len(self.buf) < need:
            chunk = self.sock.recv(262144)
            if not chunk:
                raise ConnectionError("PostgreSQL closed the connection")
            self.buf += chunk

    def _read_msg(self):
        self._fill(5)
        tag = bytes(self.buf[0:1])
        (ln,) = struct.unpack_from("!I", self.buf, 1)
        self._fill(1 + ln)
        body = bytes(self.buf[5:1 + ln])
        del self.buf[:1 + ln]
        return tag, body

    def _send(self, data: bytes):
        self.sock.sendall(data)

    # ------------------------------------------------------- підключення
    def _startup(self, user: str, database: str):
        params = (b"user\0" + user.encode() + b"\0database\0" + database.encode()
                  + b"\0client_encoding\0UTF8\0\0")
        self._send(struct.pack("!II", 8 + len(params), 196608) + params)
        while True:
            tag, body = self._read_msg()
            if tag == b"R":
                (code,) = struct.unpack_from("!I", body, 0)
                if code == 0:
                    continue
                elif code == 3:
                    self._send(_msg(b"p", self.password.encode() + b"\0"))
                elif code == 5:
                    inner = hashlib.md5((self.password + user).encode()).hexdigest()
                    outer = "md5" + hashlib.md5(inner.encode() + body[4:8]).hexdigest()
                    self._send(_msg(b"p", outer.encode() + b"\0"))
                elif code == 10:
                    self._scram_first(body[4:])
                elif code == 11:
                    self._scram_continue(body[4:])
                elif code == 12:
                    self._scram_final(body[4:])
                else:
                    raise PGError(f"unsupported authentication method {code}")
            elif tag == b"E":
                raise PGError(_parse_error(body))
            elif tag == b"Z":
                return

    # ------------------------------------------------------------ SCRAM
    def _scram_first(self, mechanisms: bytes):
        if b"SCRAM-SHA-256" not in mechanisms:
            raise PGError("server offers no SCRAM-SHA-256")
        self._nonce = base64.b64encode(os.urandom(18)).decode()
        self._first_bare = f"n=,r={self._nonce}"
        first = ("n,," + self._first_bare).encode()
        self._send(_msg(b"p", b"SCRAM-SHA-256\0" + struct.pack("!i", len(first)) + first))

    def _scram_continue(self, data: bytes):
        server_first = data.decode()
        attrs = dict(p.split("=", 1) for p in server_first.split(","))
        nonce, salt, iters = attrs["r"], base64.b64decode(attrs["s"]), int(attrs["i"])
        if not nonce.startswith(self._nonce):
            raise PGError("SCRAM nonce mismatch")
        salted = hashlib.pbkdf2_hmac("sha256", self.password.encode(), salt, iters)
        client_key = hmac.new(salted, b"Client Key", "sha256").digest()
        stored_key = hashlib.sha256(client_key).digest()
        final_wo_proof = f"c=biws,r={nonce}"
        auth_message = f"{self._first_bare},{server_first},{final_wo_proof}".encode()
        signature = hmac.new(stored_key, auth_message, "sha256").digest()
        proof = bytes(a ^ b for a, b in zip(client_key, signature))
        server_key = hmac.new(salted, b"Server Key", "sha256").digest()
        self._server_sig = base64.b64encode(hmac.new(server_key, auth_message, "sha256").digest()).decode()
        final = f"{final_wo_proof},p={base64.b64encode(proof).decode()}"
        self._send(_msg(b"p", final.encode()))

    def _scram_final(self, data: bytes):
        if data.decode() != "v=" + self._server_sig:
            raise PGError("SCRAM server signature mismatch")

    # ------------------------------------------------------------ запити
    def _collect(self):
        rows, err = [], None
        while True:
            tag, body = self._read_msg()
            if tag == b"D":
                rows.append(_datarow(body))
            elif tag == b"C":
                self.last_tag = body[:-1].decode()
            elif tag == b"E":
                err = _parse_error(body)
            elif tag == b"Z":
                break
        if err:
            raise PGError(err)
        return rows

    def query(self, sql: str):
        """Простий протокол: DDL, BEGIN/COMMIT, масове завантаження. Повертає рядки (list[list[str]])."""
        self._send(_msg(b"Q", sql.encode() + b"\0"))
        return self._collect()

    def prepare(self, name: str, sql: str):
        self._send(_msg(b"P", name.encode() + b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
                   + _msg(b"S", b""))
        self._collect()

    def execute(self, name: str, params=()):
        """Виконує підготовлений запит у режимі autocommit. Параметри передаються текстом."""
        body = b"\0" + name.encode() + b"\0" + struct.pack("!HH", 0, len(params))
        for p in params:
            if p is None:
                body += struct.pack("!i", -1)
            else:
                b = str(p).encode()
                body += struct.pack("!i", len(b)) + b
        body += struct.pack("!H", 0)
        self._send(_msg(b"B", body) + _msg(b"E", b"\0" + struct.pack("!i", 0)) + _msg(b"S", b""))
        return self._collect()

    def close(self):
        try:
            self._send(_msg(b"X", b""))
        finally:
            self.sock.close()
