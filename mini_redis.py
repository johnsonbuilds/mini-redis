#!/usr/bin/env python3
import socket
import threading
import time
import os

HOST = "0.0.0.0"
PORT = 63791
AOF_FILE = "appendonly.aof"

store = {}
expiry = {}
lock = threading.RLock()


# ------------------------
# RESP Parser
# ------------------------
def read_line(conn):
    data = b""
    while not data.endswith(b"\r\n"):
        chunk = conn.recv(1)
        if not chunk:
            return None
        data += chunk
    return data[:-2]


def parse_resp(conn):
    first = conn.recv(1)
    if not first:
        return None

    if first == b"*":  # array
        n = int(read_line(conn))
        parts = []
        for _ in range(n):
            conn.recv(1)  # $
            length = int(read_line(conn))
            data = conn.recv(length)
            conn.recv(2)  # \r\n
            parts.append(data.decode())
        return parts

    return None


# ------------------------
# RESP Writer
# ------------------------
def resp_simple(msg):
    return f"+{msg}\r\n".encode()


def resp_bulk(val):
    if val is None:
        return b"$-1\r\n"
    return f"${len(val)}\r\n{val}\r\n".encode()


def resp_int(i):
    return f":{i}\r\n".encode()


def resp_error(msg):
    return f"-{msg}\r\n".encode()


# ------------------------
# TTL
# ------------------------
def is_expired(key):
    if key in expiry:
        if time.time() > expiry[key]:
            store.pop(key, None)
            expiry.pop(key, None)
            return True
    return False


def ttl_cleaner():
    while True:
        now = time.time()
        with lock:
            expired_keys = [k for k, t in expiry.items() if now > t]
            for k in expired_keys:
                store.pop(k, None)
                expiry.pop(k, None)
        time.sleep(1)


# ------------------------
# AOF
# ------------------------
def append_aof(cmd):
    with lock:
        with open(AOF_FILE, "a") as f:
            f.write(cmd + "\n")
            f.flush()
            os.fsync(f.fileno())


def load_aof():
    if not os.path.exists(AOF_FILE):
        return

    with open(AOF_FILE) as f:
        for line in f:
            parts = line.strip().split()
            execute(parts, persist=False)


# ------------------------
# Command Execution
# ------------------------
def execute(cmd, persist=True):
    if not cmd:
        return resp_error("empty command")

    op = cmd[0].upper()

    try:
        if op == "PING":
            return resp_simple("PONG")

        if op == "SET":
            key = cmd[1]
            val = cmd[2]

            with lock:
                store[key] = val

                # TTL
                if len(cmd) > 3 and cmd[3].upper() == "EX":
                    ttl = int(cmd[4])
                    expiry[key] = time.time() + ttl

                if persist:
                    append_aof(" ".join(cmd))

            return resp_simple("OK")

        if op == "GET":
            key = cmd[1]
            with lock:
                if is_expired(key):
                    return resp_bulk(None)
                return resp_bulk(store.get(key))

        if op == "DEL":
            key = cmd[1]
            with lock:
                existed = 1 if key in store else 0
                store.pop(key, None)
                expiry.pop(key, None)

                if persist:
                    append_aof(" ".join(cmd))

            return resp_int(existed)

        if op == "EXPIRE":
            key = cmd[1]
            ttl = int(cmd[2])

            with lock:
                if key not in store:
                    return resp_int(0)

                expiry[key] = time.time() + ttl

                if persist:
                    append_aof(" ".join(cmd))

            return resp_int(1)

        return resp_error("unknown command")

    except Exception as e:
        return resp_error("error")


# ------------------------
# Client Handler
# ------------------------
def handle_client(conn):
    with conn:
        while True:
            try:
                cmd = parse_resp(conn)
                if not cmd:
                    break

                result = execute(cmd)
                conn.sendall(result)

            except Exception:
                try:
                    conn.sendall(resp_error("internal error"))
                except:
                    pass
                break


# ------------------------
# Server
# ------------------------
def main():
    load_aof()

    threading.Thread(target=ttl_cleaner, daemon=True).start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen()

    print(f"mini-redis running on {HOST}:{PORT}")

    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()