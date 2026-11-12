#!/usr/bin/env python3
"""
Бенчмарк: PostgreSQL (SQL) проти Redis (NoSQL, ключ-значення) на даних інтернет-магазину.

Команди:
    python bench.py setup    # схема + завантаження однакових даних в обидві БД
    python bench.py verify   # звірка, що обидві БД повертають однакові результати
    python bench.py run      # повний набір тестів -> results/results.json та results.md
    python bench.py report   # перегенерувати results.md з results.json

Лише стандартна бібліотека Python 3.8+ (клієнти PostgreSQL і Redis - у pgwire.py та resp.py).
Умови й обґрунтування тестів - у docs/METHODOLOGY.md та у звіті.
"""
import argparse
import array
import json
import multiprocessing as mp
import os
import platform
import random
import statistics
import sys
import time
from collections import defaultdict
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from pgwire import PG, PGError          # noqa: E402
from resp import Redis                   # noqa: E402
import data                              # noqa: E402

SQL_DIR = os.path.join(HERE, "..", "sql")
RESULTS_DIR = os.path.join(HERE, "..", "results")

# Режими надійності: однакові за суттю гарантії для обох СУБД (див. методику)
CONFIGS = {
    # без fsync на кожну операцію: ризик втратити останні мс/секунди при збої
    "fast":     dict(redis_aof=False, redis_fsync=None,       pg_sync="off"),
    # Redis AOF everysec ~ PostgreSQL synchronous_commit=off: втрата не більше ~1 с
    "balanced": dict(redis_aof=True,  redis_fsync="everysec", pg_sync="off"),
    # fsync на кожну операцію: Redis appendfsync always ~ PostgreSQL synchronous_commit=on
    "durable":  dict(redis_aof=True,  redis_fsync="always",   pg_sync="on"),
}

# ============================================================== SQL-запити
SQL_GET_CART = """
SELECT ci.offer_id, ci.quantity
  FROM carts c JOIN cart_items ci ON ci.cart_id = c.id
 WHERE c.user_id = $1 AND c.store_id = $2"""

SQL_GET_CART_DETAILS = """
SELECT o.id, o.name, o.price, ci.quantity
  FROM carts c
  JOIN cart_items ci ON ci.cart_id = c.id
  JOIN offers o      ON o.id = ci.offer_id
 WHERE c.user_id = $1 AND c.store_id = $2"""

# Один атомарний запит: створити кошик за потреби (або оновити updated_at) і додати/збільшити позицію
SQL_ADD_ITEM = """
WITH c AS (
    INSERT INTO carts (user_id, store_id) VALUES ($1::int, $2::int)
    ON CONFLICT (user_id, store_id) DO UPDATE SET updated_at = now()
    RETURNING id)
INSERT INTO cart_items (cart_id, offer_id, quantity)
SELECT id, $3::int, $4::int FROM c
ON CONFLICT (cart_id, offer_id) DO UPDATE SET quantity = cart_items.quantity + EXCLUDED.quantity"""

SQL_CLEAR_CART = """
DELETE FROM cart_items
 WHERE cart_id = (SELECT id FROM carts WHERE user_id = $1 AND store_id = $2)"""

SQL_VIEW_INC = """
INSERT INTO offer_views_daily (day, store_id, offer_id, views) VALUES ($1::date, $2::int, $3::int, 1)
ON CONFLICT (day, store_id, offer_id) DO UPDATE SET views = offer_views_daily.views + 1"""

SQL_TOP10 = """
SELECT offer_id, views FROM offer_views_daily
 WHERE day = $1::date AND store_id = $2::int
 ORDER BY views DESC LIMIT 10"""

SQL_ANALYTIC = """
SELECT c.store_id, SUM(ci.quantity * o.price)
  FROM carts c
  JOIN cart_items ci ON ci.cart_id = c.id
  JOIN offers o      ON o.id = ci.offer_id
 GROUP BY c.store_id ORDER BY c.store_id"""


# ===================================================== підключення / набір даних
_DATASET = None


def get_dataset(a):
    global _DATASET
    if _DATASET is None:
        _DATASET = data.Dataset(a.stores, a.offers_per_store, a.users, a.seed)
    return _DATASET


def connect_pg(a, cfg=None):
    c = PG(a.pg_host, a.pg_port, a.pg_user, a.pg_password, a.pg_db)
    c.query("SET search_path = public")
    if cfg:
        c.query(f"SET synchronous_commit = {CONFIGS[cfg]['pg_sync']}")
    return c


def connect_redis(a):
    return Redis(a.redis_host, a.redis_port, a.redis_password or None, a.redis_db)


def connect(system, a, cfg=None):
    return connect_pg(a, cfg) if system == "pg" else connect_redis(a)


# ================================================================ операції
def build_ops(system, conn, ds):
    """Повертає словник: ім'я операції -> функція(*параметри). Для PG підготовлюються запити."""
    D = data.DAY
    if system == "pg":
        for name, sql in (("get_cart", SQL_GET_CART), ("get_cart_details", SQL_GET_CART_DETAILS),
                          ("add_item", SQL_ADD_ITEM), ("clear_cart", SQL_CLEAR_CART),
                          ("view_inc", SQL_VIEW_INC), ("top10", SQL_TOP10)):
            conn.prepare(name, sql)

        def get_cart(user, store):
            return conn.execute("get_cart", (user, store))

        def get_cart_details(user, store):
            rows = conn.execute("get_cart_details", (user, store))
            total = Decimal(0)
            for _oid, _name, price, qty in rows:
                total += Decimal(price) * int(qty)
            return rows, total

        def add_item(user, store, offer, qty):
            return conn.execute("add_item", (user, store, offer, qty))

        def clear_cart(user, store):
            return conn.execute("clear_cart", (user, store))

        def view_inc(store, offer):
            return conn.execute("view_inc", (D, store, offer))

        def top10(store):
            return conn.execute("top10", (D, store))
    else:
        r = conn
        ttl = data.CART_TTL

        def get_cart(user, store):
            return r.cmd("HGETALL", f"cart:{user}:{store}")

        def get_cart_details(user, store):
            flat = r.cmd("HGETALL", f"cart:{user}:{store}")
            ids, qtys = flat[0::2], flat[1::2]
            if not ids:
                return [], Decimal(0)
            replies = r.pipeline([("HMGET", b"offer:" + i, "name", "price") for i in ids])
            total = Decimal(0)
            for q, (_name, price) in zip(qtys, replies):
                total += Decimal(price.decode()) * int(q)
            return replies, total

        def add_item(user, store, offer, qty):
            key = f"cart:{user}:{store}"
            return r.pipeline([("HINCRBY", key, offer, qty), ("EXPIRE", key, ttl)])

        def clear_cart(user, store):
            return r.cmd("DEL", f"cart:{user}:{store}")

        def view_inc(store, offer):
            return r.cmd("ZINCRBY", f"views:{D}:{store}", 1, offer)

        def top10(store):
            return r.cmd("ZREVRANGE", f"views:{D}:{store}", 0, 9, "WITHSCORES")

    return dict(get_cart=get_cart, get_cart_details=get_cart_details, add_item=add_item,
                clear_cart=clear_cart, view_inc=view_inc, top10=top10)


# Вибір параметрів: виконується ПОЗА виміряним інтервалом
def pick_cart(ds, rnd):
    c = ds.carts[rnd.randrange(len(ds.carts))]
    return (c[1], c[2])


def pick_add(ds, rnd):
    c = ds.carts[rnd.randrange(len(ds.carts))]
    return (c[1], c[2], rnd.choice(ds.store_offers[c[2]]), 1)


def pick_view(ds, rnd):
    store = ds.store_zipf.sample(rnd) + 1                  # "гарячі" магазини (розпродаж)
    offer = ds.hot_order[store][ds.offer_zipf.sample(rnd)]   # "гарячі" товари магазину
    return (store, offer)


def pick_store(ds, rnd):
    return (rnd.randint(1, ds.stores),)


PICKS = dict(get_cart=pick_cart, get_cart_details=pick_cart, add_item=pick_add,
             clear_cart=pick_cart, view_inc=pick_view, top10=pick_store)


def prep_clear(ops, ds, rnd, params):
    """Невиміряна підготовка: гарантуємо, що в кошику є що видаляти."""
    user, store = params
    for oid in rnd.sample(ds.store_offers[store], 3):
        ops["add_item"](user, store, oid, 1)


PREPS = dict(clear_cart=prep_clear)
MIXED = (("get_cart_details", 60), ("add_item", 25), ("view_inc", 15))   # типова "сесія покупця"


def make_runner(system, conn, ds, name):
    ops = build_ops(system, conn, ds)
    if name == "mixed":
        def nxt(rnd):
            x, acc = rnd.random() * 100, 0
            for n, w in MIXED:
                acc += w
                if x < acc:
                    break
            return ops[n], PICKS[n](ds, rnd)
    else:
        op, pick, prep = ops[name], PICKS[name], PREPS.get(name)

        def nxt(rnd):
            params = pick(ds, rnd)
            if prep:
                prep(ops, ds, rnd, params)
            return op, params
    return nxt


# ============================================================ статистика
def stats(lat_ns, duration_s=None):
    s = sorted(lat_ns)
    n = len(s)
    return dict(n=n,
                p50_us=s[n // 2] / 1000, p95_us=s[int(n * 0.95)] / 1000, p99_us=s[min(n - 1, int(n * 0.99))] / 1000,
                mean_us=sum(s) / n / 1000,
                ops_s=(n / duration_s) if duration_s else 1e9 * n / sum(s))


def summarize(reps):
    keys = ("p50_us", "p95_us", "p99_us", "mean_us", "ops_s")
    out = {k: statistics.median(r[k] for r in reps) for k in keys}
    out["p50_min_us"] = min(r["p50_us"] for r in reps)
    out["p50_max_us"] = max(r["p50_us"] for r in reps)
    out["ops_s_min"] = min(r["ops_s"] for r in reps)
    out["ops_s_max"] = max(r["ops_s"] for r in reps)
    out["n_per_rep"] = reps[0]["n"]
    out["reps"] = len(reps)
    return out


# ============================================== послідовні тести (затримка)
def run_sequential(system, a, cfg, name, n, warmup, reps, seed=1):
    ds = get_dataset(a)
    conn = connect(system, a, cfg)
    nxt = make_runner(system, conn, ds, name)
    rnd = random.Random(seed)
    for _ in range(warmup):
        fn, params = nxt(rnd)
        fn(*params)
    out = []
    pc = time.perf_counter_ns
    for _ in range(reps):
        lat = array.array("q")
        for _ in range(n):
            fn, params = nxt(rnd)
            t0 = pc()
            fn(*params)
            lat.append(pc() - t0)
        out.append(stats(lat))
    conn.close()
    return out


# ===================================== паралельні тести (пропускна здатність)
def _worker(system, a, cfg, name, duration, warmup, seed, ready, start, out):
    try:
        ds = get_dataset(a)
        conn = connect(system, a, cfg)
        nxt = make_runner(system, conn, ds, name)
        rnd = random.Random(seed)
        ready.put(1)
        start.wait()
        pc = time.perf_counter_ns
        end_warm = pc() + int(warmup * 1e9)
        while pc() < end_warm:
            fn, params = nxt(rnd)
            try:
                fn(*params)
            except PGError:
                pass
        lat, errors = array.array("q"), 0
        end = pc() + int(duration * 1e9)
        while True:
            fn, params = nxt(rnd)
            t0 = pc()
            try:
                fn(*params)
            except PGError:
                errors += 1
                continue
            t1 = pc()
            lat.append(t1 - t0)
            if t1 >= end:
                break
        out.send((errors, lat.tobytes()))
    except Exception as e:      # noqa: BLE001
        out.send((-1, repr(e).encode()))
        raise


def run_concurrent(system, a, cfg, name, workers, duration, warmup, reps, seed=100):
    ctx = mp.get_context("fork" if hasattr(os, "fork") else "spawn")
    results = []
    for rep in range(reps):
        ready, start = ctx.Queue(), ctx.Event()
        procs, pipes = [], []
        for w in range(workers):
            rx, tx = ctx.Pipe(False)
            p = ctx.Process(target=_worker, daemon=True,
                            args=(system, a, cfg, name, duration, warmup, seed + rep * 1000 + w, ready, start, tx))
            p.start()
            procs.append(p)
            pipes.append(rx)
        for _ in range(workers):
            ready.get(timeout=600)
        start.set()
        lat, errors = array.array("q"), 0
        for rx in pipes:
            err, payload = rx.recv()
            if err < 0:
                raise RuntimeError(f"worker failed: {payload.decode()}")
            errors += err
            chunk = array.array("q")
            chunk.frombytes(payload)
            lat.extend(chunk)
        for p in procs:
            p.join()
        st = stats(lat, duration)
        st["errors"] = errors
        results.append(st)
    return results


# ============================================================ конфігурація
def wait_aof(r, timeout=300):
    t0 = time.time()
    while time.time() - t0 < timeout:
        info = r.cmd("INFO", "persistence").decode()
        if "aof_rewrite_in_progress:0" in info and "aof_rewrite_scheduled:0" in info:
            return
        time.sleep(0.2)
    raise RuntimeError("AOF rewrite did not finish")


def apply_redis_config(a, cfg_name):
    cfg = CONFIGS[cfg_name]
    r = connect_redis(a)
    r.cmd("CONFIG", "SET", "save", "")                            # без RDB-знімків під час тесту
    r.cmd("CONFIG", "SET", "auto-aof-rewrite-percentage", "0")    # без автоматичного перезапису AOF
    if cfg["redis_aof"]:
        r.cmd("CONFIG", "SET", "appendfsync", cfg["redis_fsync"])
        r.cmd("CONFIG", "SET", "appendonly", "yes")
        wait_aof(r)
    else:
        r.cmd("CONFIG", "SET", "appendonly", "no")
    r.close()


def pg_checkpoint(a):
    c = connect_pg(a)
    try:
        c.query("CHECKPOINT")
    except PGError:
        pass       # звичайному користувачу заборонено - не критично
    c.close()


# ============================================================ аналітика
def pg_analytic(conn):
    return {int(s): Decimal(t) for s, t in conn.query(SQL_ANALYTIC)}


def redis_analytic(r, n_offers):
    prices = {}
    for lo in range(1, n_offers + 1, 5000):
        ids = range(lo, min(lo + 5000, n_offers + 1))
        for i, p in zip(ids, r.pipeline([("HGET", f"offer:{i}", "price") for i in ids])):
            prices[i] = Decimal(p.decode())
    totals = defaultdict(Decimal)
    cursor = b"0"
    while True:
        cursor, keys = r.cmd("SCAN", cursor, "MATCH", "cart:*", "COUNT", 5000)
        if keys:
            for k, flat in zip(keys, r.pipeline([("HGETALL", k) for k in keys])):
                store = int(k.rsplit(b":", 1)[1])
                for i in range(0, len(flat), 2):
                    totals[store] += prices[int(flat[i])] * int(flat[i + 1])
        if cursor == b"0":
            break
    return dict(totals)


# ============================================================== перевірка
def verify(a):
    ds = get_dataset(a)
    pg, r = connect_pg(a), connect_redis(a)
    po, ro = build_ops("pg", pg, ds), build_ops("redis", r, ds)
    rnd = random.Random(7)
    for _ in range(500):
        user, store = pick_cart(ds, rnd)
        p = {int(o): int(q) for o, q in po["get_cart"](user, store)}
        flat = ro["get_cart"](user, store)
        rr = {int(flat[i]): int(flat[i + 1]) for i in range(0, len(flat), 2)}
        assert p == rr, f"get_cart differs for {user},{store}: {p} != {rr}"
        assert po["get_cart_details"](user, store)[1] == ro["get_cart_details"](user, store)[1], "cart total differs"
    for store in range(1, ds.stores + 1):
        p = [(int(o), int(v)) for o, v in po["top10"](store)]
        flat = ro["top10"](store)
        rr = [(int(flat[i]), int(float(flat[i + 1]))) for i in range(0, len(flat), 2)]
        assert p == rr, f"top10 differs for store {store}"
    assert pg_analytic(pg) == redis_analytic(r, len(ds.offers)), "analytic totals differ"
    n_pg = int(pg.query("SELECT count(*) FROM carts")[0][0])
    n_rd = len(ds.carts)
    assert n_pg == n_rd, "cart count differs"
    pg.close()
    r.close()
    print(f"OK: PostgreSQL і Redis повертають однакові результати ({ds.describe()})")


# ================================================================= setup
def storage_snapshot(pg, r, ds):
    info = r.cmd("INFO", "memory").decode()
    used = int(next(l for l in info.splitlines() if l.startswith("used_memory:")).split(":")[1])
    base = int(next(l for l in info.splitlines() if l.startswith("used_memory_startup:")).split(":")[1])
    rnd = random.Random(3)
    sample = [f"cart:{c[1]}:{c[2]}" for c in rnd.sample(ds.carts, 1000)]
    cart_bytes = [r.cmd("MEMORY", "USAGE", k) for k in sample]
    sizes = {t: int(pg.query(f"SELECT pg_total_relation_size('{t}')")[0][0])
             for t in ("offers", "carts", "cart_items", "offer_views_daily")}
    return dict(redis_used_memory=used - base, redis_cart_key_avg_bytes=statistics.mean(cart_bytes),
                pg_total_bytes=sum(sizes.values()), pg_table_bytes=sizes,
                pg_cart_bytes_per_cart=(sizes["carts"] + sizes["cart_items"]) / len(ds.carts),
                carts=len(ds.carts), items=ds.n_items)


def cmd_setup(a):
    ds = get_dataset(a)
    print("Дані:", ds.describe())
    pg, r = connect_pg(a), connect_redis(a)
    t = time.time()
    pg.query(open(os.path.join(SQL_DIR, "01_schema.sql"), encoding="utf-8").read())
    data.load_pg(pg, ds)
    pg.query(open(os.path.join(SQL_DIR, "02_indexes.sql"), encoding="utf-8").read())
    pg.query("ANALYZE")
    print(f"PostgreSQL завантажено за {time.time() - t:.1f} с")
    t = time.time()
    data.load_redis(r, ds)
    print(f"Redis завантажено за {time.time() - t:.1f} с")
    snap = storage_snapshot(pg, r, ds)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    json.dump(snap, open(os.path.join(RESULTS_DIR, "storage.json"), "w"), indent=1)
    print("Обсяг: Redis {:.1f} МБ, PostgreSQL {:.1f} МБ".format(
        snap["redis_used_memory"] / 2**20, snap["pg_total_bytes"] / 2**20))
    pg.close()
    r.close()


# ================================================================== run
def environment(a):
    pg, r = connect_pg(a), connect_redis(a)
    env = dict(
        pg_version=pg.query("SHOW server_version")[0][0],
        pg_shared_buffers=pg.query("SHOW shared_buffers")[0][0],
        pg_fsync=pg.query("SHOW fsync")[0][0],
        redis_version=next(l.split(":")[1] for l in r.cmd("INFO", "server").decode().splitlines()
                           if l.startswith("redis_version")),
        cpus=os.cpu_count(), python=platform.python_version(), platform=platform.platform(),
        dataset=get_dataset(a).describe())
    pg.close()
    r.close()
    return env


def cmd_run(a):
    ds = get_dataset(a)
    print("Дані:", ds.describe())
    verify(a)
    q = a.quick
    seq_read, seq_write = (500, 300) if q else (a.n_read, a.n_write)
    warm_seq = 200 if q else 2000
    reps_seq = 1 if q else a.reps
    dur = 1.0 if q else a.duration
    warm_conc = 0.3 if q else 1.0
    reps_conc = 1 if q else a.conc_reps
    workers = [1, 4] if q else a.workers
    results = []

    def rec(workload, system, cfg, mode, w, reps, variant=""):
        results.append(dict(workload=workload, system=system, config=cfg, mode=mode, workers=w,
                            variant=variant, summary=summarize(reps), reps=reps))
        s = results[-1]["summary"]
        print(f"  {workload:17s} {system:5s} {cfg:9s} {mode[:4]} w={w:<2d} {variant:12s} "
              f"p50={s['p50_us']:9.1f}µs p99={s['p99_us']:9.1f}µs {s['ops_s']:10.0f} оп/с", flush=True)

    def seq(workload, cfg, n, systems=("pg", "redis"), variant=""):
        for sysname in systems:
            rec(workload, sysname, cfg, "sequential", 1,
                run_sequential(sysname, a, cfg, workload, n, warm_seq, reps_seq), variant)

    def conc(workload, cfg, systems=("pg", "redis"), variant=""):
        for w in workers:
            for sysname in systems:
                rec(workload, sysname, cfg, "concurrent", w,
                    run_concurrent(sysname, a, cfg, workload, w, dur, warm_conc, reps_conc), variant)

    env = environment(a)
    print("Середовище:", json.dumps(env, ensure_ascii=False))

    print("\n== Фаза 1: читання та аналітика (режим fast) ==")
    apply_redis_config(a, "fast")
    # аналітичний запит: один запуск = одне виконання
    for sysname in ("pg", "redis"):
        times = []
        conn = connect(sysname, a, "fast")
        for _ in range(1 if q else 3):
            t0 = time.perf_counter_ns()
            pg_analytic(conn) if sysname == "pg" else redis_analytic(conn, len(ds.offers))
            times.append(time.perf_counter_ns() - t0)
        conn.close()
        rep = [dict(n=1, p50_us=t / 1000, p95_us=t / 1000, p99_us=t / 1000, mean_us=t / 1000, ops_s=1e9 / t)
               for t in times]
        rec("analytic_cart_value", sysname, "fast", "sequential", 1, rep)
    for wl in ("get_cart", "get_cart_details", "top10"):
        seq(wl, "fast", seq_read)
    conc("get_cart_details", "fast")

    for cfg in ("fast", "balanced", "durable"):
        print(f"\n== Фаза 2: запис, режим надійності '{cfg}' ==")
        apply_redis_config(a, cfg)
        pg_checkpoint(a)
        for wl in ("add_item", "view_inc", "clear_cart"):
            seq(wl, cfg, seq_write)
        for wl in ("add_item", "view_inc", "mixed"):
            conc(wl, cfg)

    if not a.no_index_variant:
        print("\n== Фаза 3: варіант SQL без індексу ix_views_top (режим balanced) ==")
        apply_redis_config(a, "balanced")
        c = connect_pg(a)
        c.query("DROP INDEX IF EXISTS ix_views_top")
        c.query("ANALYZE offer_views_daily")
        c.close()
        try:
            seq("view_inc", "balanced", seq_write, ("pg",), "no_views_idx")
            seq("top10", "balanced", seq_read, ("pg",), "no_views_idx")
            conc("view_inc", "balanced", ("pg",), "no_views_idx")
        finally:
            c = connect_pg(a)
            c.query(open(os.path.join(SQL_DIR, "02_indexes.sql"), encoding="utf-8").read().split("CREATE INDEX ix_offers_store")[0])
            c.close()

    apply_redis_config(a, "fast")
    os.makedirs(RESULTS_DIR, exist_ok=True)
    name = "results_quick.json" if q else "results.json"
    storage_path = os.path.join(RESULTS_DIR, "storage.json")
    storage = json.load(open(storage_path)) if os.path.exists(storage_path) else {}
    json.dump(dict(environment=env, storage=storage, args={k: v for k, v in vars(a).items() if "password" not in k},
                   results=results), open(os.path.join(RESULTS_DIR, name), "w"), indent=1, ensure_ascii=False)
    make_report(os.path.join(RESULTS_DIR, name))
    print(f"\nГотово: results/{name}")


# =============================================================== report
def make_report(path):
    doc = json.load(open(path))
    rows = defaultdict(dict)
    for r in doc["results"]:
        key = (r["workload"], r["config"], r["mode"], r["workers"], r["variant"])
        rows[key][r["system"]] = r["summary"]
    L = ["# Результати бенчмарку", "", "## Середовище", ""]
    for k, v in doc["environment"].items():
        L.append(f"- **{k}**: {v}")
    L += ["", "Усі значення - медіана за повторами. Затримка в мкс (менше = краще), пропускна здатність у операціях за секунду.", ""]

    def table(title, mode, cols):
        L.extend([f"## {title}", "", "| Операція | Режим | Потоків | Варіант | " + " | ".join(cols[0]) + " |",
                  "|---|---|---|---|" + "---|" * len(cols[0])])
        for key in sorted(rows):
            wl, cfg, md, w, var = key
            if md != mode:
                continue
            pg, rd = rows[key].get("pg"), rows[key].get("redis")
            L.append(f"| {wl} | {cfg} | {w} | {var or '-'} | " + " | ".join(cols[1](pg, rd)) + " |")
        L.append("")

    def f(x, d=0):
        return "-" if x is None else f"{x:,.{d}f}"

    def ratio(a_, b_):
        return "-" if not a_ or not b_ else f"{a_ / b_:.1f}x"

    table("Послідовні тести (одне з'єднання): затримка", "sequential",
          (["PG p50", "PG p99", "Redis p50", "Redis p99", "PG/Redis (p50)"],
           lambda pg, rd: [f(pg and pg["p50_us"], 1), f(pg and pg["p99_us"], 1), f(rd and rd["p50_us"], 1),
                           f(rd and rd["p99_us"], 1), ratio(pg and pg["p50_us"], rd and rd["p50_us"])]))
    table("Паралельні тести: пропускна здатність та затримка", "concurrent",
          (["PG оп/с", "Redis оп/с", "Redis/PG", "PG p99", "Redis p99"],
           lambda pg, rd: [f(pg and pg["ops_s"]), f(rd and rd["ops_s"]), ratio(rd and rd["ops_s"], pg and pg["ops_s"]),
                           f(pg and pg["p99_us"], 0), f(rd and rd["p99_us"], 0)]))
    if doc.get("storage"):
        s = doc["storage"]
        L += ["## Обсяг даних", "",
              f"- Redis: {s['redis_used_memory'] / 2**20:.1f} МБ; середній кошик (ключ HASH) ≈ {s['redis_cart_key_avg_bytes']:.0f} байт",
              f"- PostgreSQL (таблиці з індексами): {s['pg_total_bytes'] / 2**20:.1f} МБ; кошик (carts + cart_items) ≈ {s['pg_cart_bytes_per_cart']:.0f} байт", ""]
    open(path.replace(".json", ".md"), "w", encoding="utf-8").write("\n".join(L))


# ================================================================== main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["setup", "verify", "run", "report"])
    ap.add_argument("--pg-host", default="127.0.0.1")
    ap.add_argument("--pg-port", type=int, default=5432)
    ap.add_argument("--pg-user", default="postgres")
    ap.add_argument("--pg-password", default=os.environ.get("PGPASSWORD", ""))
    ap.add_argument("--pg-db", default="nosql_lab")
    ap.add_argument("--redis-host", default="127.0.0.1")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--redis-password", default="")
    ap.add_argument("--redis-db", type=int, default=5, help="логічна БД Redis (очищується при setup!)")
    ap.add_argument("--stores", type=int, default=50)
    ap.add_argument("--offers-per-store", type=int, default=2000)
    ap.add_argument("--users", type=int, default=100000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--n-read", type=int, default=20000, help="операцій читання в одному повторі")
    ap.add_argument("--n-write", type=int, default=10000, help="операцій запису в одному повторі")
    ap.add_argument("--reps", type=int, default=3, help="повторів послідовних тестів")
    ap.add_argument("--conc-reps", type=int, default=2, help="повторів паралельних тестів")
    ap.add_argument("--duration", type=float, default=4.0, help="тривалість паралельного тесту, с")
    ap.add_argument("--workers", type=int, nargs="+", default=[1, 4, 16])
    ap.add_argument("--quick", action="store_true", help="короткий прогін для перевірки роботи")
    ap.add_argument("--no-index-variant", action="store_true")
    ap.add_argument("--results-file", default=None)
    a = ap.parse_args()
    if a.command == "setup":
        cmd_setup(a)
    elif a.command == "verify":
        verify(a)
    elif a.command == "run":
        cmd_run(a)
    else:
        make_report(a.results_file or os.path.join(RESULTS_DIR, "results.json"))


if __name__ == "__main__":
    main()
