"""
Детермінований генератор даних і завантажувачі для PostgreSQL та Redis.
Одні й ті самі згенеровані дані завантажуються в обидві бази, тож порівняння чесне,
а результат кожного запиту можна звірити між системами (див. bench.py verify).
"""
import bisect
import random

DAY = "2026-11-12"          # "сьогоднішня" доба для лічильників переглядів
CART_TTL = 7 * 24 * 3600    # кошик забувається через 7 днів


def money(cents: int) -> str:
    return f"{cents // 100}.{cents % 100:02d}"


class Zipf:
    """Вибір рангу 0..n-1 із розподілом Ципфа (мала кількість "гарячих" елементів)."""
    def __init__(self, n, s):
        total = 0.0
        self.cum = []
        for k in range(1, n + 1):
            total += 1.0 / (k ** s)
            self.cum.append(total)
        self.total = total

    def sample(self, rnd):
        return bisect.bisect_left(self.cum, rnd.random() * self.total)


class Dataset:
    def __init__(self, stores=50, offers_per_store=2000, users=100000, seed=42):
        self.stores = stores
        self.offers_per_store = offers_per_store
        self.users = users
        self.seed = seed
        rnd = random.Random(seed)

        # --- пропозиції (offers): id 1..N, магазин за залишком від ділення
        self.offers = []                        # (id, store_id, name, price_cents)
        self.store_offers = {s: [] for s in range(1, stores + 1)}
        for oid in range(1, stores * offers_per_store + 1):
            s = (oid - 1) % stores + 1
            self.offers.append((oid, s, f"Item {oid}", rnd.randint(500, 500000)))
            self.store_offers[s].append(oid)
        self.price_cents = {o[0]: o[3] for o in self.offers}

        # --- кошики: у кожного користувача 1-3 кошики у різних магазинах, по 1-7 позицій
        self.carts = []                         # (cart_id, user_id, store_id, [(offer_id, qty), ...])
        cart_id = 0
        for user in range(1, users + 1):
            k = rnd.choices((1, 2, 3), weights=(50, 35, 15))[0]
            for s in rnd.sample(range(1, stores + 1), k):
                cart_id += 1
                n = rnd.randint(1, 7)
                items = [(oid, rnd.randint(1, 5)) for oid in rnd.sample(self.store_offers[s], n)]
                self.carts.append((cart_id, user, s, items))

        # --- перегляди за добу: у кожному магазині унікальні лічильники за законом Ципфа
        #     (унікальність потрібна, щоб порядок у топ-N не залежав від правил розв'язання нічиїх)
        self.views = {}                         # store -> [(offer_id, views)] за спаданням views
        for s in range(1, stores + 1):
            ids = self.store_offers[s][:]
            rnd.shuffle(ids)
            self.views[s] = [(oid, int(1e9 / ((rank + 1) ** 1.05))) for rank, oid in enumerate(ids)]

        # --- розподіли для вибору параметрів тестів
        self.store_zipf = Zipf(stores, 1.0)         # "гарячі" магазини
        self.offer_zipf = Zipf(offers_per_store, 1.05)
        self.hot_order = {s: [oid for oid, _ in self.views[s]] for s in self.views}

    @property
    def n_items(self):
        return sum(len(c[3]) for c in self.carts)

    def describe(self):
        return (f"{len(self.offers)} пропозицій, {len(self.carts)} кошиків, "
                f"{self.n_items} позицій кошиків, {self.stores} магазинів")


# ============================================================== PostgreSQL
def load_pg(pg, ds: Dataset, batch=5000):
    pg.query("BEGIN")
    rows = []
    for o in ds.offers:
        rows.append(f"({o[0]},{o[1]},'{o[2]}',{money(o[3])})")
        if len(rows) >= batch:
            pg.query("INSERT INTO offers (id,store_id,name,price) VALUES " + ",".join(rows))
            rows = []
    if rows:
        pg.query("INSERT INTO offers (id,store_id,name,price) VALUES " + ",".join(rows))

    crows, irows = [], []

    def flush():
        nonlocal crows, irows
        if crows:
            pg.query("INSERT INTO carts (id,user_id,store_id) VALUES " + ",".join(crows))
        if irows:
            pg.query("INSERT INTO cart_items (cart_id,offer_id,quantity) VALUES " + ",".join(irows))
        crows, irows = [], []

    for cid, user, store, items in ds.carts:
        crows.append(f"({cid},{user},{store})")
        irows.extend(f"({cid},{oid},{q})" for oid, q in items)
        if len(crows) >= batch:
            flush()
    flush()

    vrows = []
    for s, lst in ds.views.items():
        for oid, v in lst:
            vrows.append(f"('{DAY}',{s},{oid},{v})")
            if len(vrows) >= batch:
                pg.query("INSERT INTO offer_views_daily (day,store_id,offer_id,views) VALUES " + ",".join(vrows))
                vrows = []
    if vrows:
        pg.query("INSERT INTO offer_views_daily (day,store_id,offer_id,views) VALUES " + ",".join(vrows))
    pg.query("SELECT setval(pg_get_serial_sequence('carts','id'), (SELECT max(id) FROM carts))")
    pg.query("COMMIT")


# ================================================================== Redis
def load_redis(r, ds: Dataset, batch=5000):
    r.cmd("FLUSHDB")      # лише вибрана логічна БД (за замовчуванням №5)
    cmds = []

    def flush():
        nonlocal cmds
        if cmds:
            r.pipeline(cmds)
        cmds = []

    for oid, store, name, cents in ds.offers:
        cmds.append(("HSET", f"offer:{oid}", "name", name, "price", money(cents), "store", store))
        if len(cmds) >= batch:
            flush()
    flush()

    for cid, user, store, items in ds.carts:
        key = f"cart:{user}:{store}"
        args = ["HSET", key]
        for oid, q in items:
            args += [oid, q]
        cmds.append(tuple(args))
        cmds.append(("EXPIRE", key, CART_TTL))
        if len(cmds) >= batch:
            flush()
    flush()

    for s, lst in ds.views.items():
        args = ["ZADD", f"views:{DAY}:{s}"]
        for oid, v in lst:
            args += [v, oid]
        cmds.append(tuple(args))
    flush()
