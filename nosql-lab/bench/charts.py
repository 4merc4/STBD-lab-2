"""Графіки за results/results.json (потрібен matplotlib)."""
import json, os, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
src = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "..", "results", "results.json")
out = sys.argv[2] if len(sys.argv) > 2 else os.path.join(HERE, "..", "docs", "img")
os.makedirs(out, exist_ok=True)
R = json.load(open(src))["results"]
CPG, CRD = "#2f5d8a", "#d1603d"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "axes.spines.top": False, "axes.spines.right": False})

def get(wl, sysn, cfg, mode, w=1, var=""):
    for r in R:
        if (r["workload"], r["system"], r["config"], r["mode"], r["workers"], r["variant"]) == (wl, sysn, cfg, mode, w, var):
            return r["summary"]

def bars(ax, labels, pg, rd, ylabel, fmt="{:.0f}", legend=True):
    x = range(len(labels)); w = 0.38
    b1 = ax.bar([i - w / 2 for i in x], pg, w, color=CPG, label="PostgreSQL")
    b2 = ax.bar([i + w / 2 for i in x], rd, w, color=CRD, label="Redis")
    for b in list(b1) + list(b2):
        ax.annotate(fmt.format(b.get_height()), (b.get_x() + b.get_width() / 2, b.get_height()),
                    ha="center", va="bottom", fontsize=8)
    ax.set_xticks(list(x)); ax.set_xticklabels(labels); ax.set_ylabel(ylabel)
    if legend: ax.legend(frameon=False, loc="upper right")

# 1. затримка p50, одне з'єднання, режим balanced (читання - fast)
ops = [("get_cart", "fast", "читання\nкошика"), ("get_cart_details", "fast", "кошик\nз цінами"), ("top10", "fast", "топ-10\nпереглядів"),
       ("add_item", "balanced", "додати\nтовар"), ("view_inc", "balanced", "+1\nперегляд"), ("clear_cart", "balanced", "очистити\nкошик")]
fig, ax = plt.subplots(figsize=(8.5, 4))
bars(ax, [o[2] for o in ops], [get(o[0], "pg", o[1], "sequential")["p50_us"] for o in ops],
     [get(o[0], "redis", o[1], "sequential")["p50_us"] for o in ops], "медіанна затримка p50, мкс (менше — краще)")
fig.tight_layout(); fig.savefig(f"{out}/lat.png", dpi=200); plt.close(fig)

# 2. пропускна здатність, 4 клієнти, balanced
ops = [("add_item", "додати товар"), ("view_inc", "+1 перегляд"), ("mixed", "змішане\n(60/25/15)")]
fig, axs = plt.subplots(1, 2, figsize=(9, 4), gridspec_kw={"width_ratios": [3, 1.3]})
bars(axs[0], [o[1] for o in ops], [get(o[0], "pg", "balanced", "concurrent", 4)["ops_s"] for o in ops],
     [get(o[0], "redis", "balanced", "concurrent", 4)["ops_s"] for o in ops], "операцій за секунду (більше — краще)")
axs[0].set_title("Запис і змішане навантаження, 4 клієнти", fontsize=10)
bars(axs[1], ["кошик з цінами"], [get("get_cart_details", "pg", "fast", "concurrent", 4)["ops_s"]],
     [get("get_cart_details", "redis", "fast", "concurrent", 4)["ops_s"]], "", legend=False)
axs[1].set_title("Читання, 4 клієнти", fontsize=10)
fig.tight_layout(); fig.savefig(f"{out}/thr.png", dpi=200); plt.close(fig)

# 3. вплив режиму надійності на запис (add_item, 4 клієнти)
cfgs = [("fast", "без fsync\n(PG off / Redis RDB-off)"), ("balanced", "раз на ~1 с\n(PG off / AOF everysec)"), ("durable", "fsync на кожну\n(PG on / AOF always)")]
fig, ax = plt.subplots(figsize=(8.5, 4))
bars(ax, [c[1] for c in cfgs], [get("add_item", "pg", c[0], "concurrent", 4)["ops_s"] for c in cfgs],
     [get("add_item", "redis", c[0], "concurrent", 4)["ops_s"] for c in cfgs], "операцій за секунду, 4 клієнти")
fig.tight_layout(); fig.savefig(f"{out}/dur.png", dpi=200); plt.close(fig)

# 4. аналітика та індекс (лог. шкала)
a_pg = get("analytic_cart_value", "pg", "fast", "sequential")["p50_us"] / 1000
a_rd = get("analytic_cart_value", "redis", "fast", "sequential")["p50_us"] / 1000
t_idx = get("top10", "pg", "fast", "sequential")["p50_us"] / 1000
t_no = get("top10", "pg", "balanced", "sequential", 1, "no_views_idx")["p50_us"] / 1000
t_rd = get("top10", "redis", "fast", "sequential")["p50_us"] / 1000
fig, axs = plt.subplots(1, 2, figsize=(9, 3.8))
axs[0].bar(["PostgreSQL", "Redis"], [a_pg, a_rd], color=[CPG, CRD])
for i, v in enumerate([a_pg, a_rd]): axs[0].annotate(f"{v:.0f} мс", (i, v), ha="center", va="bottom")
axs[0].set_ylabel("час одного запиту, мс"); axs[0].set_title("Аналітика: вартість кошиків\nпо магазинах (SUM + GROUP BY)", fontsize=10)
axs[1].bar(["PG з індексом", "PG без індексу", "Redis ZSET"], [t_idx, t_no, t_rd], color=[CPG, "#8aa7c4", CRD])
for i, v in enumerate([t_idx, t_no, t_rd]): axs[1].annotate(f"{v:.2f} мс", (i, v), ha="center", va="bottom")
axs[1].set_ylabel("p50, мс"); axs[1].set_title("Топ-10 за переглядами:\nвплив індексу", fontsize=10)
fig.tight_layout(); fig.savefig(f"{out}/analytic.png", dpi=200); plt.close(fig)
print("charts ok")
