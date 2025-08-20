# python concurrency_bakeoff.py snowflake OR python concurrency_bakeoff.py databricks


import time, threading, statistics, argparse, os, sys
from concurrent.futures import ThreadPoolExecutor
# pip install matplotlib
import matplotlib.pyplot as plt  
# pip install python-dotenv
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv() 

# ---------- connection helpers ----------
def snowflake_connect():
    #pip install snowflake-connector-python
    import snowflake.connector
    return snowflake.connector.connect(
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASS"),
        account=os.getenv("SF_ACCOUNT"),
        warehouse="MASTERCLASSWH",
        database="MASTERCLASS",
        schema="PUBLIC",   
    )

def databricks_connect():
    #pip install databricks-sql-connector
    from databricks import sql
    return sql.connect(
        server_hostname=os.getenv("SERVER_HOSTNAME"),
        http_path=os.getenv("HTTP_PATH"),
        access_token=os.getenv("ACCESS_TOKEN")
    )

QUERY = """
    SELECT l_returnflag,
        l_linestatus,
        SUM(l_quantity)                          AS total_qty,
        SUM(l_extendedprice*(1-l_discount))      AS total_revenue,
        AVG(l_discount)                          AS avg_discount,
        COUNT(*)                                 AS row_cnt
    FROM   {table}
    WHERE  l_shipdate <= DATE '1998-09-02'
    GROUP  BY l_returnflag, l_linestatus
"""

TABLE = {"snowflake": "MASTERCLASS.PUBLIC.LINEITEMS_WAREHOUSE", "databricks": "MASTERCLASS.BRONZE.LINEITEMS_WAREHOUSE"}

# ---------- runner ----------
def run_once(conn, sql_text):
    cur = conn.cursor()
    t0 = time.perf_counter()
    cur.execute(sql_text)
    cur.fetchall()          # force completion
    return time.perf_counter() - t0

def exercise(kind, concurrency):
    connect = snowflake_connect if kind == "snowflake" else databricks_connect
    durations = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [
            pool.submit(lambda: run_once(connect(), QUERY.format(table=TABLE[kind])))
            for _ in range(concurrency)
        ]
        for f in futures:
            durations.append(f.result())
    return durations


# ---------- charting helper ----------
def safe_quantile(times, n, idx):
    if len(times) < 2:
        return float('nan')
    try:
        return statistics.quantiles(times, n=n)[idx]
    except Exception:
        return float('nan')

def plot_results(results):
    users = sorted(next(iter(results.values())).keys())
    fig, ax = plt.subplots(figsize=(10, 6))
    for kind, data in results.items():
        p50 = [statistics.median(data[u]) if data[u] else float('nan') for u in users]
        p95 = [safe_quantile(data[u], 20, 18) for u in users]
        maxv = [max(data[u]) if data[u] else float('nan') for u in users]
        ax.plot(users, p50, marker='o', label=f'{kind} p50')
        ax.plot(users, p95, marker='x', linestyle='--', label=f'{kind} p95')
        ax.plot(users, maxv, marker='s', linestyle=':', label=f'{kind} max')
    ax.set_xlabel('Concurrent Users')
    ax.set_ylabel('Query Time (s)')
    ax.set_title('Concurrency Benchmark: Snowflake vs Databricks')
    ax.legend()
    ax.grid(True)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--platforms", nargs="*", default=["snowflake", "databricks"],
                        help="Platforms to benchmark (default: both)")
    parser.add_argument("--users", nargs="*", type=int, default=[1, 8, 32, 64],
                        help="List of concurrent user counts")
    args = parser.parse_args()

    results = {kind: {} for kind in args.platforms}
    for kind in args.platforms:
        print(f"\n--- {kind.upper()} ---")
        for users in args.users:
            try:
                times = exercise(kind, users)
            except Exception as e:
                print(f"{users:>2} users  →  ERROR: {e}")
                times = []
            results[kind][users] = times
            if times:
                p50 = statistics.median(times)
                p95 = safe_quantile(times, 20, 18)
                maxv = max(times)
                print(f"{users:>2} users  →  p50 {p50:5.2f}s  "
                      f"p95 {p95:5.2f}s  "
                      f"max {maxv:5.2f}s  "
                      f"(n={len(times)})")
            else:
                print(f"{users:>2} users  →  No data")
    plot_results(results)
