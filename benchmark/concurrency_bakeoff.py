# python concurrency_bakeoff.py snowflake OR python concurrency_bakeoff.py databricks

import time, threading, statistics, argparse, os, sys
from concurrent.futures import ThreadPoolExecutor
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

# ---------- main ----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=["snowflake", "databricks"])
    args = parser.parse_args()

    for users in (1, 8, 32, 64):
        times = exercise(args.kind, users)
        print(f"{users:>2} users  →  p50 {statistics.median(times):5.2f}s  "
              f"p95 {statistics.quantiles(times, n=20)[18]:5.2f}s  "
              f"max {max(times):5.2f}s  "
              f"(n={len(times)})")
