"""
PostgreSQL 連線管理模組。
風格比照 redisConnect.py：模組層級的全域連線池 + 重試邏輯，
提供給 read_queue.py 用 with get_conn() as conn: 的方式取得連線。
"""
import os
import time
import psycopg2
import psycopg2.extras
from psycopg2 import pool as pg_pool
from contextlib import contextmanager

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "queue_system")
PG_USER = os.getenv("POSTGRES_USER", "queue_admin")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "please-change-me")

_pool = None


def connect_to_pg(retry=10, delay=2, minconn=1, maxconn=10):
    """建立全域連線池，啟動時呼叫一次（比照 redisConnect.connect_to_master）"""
    global _pool
    for i in range(retry):
        try:
            _pool = pg_pool.ThreadedConnectionPool(
                minconn, maxconn,
                host=PG_HOST, port=PG_PORT,
                dbname=PG_DB, user=PG_USER, password=PG_PASS,
                connect_timeout=5,
            )
            # 立即測試一次連線是否真的活著
            conn = _pool.getconn()
            conn.close = conn.close  # no-op，避免誤用
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
            _pool.putconn(conn)
            print(f"✅ pg-PostgreSQL 連線池建立成功（第 {i + 1} 次嘗試）")
            return
        except Exception as e:
            print(f"⏳ pg-PostgreSQL 尚未就緒，第 {i + 1}/{retry} 次重試中… {repr(e)}")
            time.sleep(delay)
    raise RuntimeError(f"❌ pg-在 {retry} 次重試後仍無法連線 PostgreSQL，請檢查服務狀態。")


@contextmanager
def get_conn():
    """
    用法：
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(...)
    發生例外會自動 rollback，並在最後把連線還給連線池。
    """
    if _pool is None:
        raise RuntimeError("PostgreSQL 連線池尚未初始化，請先呼叫 connect_to_pg()")
    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


def set_current_user(conn, username):
    """
    設定這個連線（transaction）的 app.current_user，
    讓 equipment_history 的 trigger 能記錄是誰做的異動。
    必須在同一個 conn/transaction 內先呼叫，才會生效。
    """
    with conn.cursor() as cur:
        cur.execute("SELECT set_config('app.current_user', %s, true);", (username or '',))


def dict_cursor(conn):
    """回傳一個會把查詢結果轉成 dict 的 cursor，方便 Jinja 模板直接取欄位"""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
