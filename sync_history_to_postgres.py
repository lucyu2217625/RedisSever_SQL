"""
history 同步橋接服務

背景說明：
    dispatched_log / failed_queue 這兩個 Redis List 是由「外部的 RPA/AMR
    worker 程式」寫入的（不在這個 repo 裡），read_queue.py 這支 Flask 應用
    只負責「讀」。因此無法直接在 read_queue.py 裡把寫入邏輯改成 SQL——
    真正動手的地方是 worker 端的程式碼。

    在還沒有機會去改 worker 端程式碼之前，這支腳本先當「橋接器」：
    持續把 Redis 裡的 dispatched_log / failed_queue 用 LPOP 撈出來、寫進
    PostgreSQL 的 task_history 表，然後把 Redis 裡的資料清掉，讓 Redis list
    不會無限增長，同時前台的 queue_history / dashboard_data 已經改讀
    task_history，資料會持續更新。

    等未來 worker 端程式改成直接寫 task_history（用同樣的 schema），
    就可以關掉這支腳本，dispatched_log / failed_queue 也可以考慮完全
    移除、只當作 Redis 端的暫存佇列。

用法：
    python sync_history_to_postgres.py
    建議用一個獨立的容器/服務長時間跑（見 docker-compose.yaml 的 history-sync）
"""
import json
import time
import redisConnect
import pgConnect

CATEGORIES = ["LotActions", "prober", "LineNotify"]
SUFFIX_TO_STATUS = {
    "dispatched_log": "dispatched",
    "failed_queue": "failed",
}
BATCH_SIZE = 500       # 每次最多處理幾筆，避免單次交易時間過長
POLL_INTERVAL_SEC = 5  # 沒有資料時多久檢查一次


def _resolve_key(category, suffix):
    """跟 read_queue.py 原本的邏輯一致：可能有兩種 key 命名方式，找第一個存在的"""
    candidates = [f"queue_history:{category}_{suffix}", f"{category}_{suffix}"]
    for k in candidates:
        if redisConnect.redis_master.exists(k):
            return k
    return None


def _drain_one_list(redis_key, category, status):
    """把一個 Redis list 裡目前的資料全部 LPOP 出來寫進 task_history"""
    synced = 0
    with pgConnect.get_conn() as conn:
        with conn.cursor() as cur:
            while True:
                # 用 pipeline 一次批次 LPOP，減少來回次數
                pipe = redisConnect.redis_master.pipeline()
                for _ in range(BATCH_SIZE):
                    pipe.lpop(redis_key)
                raw_items = pipe.execute()
                raw_items = [r for r in raw_items if r is not None]
                if not raw_items:
                    break

                for raw in raw_items:
                    try:
                        item = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(item, dict) and item.get('task_id') == '__INIT__':
                        continue

                    ts_val = item.get('dispatch_time') or item.get('timestamp')
                    dispatch_time = _parse_ts(ts_val)
                    if dispatch_time is None:
                        continue

                    rpa_worker = item.get('RPA_worker_name') or item.get('assigned_bot')

                    cur.execute(
                        """
                        INSERT INTO task_history (category, task_id, status, rpa_worker, dispatch_time, payload)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            category,
                            item.get('task_id'),
                            status,
                            rpa_worker,
                            dispatch_time,
                            json.dumps(item, ensure_ascii=False),
                        )
                    )
                    synced += 1

                if len(raw_items) < BATCH_SIZE:
                    break
    return synced


def _parse_ts(ts_val):
    from datetime import datetime, timezone, timedelta
    local_tz = timezone(timedelta(hours=8))
    if ts_val is None:
        return None
    if isinstance(ts_val, (int, float)):
        return datetime.fromtimestamp(ts_val, tz=local_tz)
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(str(ts_val), fmt).replace(tzinfo=local_tz)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(str(ts_val).replace('Z', '+00:00'))
    except ValueError:
        return None


def sync_once():
    total_synced = 0
    for category in CATEGORIES:
        for suffix, status in SUFFIX_TO_STATUS.items():
            redis_key = _resolve_key(category, suffix)
            if not redis_key:
                continue
            n = _drain_one_list(redis_key, category, status)
            if n:
                print(f"[sync] {redis_key} -> task_history({category}, {status})：{n} 筆")
            total_synced += n
    return total_synced


if __name__ == '__main__':
    redisConnect.connect_to_master()
    pgConnect.connect_to_pg()
    print("history 同步橋接服務啟動，開始持續輪詢…")
    while True:
        try:
            n = sync_once()
            if n == 0:
                time.sleep(POLL_INTERVAL_SEC)
        except Exception as e:
            print(f"⚠️ 同步發生錯誤，{POLL_INTERVAL_SEC} 秒後重試：{e}")
            time.sleep(POLL_INTERVAL_SEC)
