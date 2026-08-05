"""
history 同步橋接服務

背景說明：
    dispatched_log / failed_queue 這兩個 Redis List 是由「外部的 RPA/AMR
    worker 程式」寫入的（不在這個 repo 裡），read_queue.py 這支 Flask 應用
    只負責「讀」。因此無法直接在 read_queue.py 裡把寫入邏輯改成 SQL——
    真正動手的地方是 worker 端的程式碼。

    在還沒有機會去改 worker 端程式碼之前，這支腳本先當「橋接器」：
    持續把 Redis 裡的 dispatched_log / failed_queue 搬進 PostgreSQL 的
    task_history 表，然後把 Redis 裡的資料清掉，讓 Redis list 不會無限
    增長，同時前台的 queue_history / dashboard_data 已經改讀 task_history，
    資料會持續更新。

    安全機制（避免資料遺失）：
    不直接對來源 list 做 LPOP（那個動作立即生效、無法回滾）。
    而是先用 LMOVE 把資料原子性地搬到一個「處理中」暫存 list
    （<redis_key>:processing），確認整批資料已經成功 INSERT 並 commit
    進 PostgreSQL 之後，才用 LTRIM 把暫存 list 裡對應的資料清掉。

    如果 PostgreSQL 寫入中途失敗（斷線、約束衝突等），這批資料仍然完整
    留在 :processing 這個暫存 list 裡，不會憑空消失；下次啟動或下一輪
    輪詢會優先處理 :processing 裡的殘留資料（等同自動復原）。

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
BATCH_SIZE = 500       # 每次最多搬移/處理幾筆，避免單次交易時間過長
POLL_INTERVAL_SEC = 5  # 沒有資料時多久檢查一次


def _resolve_key(category, suffix):
    """跟 read_queue.py 原本的邏輯一致：可能有兩種 key 命名方式，找第一個存在的。
    注意：這裡只檢查來源 queue 是否存在；:processing 暫存 list 就算來源已經
    消失也仍要檢查殘留資料，所以呼叫端會另外處理。"""
    candidates = [f"queue_history:{category}_{suffix}", f"{category}_{suffix}"]
    for k in candidates:
        if redisConnect.redis_master.exists(k):
            return k
    return None


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


def _insert_batch(pending_raw, category, status):
    """把一批已經安全搬進 :processing 暫存 list 的原始資料，INSERT 進
    task_history 並 commit。任何一筆失敗都會讓整批 rollback，並把例外
    往上丟——呼叫端看到例外就不會執行 LTRIM，資料會繼續留在暫存 list。"""
    synced = 0
    with pgConnect.get_conn() as conn:
        with conn.cursor() as cur:
            for raw in pending_raw:
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
    # 離開 with 區塊時 get_conn() 已經 commit 成功，才會執行到這裡
    return synced


def _drain_one_list(redis_key, processing_key, category, status):
    """
    每一輪：
      1. 用 LMOVE 把最多 BATCH_SIZE 筆資料，從來源 list 原子性地搬到
         :processing 暫存 list（如果 :processing 裡已經有上一輪失敗留下
         的殘留資料，會自然排在前面，一起被處理——等於自動復原）。
      2. 讀出 :processing 目前的「全部」內容，INSERT 進 PostgreSQL 並 commit。
      3. 只有 commit 成功，才用 LTRIM 把剛剛處理過的那些資料從 :processing 清掉。
      4. 任何一步失敗，資料仍完整留在 :processing，下一輪會重新嘗試。
    """
    total_synced = 0
    while True:
        pipe = redisConnect.redis_master.pipeline()
        for _ in range(BATCH_SIZE):
            pipe.lmove(redis_key, processing_key, 'LEFT', 'RIGHT')
        move_results = pipe.execute()
        moved = sum(1 for r in move_results if r is not None)

        pending_raw = redisConnect.redis_master.lrange(processing_key, 0, -1)
        if not pending_raw:
            break  # 來源跟暫存區都空了，這個 key 這輪處理完畢

        pending_count = len(pending_raw)

        # 這一行如果拋例外（PostgreSQL 斷線、約束衝突等），資料仍完整留在
        # :processing，不會執行下面的 LTRIM，等於安全地留到下一輪重試。
        synced = _insert_batch(pending_raw, category, status)
        total_synced += synced

        # 只有成功 commit 之後，才把已經處理過的這批資料從暫存區清掉
        redisConnect.redis_master.ltrim(processing_key, pending_count, -1)

        if moved < BATCH_SIZE:
            break  # 來源 list 這輪已經搬空，沒有更多資料了

    return total_synced


def sync_once():
    total_synced = 0
    for category in CATEGORIES:
        for suffix, status in SUFFIX_TO_STATUS.items():
            redis_key = _resolve_key(category, suffix)
            # 就算來源 key 目前不存在，也要用「常見的」命名方式檢查一次
            # :processing 暫存 list 有沒有殘留資料（例如來源那陣子沒有新資料，
            # 但上次同步中途失敗留下未清完的暫存資料）。
            candidate_source = redis_key or f"{category}_{suffix}"
            processing_key = f"{candidate_source}:processing"

            if not redis_key and not redisConnect.redis_master.exists(processing_key):
                continue

            n = _drain_one_list(candidate_source, processing_key, category, status)
            if n:
                print(f"[sync] {candidate_source} -> task_history({category}, {status})：{n} 筆")
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