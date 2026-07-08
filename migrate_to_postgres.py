"""
一次性搬遷腳本：把 Redis 裡的 users / equipments / contacts 三個 Hash
搬進 PostgreSQL 對應的表。

用法：
    python migrate_to_postgres.py

執行前提：
    1. schema.sql 已經在 PostgreSQL 執行過（表已建立）
    2. Redis Sentinel 與 PostgreSQL 皆可連線（環境變數與 read_queue.py 相同）
    3. 這支腳本可以重複執行，使用 ON CONFLICT DO NOTHING/UPDATE，不會造成重複資料
"""
import json
import redisConnect
import pgConnect


def migrate_users():
    redisConnect.connect_to_master()
    pgConnect.connect_to_pg()

    all_users = redisConnect.redis_master.hgetall('users')
    print(f"[users] 從 Redis 讀到 {len(all_users)} 筆")

    with pgConnect.get_conn() as conn:
        with conn.cursor() as cur:
            for username, value in all_users.items():
                if ':' not in value:
                    print(f"  ⚠️ 跳過格式異常的 user: {username}")
                    continue
                password_hash, role = value.rsplit(':', 1)
                cur.execute(
                    """
                    INSERT INTO users (username, password_hash, role)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (username) DO UPDATE
                        SET password_hash = EXCLUDED.password_hash,
                            role = EXCLUDED.role
                    """,
                    (username, password_hash, role)
                )
    print("[users] 搬遷完成")


def migrate_equipments():
    all_eqp = redisConnect.redis_master.hgetall('equipments')
    print(f"[equipments] 從 Redis 讀到 {len(all_eqp)} 筆")

    with pgConnect.get_conn() as conn:
        # 搬遷時不記錄稽核紀錄（避免灌入大量 "created" 事件），
        # 先關閉 session_replication_role 讓 trigger 不觸發
        with conn.cursor() as cur:
            cur.execute("SET session_replication_role = 'replica';")
            for eqpid, val_str in all_eqp.items():
                try:
                    data = json.loads(val_str)
                except json.JSONDecodeError:
                    print(f"  ⚠️ 跳過無法解析的 equipment: {eqpid}")
                    continue
                cur.execute(
                    """
                    INSERT INTO equipments (eqpid, eqptype, testerip, proberip, linegroup, floor, action)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (eqpid) DO UPDATE
                        SET eqptype = EXCLUDED.eqptype,
                            testerip = EXCLUDED.testerip,
                            proberip = EXCLUDED.proberip,
                            linegroup = EXCLUDED.linegroup,
                            floor = EXCLUDED.floor,
                            action = EXCLUDED.action,
                            updated_at = now()
                    """,
                    (
                        eqpid,
                        data.get('EQPTYPE', ''),
                        data.get('TESTERIP', ''),
                        data.get('PROBERIP', ''),
                        data.get('LINEGROUP', ''),
                        data.get('floor', ''),
                        data.get('Action', ''),
                    )
                )
            cur.execute("SET session_replication_role = 'origin';")
    print("[equipments] 搬遷完成")


def migrate_contacts():
    all_contacts = redisConnect.redis_master.hgetall('contacts')
    print(f"[contacts] 從 Redis 讀到 {len(all_contacts)} 筆")

    with pgConnect.get_conn() as conn:
        with conn.cursor() as cur:
            for _old_redis_id, val_str in all_contacts.items():
                try:
                    data = json.loads(val_str)
                except json.JSONDecodeError:
                    print(f"  ⚠️ 跳過無法解析的 contact: {_old_redis_id}")
                    continue
                # 注意：舊的 Redis 自增 ID 不搬過來，PostgreSQL 用自己的 SERIAL 重新編號
                cur.execute(
                    """
                    INSERT INTO contacts (eqpid, eqptype, action, linegroup, floor)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        data.get('EQPID', ''),
                        data.get('EQPTYPE', ''),
                        data.get('Action', ''),
                        data.get('LINEGROUP', ''),
                        data.get('floor', ''),
                    )
                )
    print("[contacts] 搬遷完成")


if __name__ == '__main__':
    migrate_users()
    migrate_equipments()
    migrate_contacts()
    print("\n✅ 全部搬遷完成，建議搬遷後手動抽查幾筆資料再切換 read_queue.py 的讀寫來源。")
