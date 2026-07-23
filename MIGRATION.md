# Redis → Redis + PostgreSQL 混合架構 遷移指南

## 改動總覽

| 資料 | 舊架構 | 新架構 | 說明 |
|---|---|---|---|
| `users` | Redis Hash | PostgreSQL `users` 表 | 帳號密碼/角色，需要唯一性約束 |
| `equipments` | Redis Hash | PostgreSQL `equipments` 表 | 設備主檔，新增 `equipment_history` 稽核紀錄（trigger 自動寫入） |
| `contacts` | Redis Hash + `contacts_id_counter` | PostgreSQL `contacts` 表 | ID 改用 PostgreSQL `SERIAL`，不用手動 `INCR` |
| `dispatched_log` / `failed_queue`（歷史查詢用途） | Redis List | PostgreSQL `task_history` 表 | `queue_history` / `dashboard_data` 改用 SQL 直接分桶聚合 |
| `task_queue` / `processing_queue` / `retry_queue` | Redis List | **不變** | 真正的工作佇列，維持 Redis |
| `worker_status` | Redis Hash | **不變**（依你的要求） | 即時心跳狀態 |

## 上線步驟

1. **啟動 PostgreSQL**
   ```bash
   docker network create redisnet   # 若尚未建立
   docker compose up -d postgres
   ```
   `schema.sql` 會在 PostgreSQL 容器第一次啟動、資料目錄是空的時候自動執行（透過 `docker-entrypoint-initdb.d`）。
   若是接到既有的 PostgreSQL（非首次啟動的容器），要手動跑一次：
   ```bash
   psql -h <host> -U queue_admin -d queue_system -f schema.sql
   ```

2. **搬遷既有資料**（users / equipments / contacts）
   ```bash
   python migrate_to_postgres.py
   ```
   這支腳本可重複執行（用 `ON CONFLICT` upsert），不會造成重複資料。
   equipments 搬遷時會暫時關閉 `equipment_history` 的 trigger，避免灌入大量「created」事件。

3. **啟動 history-sync 橋接服務**（在你把外部 RPA worker 程式改成直接寫 PostgreSQL 之前的過渡方案）
   ```bash
   docker compose up -d history-sync
   ```
   > `dispatched_log` / `failed_queue` 是由 repo 之外的 RPA/AMR worker 程式寫入的，
   > 這個 repo（`read_queue.py`）本身只負責讀，改不到寫入端。
   > `history-sync` 會持續把 Redis 裡新進的資料 `LPOP` 出來寫進 `task_history`，
   > 讓 Redis list 不會無限增長，同時 `queue_history` / `dashboard_data` 讀到的是最新資料。
   > 等 worker 端程式改成直接寫 `task_history`，就可以停用這個服務。

4. **部署新版 `read_queue.py`**
   ```bash
   docker compose up -d --build web
   ```

5. **驗證**
   - 登入功能（帳號密碼現在查 PostgreSQL）
   - 設備 / 聯絡人的新增、修改、刪除
   - `/equipment_history?eqpid=xxx` 能看到剛剛的異動紀錄
   - 趨勢圖（`/queue_history`、`/dashboard`）資料量隨 `history-sync` 同步而增加

6. **確認穩定後的收尾**
   - 舊的 `users` / `equipments` / `contacts` Redis Hash 可以保留一段時間當備份，確認沒問題後再清除
   - 待 worker 端改完直接寫 `task_history`，`history-sync` 可以下線，`dispatched_log` / `failed_queue` 這兩個 Redis key 也可以視需求繼續保留（當即時佇列）或移除（只做歷史用途的話就不需要了）

## 環境變數（新增）

| 變數 | 預設值 | 說明 |
|---|---|---|
| `POSTGRES_HOST` | `postgres` | PostgreSQL 主機 |
| `POSTGRES_PORT` | `5432` | PostgreSQL 埠 |
| `POSTGRES_DB` | `queue_system` | 資料庫名稱 |
| `POSTGRES_USER` | `queue_admin` | 帳號 |
| `POSTGRES_PASSWORD` | 需自行設定 | **正式環境務必更換，不要用預設值** |

## 版本需求

`queue_history` / `dashboard_data` 用到 `date_bin()` 函式，需要 **PostgreSQL 14 以上**（`docker-compose.yaml` 用的是 `postgres:16`，符合需求）。
