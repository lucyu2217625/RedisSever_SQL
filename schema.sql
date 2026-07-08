-- ============================================================
-- PostgreSQL Schema
-- 取代 Redis Hash 主檔資料（users / equipments / contacts）
-- 新增 task_history（取代 dispatched_log / failed_queue 的歷史統計用途）
-- 新增 equipment_history（equipments 的異動稽核紀錄，用 trigger 自動寫入）
-- worker_status 維持在 Redis，不受影響
-- ============================================================

-- ---------- users ----------
CREATE TABLE users (
    id            SERIAL PRIMARY KEY,
    username      VARCHAR(64) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role          VARCHAR(16) NOT NULL DEFAULT 'viewer',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------- equipments ----------
CREATE TABLE equipments (
    eqpid      VARCHAR(64) PRIMARY KEY,
    eqptype    VARCHAR(64) NOT NULL,
    testerip   VARCHAR(64) NOT NULL,
    proberip   VARCHAR(64) NOT NULL,
    linegroup  VARCHAR(64) NOT NULL,
    floor      VARCHAR(32) NOT NULL,
    action     VARCHAR(64) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------- contacts ----------
CREATE TABLE contacts (
    id         SERIAL PRIMARY KEY,
    eqpid      VARCHAR(64) NOT NULL,
    eqptype    VARCHAR(64) NOT NULL,
    action     VARCHAR(64) NOT NULL,
    linegroup  VARCHAR(64) NOT NULL,
    floor      VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_contacts_eqpid ON contacts (eqpid);

-- ---------- task_history（取代 dispatched_log / failed_queue 的歷史統計）----------
CREATE TABLE task_history (
    id            BIGSERIAL PRIMARY KEY,
    category      VARCHAR(32) NOT NULL,   -- LotActions / prober / LineNotify
    task_id       VARCHAR(64),
    status        VARCHAR(16) NOT NULL,   -- dispatched / failed
    rpa_worker    VARCHAR(64),
    dispatch_time TIMESTAMPTZ NOT NULL,
    payload       JSONB,                  -- 保留原始完整內容，方便日後擴充欄位
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_task_history_category_time ON task_history (category, dispatch_time);
CREATE INDEX idx_task_history_status_time   ON task_history (status, dispatch_time);
CREATE INDEX idx_task_history_worker        ON task_history (rpa_worker);

-- ============================================================
-- equipment_history：稽核紀錄表
-- 用 trigger 自動記錄 equipments 的 INSERT / UPDATE / DELETE，
-- 不需要在每個 route 手動呼叫記錄，app 端只需在異動前
-- 呼叫 SELECT set_config('app.current_user', :username, true)
-- 讓 trigger 能記下是誰改的（沒設定的話會是 NULL）。
-- ============================================================
CREATE TABLE equipment_history (
    id          BIGSERIAL PRIMARY KEY,
    eqpid       VARCHAR(64) NOT NULL,
    change_type VARCHAR(16) NOT NULL CHECK (change_type IN ('created', 'updated', 'deleted')),
    changed_by  VARCHAR(64),
    old_value   JSONB,
    new_value   JSONB,
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_equipment_history_eqpid_time ON equipment_history (eqpid, changed_at);

CREATE OR REPLACE FUNCTION fn_equipment_history() RETURNS TRIGGER AS $$
DECLARE
    v_user VARCHAR(64);
BEGIN
    -- current_setting 第二參數 true = 找不到時回傳 NULL 而不是丟例外
    v_user := current_setting('app.current_user', true);

    IF TG_OP = 'INSERT' THEN
        INSERT INTO equipment_history (eqpid, change_type, changed_by, old_value, new_value)
        VALUES (NEW.eqpid, 'created', v_user, NULL, to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO equipment_history (eqpid, change_type, changed_by, old_value, new_value)
        VALUES (NEW.eqpid, 'updated', v_user, to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO equipment_history (eqpid, change_type, changed_by, old_value, new_value)
        VALUES (OLD.eqpid, 'deleted', v_user, to_jsonb(OLD), NULL);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_equipment_history
AFTER INSERT OR UPDATE OR DELETE ON equipments
FOR EACH ROW EXECUTE FUNCTION fn_equipment_history();
