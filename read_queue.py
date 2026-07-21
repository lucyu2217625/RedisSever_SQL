from flask import Flask, render_template, request, Response, redirect, url_for, flash, session,jsonify,render_template_string, send_from_directory
import redis
import json
import os
import redisConnect
import pgConnect
import psycopg2.extras
import threading
from redis.exceptions import RedisError, ConnectionError
from functools import wraps
from werkzeug.security import check_password_hash,generate_password_hash
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import requests

def safe_parse_datetime(dt_str):
    """
    自定義日期解析，取代 dateutil.parser.parse 以減少外部依賴。
    """
    if not dt_str:
        return None
    dt_str = str(dt_str).strip()
    
    # 處理 ISO 格式 (例如: 2023-10-27T10:30:00)
    try:
        # datetime.fromisoformat 在 3.7+ 支援
        # 替換 Z 為 +00:00 以相容舊版 fromisoformat
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        pass
    
    # 嘗試其他常見格式
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    return None


redisConnect.connect_to_master()
threading.Thread(target=redisConnect.listen_for_failover, daemon=True).start()

pgConnect.connect_to_pg()

def init_admin_user():
    try:
        with pgConnect.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM users WHERE username = %s", ('admin',))
                if cur.fetchone() is None:
                    hashed = generate_password_hash('admin123')
                    cur.execute(
                        "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s)",
                        ('admin', hashed, 'admin')
                    )
                    print("Default admin user created.")
    except Exception as e:
        print(f"Error initializing admin user: {e}")

init_admin_user()

app = Flask(__name__, static_folder="static")
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'your-secret-key')  # 請設置安全的密鑰
app.config['SESSION_PERMANENT'] = False  # 設置 session 為非永久，瀏覽器關閉後失效

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(app.static_folder, 'favicon.ico', mimetype='image/vnd.microsoft.icon')

# 預定義可查詢的佇列名稱
ALLOWED_QUEUES = os.getenv('ALLOWED_QUEUES', 'processing_queue,failed_queue,task_queue,retry_queue,worker_status,dispatched_log').split(',')

# 自定義 JSON 過濾器
@app.template_filter('from_json')
def from_json_filter(s):
    if isinstance(s, str):
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return {}
    return s

# 注入 max 和 min 函數供模板使用
@app.context_processor
def inject_functions():
    return dict(max=max, min=min)

# 檢查是否登入的裝飾器
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            # flash('請先登入', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# 檢查角色權限的裝飾器
def role_required(role):
    @wraps(role)
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'username' not in session:
                # flash('請先登入', 'error')
                return redirect(url_for('login'))
            try:
                with pgConnect.get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT role FROM users WHERE username = %s", (session['username'],))
                        row = cur.fetchone()
                if not row:
                    flash('使用者資料不存在', 'error')
                    return redirect(url_for('login'))
                user_role = row[0]
                if user_role != role:
                    flash('您沒有權限執行此操作', 'error')
                    return redirect(url_for('index'))
                return f(*args, **kwargs)
            except Exception as e:
                flash('無法連接到資料庫，請稍後再試', 'error')
                return redirect(url_for('login'))
        return decorated_function
    return decorator

# 登入頁面
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()

        try:
            with pgConnect.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT password_hash, role FROM users WHERE username = %s",
                        (username,)
                    )
                    row = cur.fetchone()
            if row:
                stored_hash, stored_role = row
                if check_password_hash(stored_hash, password):
                    session['username'] = username
                    session['role'] = stored_role
                    # flash('登入成功！', 'success')
                    return redirect(url_for('index'))
                else:
                    return render_template('login.html', error=f'使用者名稱或密碼錯誤')
            else:
                return render_template('login.html', error='使用者名稱或密碼錯誤')
        except Exception:
            return render_template('login.html', error='無法連接到資料庫，請稍後再試')

    return render_template('login.html')

# 登出
@app.route('/logout')
def logout():
    session.pop('username', None)
    session.pop('role', None)
    # flash('您已登出', 'success')
    return redirect(url_for('login'))

# 主頁（佇列查詢）
@app.route('/', methods=['GET', 'POST'])
@login_required
def index():
    queue_name = ''
    queue_data = []
    queue_length = 0
    error = None
    page = int(request.args.get('page', 1))
    
    # Per page logic
    try:
        per_page_source = request.form if request.method == 'POST' else request.args
        per_page = int(per_page_source.get('per_page', 10))
    except (ValueError, TypeError):
        per_page = 10
    if per_page not in [10, 25, 50, 100]: # Validate allowed values
        per_page = 10
    is_hash = False
    hash_data = {}
    is_history_queue = False
    user_role = session.get('role', 'viewer')
    total_pages = 1 # Initialize total_pages here

    if request.method == 'POST':
        queue_name = request.form.get('queue_name', '').strip()
    else:
        queue_name = request.args.get('queue_name', '').strip()

    available_queues = ALLOWED_QUEUES

    history_view = load_history_queue(queue_name, page, per_page) if queue_name else None
    if history_view:
        queue_data = history_view['queue_data']
        queue_length = history_view['queue_length']
        total_pages = history_view['total_pages'] or 1
        is_history_queue = True
    elif queue_name in ALLOWED_QUEUES:
        try:
            if redisConnect.redis_master.exists(queue_name):
                key_type = redisConnect.type(queue_name)
                if key_type == 'list':
                    total_queue_length = redisConnect.redis_master.llen(queue_name)
                    if total_queue_length > 0:
                        start = (page - 1) * per_page
                        end = start + per_page * 5

                        raw_data = redisConnect.redis_master.lrange(queue_name, 0, -1)

                        filtered_data = []
                        for raw in raw_data:
                            # 確保 raw 是字串格式
                            raw_str = raw.decode() if isinstance(raw, bytes) else raw
                            parsed = parse_json(raw_str)
                            if isinstance(parsed, dict) and parsed.get('task_id') == '__INIT__':
                                continue
                            filtered_data.append({'parsed': parsed, 'raw': raw_str})

                        queue_length = len(filtered_data)
                        total_pages = (queue_length + per_page - 1) // per_page

                        start = (page - 1) * per_page
                        end = start + per_page
                        queue_data = filtered_data[start:end]

                        queue_length = total_queue_length
                        if not queue_data and start == 0:
                            error = f"「{queue_name}」沒有內容。"
                    else:
                        error = f"「{queue_name}」沒有內容。"
                        
                elif key_type == 'hash':
                    is_hash = True
                    raw_hash_data = redisConnect.redis_master.hgetall(queue_name)
                    print(f"[DEBUG] hgetall({queue_name}) -> {raw_hash_data}")   # 🔥 log 1
                    
                    hash_data = {} 
                    if raw_hash_data:
                        for key, val in raw_hash_data.items():
                            key_str = key.decode() if isinstance(key, bytes) else key
                            val_str = val.decode() if isinstance(val, bytes) else val
                            print(f"[DEBUG] key={key_str}, val={val_str}")       # 🔥 log 2
                            try:
                                json_val = json.loads(val_str)
                                hash_data[key_str] = json_val.get("status", "未知")
                            except (json.JSONDecodeError, AttributeError) as e:
                                print(f"[ERROR] JSON parse error: {e} for val={val_str}")
                                hash_data[key_str] = "無法解析"
                    else:
                        error = f"「{queue_name}」沒有內容。"
                        print(f"[DEBUG] {error}")
                else:
                    error = f"「{queue_name}」的資料型態不受支持。"
                    print(f"[DEBUG] {error}")


            else:
                error = f"「{queue_name}」沒有內容。"
        except (RedisError, ConnectionError) as e:
            error = "無法連接到資料庫，請稍後再試。"
            redisConnect.connect_to_master()
    elif queue_name:
        error = "請選擇一個有效的佇列名稱。"



    return render_template(
        'queue_viewer.html',
        queue_name=queue_name,
        queue_data=queue_data,
        queue_length=queue_length,
        error=error,
        available_queues=available_queues,
        page=page,
        total_pages=total_pages,
        per_page=per_page,
        is_hash=is_hash,
        hash_data=hash_data,
        is_history_queue=is_history_queue,
        user_role=user_role,
        active_page='index'
    )

def parse_json(item):
    try:
        return json.loads(item)
    except json.JSONDecodeError:
        return item


def history_queue_spec(queue_name):
    for suffix, status in (
        ('_dispatched_log', 'dispatched'),
        ('_failed_queue', 'failed'),
    ):
        if queue_name.endswith(suffix):
            return queue_name[:-len(suffix)], status
    return None, None


def normalize_history_row(row):
    payload = row.get('payload') or {}
    if not isinstance(payload, dict):
        payload = {'payload': payload}

    dispatch_time = row.get('dispatch_time')
    if isinstance(dispatch_time, datetime):
        dispatch_time = dispatch_time.astimezone(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')

    display = dict(payload)
    display.update({
        'category': row.get('category'),
        'task_id': row.get('task_id'),
        'status': row.get('status'),
        'rpa_worker': row.get('rpa_worker'),
        'dispatch_time': dispatch_time,
    })

    return {
        'parsed': display,
        'raw': json.dumps(display, ensure_ascii=False),
    }


def load_history_queue(queue_name, page, per_page):
    category, status = history_queue_spec(queue_name)
    if not category:
        return None

    offset = (page - 1) * per_page
    with pgConnect.get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT count(*) AS cnt
                FROM task_history
                WHERE category = %s AND status = %s
                """,
                (category, status)
            )
            total = cur.fetchone()['cnt']

            cur.execute(
                """
                SELECT category, task_id, status, rpa_worker, dispatch_time, payload
                FROM task_history
                WHERE category = %s AND status = %s
                ORDER BY dispatch_time DESC, id DESC
                LIMIT %s OFFSET %s
                """,
                (category, status, per_page, offset)
            )
            rows = cur.fetchall()

    queue_data = [normalize_history_row(row) for row in rows]
    total_pages = (total + per_page - 1) // per_page if total else 0
    return {
        'queue_data': queue_data,
        'queue_length': total,
        'total_pages': total_pages,
        'is_history_queue': True,
    }

@app.route('/download')
@login_required
def download():
    queue_name = request.args.get('queue_name', '').strip()

    if not queue_name:
        return "錯誤：請選擇有效的佇列名稱", 400

    try:
        data = []
        history_view = load_history_queue(queue_name, 1, 1000000) if queue_name else None
        if history_view:
            data = [item['parsed'] for item in history_view['queue_data']]
        elif redisConnect.redis_master.exists(queue_name):
            key_type = redisConnect.type(queue_name)
            if key_type == 'list':
                # Since decode_responses=True, lrange returns list of strings
                raw_data = redisConnect.redis_master.lrange(queue_name, 0, -1)
                for item in raw_data:
                    parsed = parse_json(item)
                    if parsed is not None and not (isinstance(parsed, dict) and parsed.get('task_id') == '__INIT__'):
                        data.append(parsed)
            elif key_type == 'hash':
                # Since decode_responses=True, hgetall returns dict of str:str
                hash_data = redisConnect.redis_master.hgetall(queue_name)
                data = {}
                for key, val in hash_data.items():
                    # The key is already a string.
                    # The value is a string, which might be JSON.
                    data[key] = parse_json(val)
            else:
                return "不支援的資料類型", 400
        else:
            return "佇列不存在或為空", 404

        json_str = json.dumps(data, ensure_ascii=False, indent=2)
        return Response(
            json_str,
            mimetype='application/json',
            headers={"Content-Disposition": f"attachment;filename={queue_name}.json"}
        )
    except Exception as e:
        return f"下載失敗：{str(e)}", 500


@app.route('/delete_selected', methods=['POST'])
@login_required
@role_required('admin')
def delete_selected():
    queue_name = request.form.get('queue_name', '').strip()
    delete_all_force = request.form.get('delete_all_force') == '1'
    selected_items = request.form.getlist('selected_items[]')

    if not queue_name:
        flash("缺少佇列名稱", "error")
        return redirect(url_for('index'))

    if history_queue_spec(queue_name)[0]:
        flash("歷史紀錄頁面僅供查詢與下載，不能刪除。", "error")
        return redirect(url_for('index', queue_name=queue_name))

    try:
        if delete_all_force:
            redisConnect.redis_master.delete(queue_name)
            flash(f'已清空整個佇列「{queue_name}」', 'success')
            return redirect(url_for('index', queue_name=queue_name))

        if not selected_items:
            flash("請選擇要刪除的項目", "error")
            return redirect(url_for('index', queue_name=queue_name))

        key_type = redisConnect.redis_master.type(queue_name)
        if key_type == 'list':
            count = 0
            # 取得 Redis 裡所有的原始資料
            all_raw = redisConnect.redis_master.lrange(queue_name, 0, -1)
            
            for item_val in selected_items:
                # 1. 嘗試直接刪除（完全匹配）
                if redisConnect.redis_master.lrem(queue_name, 1, item_val):
                    count += 1
                else:
                    # 2. 如果失敗，嘗試解析後再標準化刪除（解決空白字元問題）
                    try:
                        normalized_val = json.dumps(json.loads(item_val), separators=(',', ':'), ensure_ascii=False)
                        # 在 Redis 裡尋找符合標準化格式的項並刪除
                        for raw in all_raw:
                            try:
                                if json.dumps(json.loads(raw), separators=(',', ':'), ensure_ascii=False) == normalized_val:
                                    if redisConnect.redis_master.lrem(queue_name, 1, raw):
                                        count += 1
                                        break
                            except: continue
                    except:
                        pass
            flash(f'已成功從「{queue_name}」刪除 {count} 筆資料', 'success')
        elif key_type == 'hash':
            count = redisConnect.redis_master.hdel(queue_name, *selected_items)
            flash(f'已成功從「{queue_name}」刪除 {count} 個欄位', 'success')
        else:
            flash(f"不支援的資料類型: {key_type}", "error")
    except (RedisError, ConnectionError) as e:
        flash(f"刪除失敗：{str(e)}", "error")
        redisConnect.connect_to_master()

    # 解決 Codespaces 重導向問題：使用相對路徑
    resp = redirect(url_for('index', queue_name=queue_name))
    resp.headers['Location'] = resp.headers['Location'].replace(':5000', '')
    return resp

@app.route('/update_status', methods=['POST'])
@login_required
@role_required('admin')
def update_status():
    queue_name = request.form.get('queue_name')
    if not queue_name:
        return "缺少 queue_name", 400

    key_type = redisConnect.redis_master.type(queue_name)
    if key_type != 'hash':
        return f"佇列 {queue_name} 不是 hash 型態! 是 {key_type}", 400

    # 取得所有 status_* 欄位並轉成 rpa_name → status 的 dict
    updates = {
        key.replace("status_", ""): value
        for key, value in request.form.items()
        if key.startswith("status_")
    }

    try:
        with redisConnect.redis_master.pipeline() as pipe:
            for rpa_name, new_status in updates.items():
                raw = redisConnect.redis_master.hget(queue_name, rpa_name)
                try:
                    data = json.loads(raw)
                    data["status"] = new_status
                except (json.JSONDecodeError, TypeError):
                    data = {"status": new_status}  

                pipe.hset(queue_name, rpa_name, json.dumps(data))
            pipe.execute()
    except Exception as e:
        return f"更新失敗：{str(e)}", 500

    return redirect(url_for("index", queue_name=queue_name))

@app.route("/queue_lengths")
def queue_lengths():
    grouped_queues = {}

    for q in ALLOWED_QUEUES:
        if 'worker_status' in q:
            continue
        try:
            q_type = redisConnect.redis_master.type(q)
            length = redisConnect.redis_master.llen(q) if q_type == "list" else 0
        except redis.RedisError:
            q_type = "unknown"
            length = 0

        prefix = q.split("_", 1)[0]
        if prefix not in grouped_queues:
            grouped_queues[prefix] = []
        grouped_queues[prefix].append({"name": q, "length": length, "type": q_type})

    return render_template("queue_lengths.html", grouped_queues=grouped_queues, active_page='queue_lengths')

@app.route("/queue_lengths_partial")
def queue_lengths_partial():
    grouped_queues = {}
    for q in ALLOWED_QUEUES:
        if 'worker_status' in q:
            continue
        try:
            q_type = redisConnect.redis_master.type(q)
            length = redisConnect.redis_master.llen(q) if q_type == "list" else 0
        except redis.RedisError:
            q_type = "unknown"
            length = 0

        prefix = q.split("_", 1)[0]
        if prefix not in grouped_queues:
            grouped_queues[prefix] = []
        grouped_queues[prefix].append({"name": q, "length": length, "type": q_type})

    tbodies = []
    for prefix, queues in grouped_queues.items():
        tbody_html = render_template_string('''
            {% for q in queues %}
                {% if q.type != 'hash' %}
                <tr class="hover:bg-blue-50 cursor-pointer transition-colors group"
                    onclick="window.location.href='/?queue_name={{ q.name }}'">
                    <td class="py-3 px-4 font-mono text-xs text-gray-600 group-hover:text-blue-700">
                        {{ q.name }}
                    </td>
                    <td class="py-3 px-4 text-right">
                        <span class="{% if q.length > 20 %}text-red-600 font-bold{% else %}text-gray-900{% endif %}">
                            {{ q.length }}
                        </span>
                    </td>
                </tr>
                {% endif %}
            {% endfor %}
        ''', queues=queues)
        tbodies.append(tbody_html)

    return jsonify(tbodies)

@app.route("/worker_status")
def worker_status():
    return render_template("worker_status.html", active_page='worker_status')

@app.route("/worker_status_partial")
def worker_status_partial():
    import json
    queues = ["prober_worker_status", "LineNotify_worker_status", "LotActions_worker_status"]
    grouped_workers = {}

    try:
        for queue_name in queues:
            queue_workers = {}
            all_workers = redisConnect.redis_master.hgetall(queue_name)
            for k, v in all_workers.items():
                key_str = k.decode() if isinstance(k, bytes) else str(k)
                val_str = v.decode() if isinstance(v, bytes) else str(v)

                try:
                    json_val = json.loads(val_str)
                    status = json_val.get("status", "unknown").strip().lower()
                except Exception:
                    status = "unknown"

                queue_workers[key_str] = status
            grouped_workers[queue_name] = queue_workers
    except redis.RedisError:
        grouped_workers = {}

    return jsonify(grouped_workers)



@app.route('/update_equipments', methods=['POST'])
@login_required
def update_equipments():
    edit_mode = request.form.get('edit_mode', '0')
    current_user = session.get('username', '')

    if edit_mode == '1':
        with pgConnect.get_conn() as conn:
            # 讓 equipment_history 的 trigger 記得是誰做的異動
            pgConnect.set_current_user(conn, current_user)

            with conn.cursor() as cur:
                cur.execute("SELECT eqpid, eqptype, testerip, proberip, linegroup, floor, action FROM equipments")
                existing_map = {
                    row[0]: (row[1] or '', row[2] or '', row[3] or '', row[4] or '', row[5] or '', row[6] or '')
                    for row in cur.fetchall()
                }
                existing_ids = list(existing_map.keys())

                for key in existing_ids:
                    if request.form.get(f'delete_{key}'):
                        cur.execute("DELETE FROM equipments WHERE eqpid = %s", (key,))
                        continue

                    eqptype = request.form.get(f'EQPTYPE_{key}', '').strip()
                    testerip = request.form.get(f'TESTERIP_{key}', '').strip()
                    proberip = request.form.get(f'PROBERIP_{key}', '').strip()
                    linegroup = request.form.get(f'LINEGROUP_{key}', '').strip()
                    floor = request.form.get(f'floor_{key}', '').strip()
                    action = request.form.get(f'ACTION_{key}', '').strip()

                    if eqptype and testerip and proberip and linegroup and floor and action:
                        new_vals = (eqptype, testerip, proberip, linegroup, floor, action)
                        old_vals = existing_map.get(key)
                        if old_vals != new_vals:
                            cur.execute(
                                """
                                UPDATE equipments
                                   SET eqptype = %s, testerip = %s, proberip = %s,
                                       linegroup = %s, floor = %s, action = %s,
                                       updated_at = now()
                                 WHERE eqpid = %s
                                """,
                                (eqptype, testerip, proberip, linegroup, floor, action, key)
                            )

                new_eqpids = request.form.getlist('new_EQPID[]')
                new_eqptypes = request.form.getlist('new_EQPTYPE[]')
                new_testerips = request.form.getlist('new_TESTERIP[]')
                new_proberips = request.form.getlist('new_PROBERIP[]')
                new_linegroups = request.form.getlist('new_LINEGROUP[]')
                new_floors = request.form.getlist('new_floor[]')
                new_actions = request.form.getlist('new_ACTION[]')

                for i in range(len(new_eqpids)):
                    eqpid = new_eqpids[i].strip()
                    if not eqpid or eqpid in existing_ids:
                        continue

                    if (new_eqptypes[i].strip() and new_testerips[i].strip() and
                            new_proberips[i].strip() and new_linegroups[i].strip() and
                            new_floors[i].strip() and new_actions[i].strip()):
                        cur.execute(
                            """
                            INSERT INTO equipments (eqpid, eqptype, testerip, proberip, linegroup, floor, action)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                eqpid,
                                new_eqptypes[i].strip(),
                                new_testerips[i].strip(),
                                new_proberips[i].strip(),
                                new_linegroups[i].strip(),
                                new_floors[i].strip(),
                                new_actions[i].strip(),
                            )
                        )
        return redirect(url_for("equipments"))

    return redirect(url_for("equipments"))


@app.route('/equipment_history')
@login_required
def equipment_history():
    """查看單一 EQPID 的異動紀錄（equipment_history 表由 PostgreSQL trigger 自動寫入）"""
    eqpid = request.args.get('eqpid', '').strip()
    history = []
    error = None
    try:
        with pgConnect.get_conn() as conn:
            with pgConnect.dict_cursor(conn) as cur:
                if eqpid:
                    cur.execute(
                        """
                        SELECT * FROM equipment_history
                         WHERE eqpid = %s
                         ORDER BY changed_at DESC
                         LIMIT 200
                        """,
                        (eqpid,)
                    )
                else:
                    cur.execute(
                        """
                        SELECT * FROM equipment_history
                         ORDER BY changed_at DESC
                         LIMIT 200
                        """
                    )
                history = cur.fetchall()
        if not history:
            error = "沒有找到異動紀錄。"
    except Exception as e:
        error = f"查詢失敗：{e}"

    return render_template(
        'equipment_history.html',
        history=history,
        eqpid=eqpid,
        error=error,
        active_page='equipment_history'
    )

@app.route('/register', methods=['GET', 'POST'])
@role_required('admin')
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        role = request.form['role']
        try:
            with pgConnect.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM users WHERE username = %s", (username,))
                    if cur.fetchone():
                        flash('使用者已存在', 'error')
                    else:
                        hashed = generate_password_hash(password)
                        cur.execute(
                            "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s)",
                            (username, hashed, role)
                        )
                        flash('使用者新增成功', 'success')
        except Exception as e:
            flash(f'新增失敗：{e}', 'error')
        return redirect(url_for('register'))

    # 回傳目前所有使用者
    user_list = []
    with pgConnect.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT username, role FROM users ORDER BY username")
            user_list = [{'username': u, 'role': r} for u, r in cur.fetchall()]

    return render_template('register.html', user_list=user_list, active_page='register')


@app.route('/delete_user', methods=['POST'])
@role_required('admin')
def delete_user():
    username = request.form['username']
    if username == 'admin':
        flash('不能刪除 admin 使用者', 'error')
    else:
        with pgConnect.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM users WHERE username = %s", (username,))
        flash(f'{username} 已刪除', 'success')
    return redirect(url_for('register'))


@app.route('/update_user_password', methods=['POST'])
@role_required('admin')
def update_user_password():
    username = request.form.get('username', '').strip()
    new_password = request.form.get('new_password', '').strip()

    if not username:
        flash('缺少使用者名稱', 'error')
        return redirect(url_for('register'))
    if not new_password:
        flash('新密碼不可為空', 'error')
        return redirect(url_for('register'))

    with pgConnect.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE username = %s", (username,))
            if cur.fetchone() is None:
                flash('使用者不存在', 'error')
                return redirect(url_for('register'))

            hashed = generate_password_hash(new_password)
            cur.execute(
                "UPDATE users SET password_hash = %s WHERE username = %s",
                (hashed, username)
            )
    flash(f'{username} 的密碼已更新', 'success')
    return redirect(url_for('register'))


@app.route('/equipments', methods=['GET', 'POST'])
def equipments():
    error = None
    # hash_data 維持跟 Jinja 樣板相同的結構：{eqpid: json字串}，
    # 這樣 equipments.html 裡原本的 |from_json 樣板邏輯不用改
    hash_data = {}
    user_role = session.get('role', 'viewer')

    # Get filter criteria from request args, converting to uppercase for case-insensitive matching
    filter_floor = request.args.get('floor', '').strip()
    filter_eqptype = request.args.get('eqptype', '').strip()
    filter_eqpid = request.args.get('eqpid', '').strip()

    try:
        conditions = []
        params = []
        if filter_floor:
            conditions.append("floor ILIKE %s")
            params.append(filter_floor)
        if filter_eqptype:
            conditions.append("eqptype ILIKE %s")
            params.append(filter_eqptype)
        if filter_eqpid:
            conditions.append("eqpid ILIKE %s")
            params.append(filter_eqpid)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        with pgConnect.get_conn() as conn:
            with pgConnect.dict_cursor(conn) as cur:
                cur.execute(f"SELECT * FROM equipments {where_clause} ORDER BY eqpid", params)
                rows = cur.fetchall()

        for row in rows:
            hash_data[row['eqpid']] = json.dumps({
                'EQPTYPE': row['eqptype'],
                'TESTERIP': row['testerip'],
                'PROBERIP': row['proberip'],
                'LINEGROUP': row['linegroup'],
                'floor': row['floor'],
                'Action': row['action'],
            }, ensure_ascii=False)

        if not hash_data and (filter_floor or filter_eqptype or filter_eqpid):
            error = "沒有找到符合篩選條件的資料。"
        elif not hash_data:
            error = "「equipments」沒有內容。"

    except Exception as e:
        error = "無法連接到資料庫，請稍後再試。"
        hash_data = {}

    return render_template(
        'equipments.html',
        error=error,
        hash_data=hash_data,
        user_role=user_role,
        filter_floor=filter_floor,
        filter_eqptype=filter_eqptype,
        filter_eqpid=filter_eqpid,
        active_page='equipments'
    )

def is_valid_json(s):
    try:
        json.loads(s)
        return True
    except:
        return False
    
@app.route('/script_update', methods=['GET', 'POST'])
@login_required
@role_required('admin')
def script_update():
    import requests
    import json

    # 讀 conf.json
    CONFIG_PATH = os.path.join(os.path.dirname(__file__), "conf.json")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        conf_list = json.load(f)

    # 從 worker_status_partial 抓所有 RPA
    filtered_workers = {}
    queues = ["prober_worker_status", "LineNotify_worker_status", "LotActions_worker_status"]
    try:
        for queue_name in queues:
            all_workers = redisConnect.redis_master.hgetall(queue_name)
            for k, v in all_workers.items():
                key_str = k.decode() if isinstance(k, bytes) else str(k)
                val_str = v.decode() if isinstance(v, bytes) else str(v)
                try:
                    json_val = json.loads(val_str)
                    status = json_val.get("status", "unknown").strip().lower()
                except Exception:
                    status = "unknown"
                filtered_workers[key_str] = status
    except redis.RedisError:
        filtered_workers = {}

    rpa_list = [{"name": name, "status": status} for name, status in filtered_workers.items()]

    if request.method == 'POST':
        selected_rpas = request.form.getlist('rpa')
        source_path = request.form.get('source_path', '').strip()
        target_path = request.form.get('target_path', '').strip()

        if not selected_rpas:
            flash("請至少選擇一個 RPA", "error")
            return redirect(url_for('script_update'))
        if not source_path or not target_path:
            flash("Source Path 與 Target Path 為必填欄位", "error")
            return redirect(url_for('script_update'))

        for rpa_name in selected_rpas:
            conf = next((c for c in conf_list if c['name'] == rpa_name), None)
            if not conf:
                flash(f"{rpa_name} 對應的設定不存在", "error")
                continue

            payload = {
                "projectID": "86HklPenDDAsPmtc",
                "taskID": "KT59-m35TfGliLDH",
                "args" : [source_path,target_path]
            }

            api_url = conf.get('url')
            headers = {"Content-Type": "application/json", "X-API-KEY": conf.get("X-API-KEY")}

            try:
                response = requests.post(api_url, json=payload, headers=headers, timeout=15,verify=False )
                if response.status_code == 200:
                    flash(f"{rpa_name} 指令已成功發送", "success")
                else:
                    flash(f"{rpa_name} API 回傳錯誤：{response.status_code},{response}", "error")
            except Exception as e:
                flash(f"{rpa_name} 發送 API 失敗：{e}", "error")

        return redirect(url_for('script_update'))

    return render_template('script_update.html', rpa_list=rpa_list, active_page='script_update')

@app.route('/update_contacts', methods=['POST'])
@login_required
def update_contacts():
    edit_mode = request.form.get('edit_mode', '0')

    if edit_mode == '1':
        with pgConnect.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, eqpid, eqptype, action, linegroup, floor FROM contacts")
                existing_map = {
                    row[0]: (row[1] or '', row[2] or '', row[3] or '', row[4] or '', row[5] or '')
                    for row in cur.fetchall()
                }
                existing_ids = list(existing_map.keys())

                for key in existing_ids:
                    if request.form.get(f'delete_{key}'):
                        cur.execute("DELETE FROM contacts WHERE id = %s", (key,))
                        continue

                    eqpid = request.form.get(f'EQPID_{key}', '').strip()
                    eqptype = request.form.get(f'EQPTYPE_{key}', '').strip()
                    action = request.form.get(f'ACTION_{key}', '').strip()
                    linegroup = request.form.get(f'LINEGROUP_{key}', '').strip()
                    floor = request.form.get(f'floor_{key}', '').strip()

                    if eqpid and eqptype and action and linegroup and floor:
                        new_vals = (eqpid, eqptype, action, linegroup, floor)
                        old_vals = existing_map.get(key)
                        if old_vals != new_vals:
                            cur.execute(
                                """
                                UPDATE contacts
                                   SET eqpid = %s, eqptype = %s, action = %s,
                                       linegroup = %s, floor = %s
                                 WHERE id = %s
                                """,
                                (eqpid, eqptype, action, linegroup, floor, key)
                            )

                new_eqpids = request.form.getlist('new_EQPID[]')
                new_eqptypes = request.form.getlist('new_EQPTYPE[]')
                new_actions = request.form.getlist('new_ACTION[]')
                new_linegroups = request.form.getlist('new_LINEGROUP[]')
                new_floors = request.form.getlist('new_floor[]')

                new_row_count = max(len(new_eqpids), len(new_eqptypes), len(new_actions), len(new_linegroups), len(new_floors), default=0)
                for i in range(new_row_count):
                    eqpid = new_eqpids[i].strip() if i < len(new_eqpids) else ''
                    eqptype = new_eqptypes[i].strip() if i < len(new_eqptypes) else ''
                    action = new_actions[i].strip() if i < len(new_actions) else ''
                    linegroup = new_linegroups[i].strip() if i < len(new_linegroups) else ''
                    floor = new_floors[i].strip() if i < len(new_floors) else ''

                    if all(not value for value in [eqpid, floor, eqptype, action, linegroup]):
                        continue

                    # id 由 PostgreSQL 的 SERIAL 自動產生，不用再手動 INCR
                    cur.execute(
                        """
                        INSERT INTO contacts (eqpid, eqptype, action, linegroup, floor)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (eqpid, eqptype, action, linegroup, floor)
                    )

        return redirect(url_for("contacts"))

    return redirect(url_for("contacts"))


@app.route('/contacts', methods=['GET', 'POST'])
def contacts():
    error = None
    # hash_data 維持跟原本一樣的 {id: json字串} 結構，相容原本的 contacts.html 樣板
    hash_data = {}
    user_role = session.get('role', 'viewer')

    filter_floor = request.args.get('floor', '').strip()
    filter_eqptype = request.args.get('eqptype', '').strip()
    filter_eqpid = request.args.get('eqpid', '').strip()

    try:
        conditions = []
        params = []
        if filter_floor:
            conditions.append("floor ILIKE %s")
            params.append(filter_floor)
        if filter_eqptype:
            conditions.append("eqptype ILIKE %s")
            params.append(filter_eqptype)
        if filter_eqpid:
            conditions.append("eqpid ILIKE %s")
            params.append(filter_eqpid)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        with pgConnect.get_conn() as conn:
            with pgConnect.dict_cursor(conn) as cur:
                cur.execute(f"SELECT * FROM contacts {where_clause} ORDER BY id", params)
                rows = cur.fetchall()

        for row in rows:
            hash_data[str(row['id'])] = json.dumps({
                'EQPID': row['eqpid'],
                'EQPTYPE': row['eqptype'],
                'Action': row['action'],
                'LINEGROUP': row['linegroup'],
                'floor': row['floor'],
            }, ensure_ascii=False)

        if not hash_data and (filter_floor or filter_eqptype or filter_eqpid):
            error = "沒有找到符合篩選條件的資料。"
        elif not hash_data:
            error = "「contacts」沒有內容。"

    except Exception as e:
        error = "無法連接到資料庫，請稍後再試。"
        hash_data = {}

    return render_template(
        'contacts.html',
        error=error,
        hash_data=hash_data,
        user_role=user_role,
        filter_floor=filter_floor,
        filter_eqptype=filter_eqptype,
        filter_eqpid=filter_eqpid,
        active_page='contacts'
    )

@app.route('/queue_history')
@login_required
def queue_history():
    # 限定任務類別
    available_categories = ['LotActions', 'prober', 'LineNotify']

    category = request.args.get('category', '').strip()
    if not category:
        category = available_categories[0]

    # 時間一律固定為 dispatch_time
    time_field = 'dispatch_time'
    worker_name_filter = request.args.get('worker_name', '').strip()

    # 獲取所有 RPA 清單 (worker_status 維持在 Redis，不受這次改動影響)
    all_rpa_names = set()
    worker_queues = ["prober_worker_status", "LineNotify_worker_status", "LotActions_worker_status"]
    try:
        for q in worker_queues:
            keys = redisConnect.redis_master.hkeys(q)
            for k in keys:
                all_rpa_names.add(k)
    except: pass
    sorted_workers = sorted(list(all_rpa_names))

    # 時間模式：last (最近) 或 range (區間)
    mode = request.args.get('mode', 'last')

    local_tz = timezone(timedelta(hours=8))
    now_dt = datetime.now(tz=local_tz)

    if mode == 'range':
        start_str = request.args.get('start_time', '').strip()
        end_str = request.args.get('end_time', '').strip()
        try:
            parsed_start = safe_parse_datetime(start_str)
            cutoff = parsed_start.replace(tzinfo=local_tz) if parsed_start else now_dt - timedelta(hours=24)

            parsed_end = safe_parse_datetime(end_str)
            end_dt = parsed_end.replace(tzinfo=local_tz) if parsed_end else now_dt
        except:
            cutoff = now_dt - timedelta(hours=24)
            end_dt = now_dt
        hours = int((end_dt - cutoff).total_seconds() / 3600)
    else:
        try:
            hours = int(request.args.get('hours', 24))
        except ValueError:
            hours = 24
        if hours not in [1, 3, 6, 12, 24, 48, 72]:
            hours = 24
        cutoff = now_dt - timedelta(hours=hours)
        end_dt = now_dt

    try:
        interval_min = int(request.args.get('interval', 60))
    except ValueError:
        interval_min = 60
    if interval_min not in [10, 30, 60]:
        interval_min = 60

    chart_data = None
    error = None
    total_dispatched = 0
    total_failed = 0

    if category:
        try:
            # 定義標籤軸（維持跟原本一樣的顯示格式，方便前端不用改）
            all_labels = []
            bucket_origin = cutoff.replace(minute=(cutoff.minute // interval_min) * interval_min, second=0, microsecond=0)
            ptr = bucket_origin
            while ptr <= end_dt:
                all_labels.append(ptr.strftime('%m-%d %H:%M'))
                ptr += timedelta(minutes=interval_min)

            # 用一句 SQL 直接做時間分桶聚合，取代原本把整個 Redis list 撈進 Python 再手動算的寫法。
            # date_bin 把每一筆 dispatch_time 對齊到 interval_min 分鐘的桶子，直接在資料庫端 GROUP BY。
            sql = """
                SELECT
                    to_char(
                        date_bin(%(interval)s, dispatch_time, %(origin)s) AT TIME ZONE 'Asia/Taipei',
                        'MM-DD HH24:MI'
                    ) AS bucket,
                    status,
                    count(*) AS cnt
                FROM task_history
                WHERE category = %(category)s
                  AND dispatch_time BETWEEN %(start)s AND %(end)s
                  AND (%(worker)s = '' OR rpa_worker = %(worker)s)
                GROUP BY bucket, status
            """
            params = {
                'interval': timedelta(minutes=interval_min),
                'origin': bucket_origin,
                'category': category,
                'start': cutoff,
                'end': end_dt,
                'worker': worker_name_filter,
            }

            disp_bucket = defaultdict(int)
            fail_bucket = defaultdict(int)
            with pgConnect.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    for bucket_label, status, cnt in cur.fetchall():
                        if status == 'dispatched':
                            disp_bucket[bucket_label] += cnt
                            total_dispatched += cnt
                        elif status == 'failed':
                            fail_bucket[bucket_label] += cnt
                            total_failed += cnt

            chart_data = {
                'labels': all_labels,
                'counts_dispatched': [disp_bucket.get(lbl, 0) for lbl in all_labels],
                'counts_failed': [fail_bucket.get(lbl, 0) for lbl in all_labels]
            }

        except Exception as e:
            error = f"查詢失敗：{str(e)}"

    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        if error: return jsonify({'error': error}), 400
        return jsonify({
            'chart_data': chart_data,
            'total_dispatched': total_dispatched,
            'total_failed': total_failed,
            'category': category
        })

    return render_template(
        'queue_history.html',
        category=category,
        time_field=time_field,
        worker_name_filter=worker_name_filter,
        sorted_workers=sorted_workers,
        hours=hours,
        mode=mode,
        start_time=request.args.get('start_time', ''),
        end_time=request.args.get('end_time', ''),
        interval=interval_min,
        chart_data=chart_data,
        error=error,
        total_dispatched=total_dispatched,
        total_failed=total_failed,
        available_categories=available_categories,
        active_page='queue_history'
    )

@app.route('/api_trigger', methods=['GET', 'POST'])
@login_required
def api_trigger():
    EXTERNAL_API_BASE = "http://10.97.210.35:8000"
    user_role = session.get('role', 'viewer')
    
    # 獲取所有機台編號供下拉選單使用（equipments 已搬到 PostgreSQL）
    eqp_list = []
    try:
        with pgConnect.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT eqpid FROM equipments ORDER BY eqpid")
                eqp_list = [row[0] for row in cur.fetchall()]
    except: pass

    if request.method == 'POST':
        endpoint = request.form.get('endpoint', '/EQPAction')
        eqpid = request.form.get('eqpid', '').strip()
        action = request.form.get('action', 'LotEnd').strip()
        
        payload = { "EQPID": eqpid, "Action": action }
        
        # 針對 DataEntry 補上額外欄位
        if endpoint == '/EQPAction_DataEntry':
            payload.update({
                "TesterInput": request.form.get('tester_input', ''),
                "ProberInput": request.form.get('prober_input', ''),
                "Mode": request.form.get('mode', 'Auto')
            })

        target_url = f"{EXTERNAL_API_BASE}{endpoint}"
        try:
            if endpoint == '/ContactInquiry':
                # ContactInquiry 是 GET 請求
                response = requests.get(target_url, params=payload, timeout=5)
            else:
                # 其他是 POST 請求
                response = requests.post(target_url, json=payload, timeout=5)
            
            result = {
                "status_code": response.status_code,
                "reason": response.reason,
                "content": response.text
            }
            return render_template('api_trigger.html', eqp_list=eqp_list, result=result, last_payload=payload, endpoint=endpoint, user_role=user_role, active_page='api_trigger')
        except Exception as e:
            return render_template('api_trigger.html', eqp_list=eqp_list, error=f"連線失敗: {str(e)}", user_role=user_role, active_page='api_trigger')

    return render_template('api_trigger.html', eqp_list=eqp_list, user_role=user_role, active_page='api_trigger')

# ── Dashboard ──────────────────────────────────────────────────
@app.route('/dashboard')
@login_required
def dashboard():
    user_role = session.get('role', 'viewer')
    return render_template('dashboard.html', user_role=user_role, active_page='dashboard')


@app.route('/dashboard_data')
@login_required
def dashboard_data():
    """Dashboard 用的單一聚合 API，前端每 5 秒輪詢一次"""
    local_tz = timezone(timedelta(hours=8))
    now_dt   = datetime.now(tz=local_tz)
    mode = request.args.get('mode', 'last')
    if mode == 'range':
        parsed_start = safe_parse_datetime(request.args.get('start_time', '').strip())
        parsed_end = safe_parse_datetime(request.args.get('end_time', '').strip())
        cutoff = parsed_start.replace(tzinfo=local_tz) if parsed_start else now_dt - timedelta(hours=6)
        end_dt = parsed_end.replace(tzinfo=local_tz) if parsed_end else now_dt
    else:
        try:
            hours = int(request.args.get('hours', 6))
        except ValueError:
            hours = 6
        cutoff = now_dt - timedelta(hours=hours)
        end_dt = now_dt

    try:
        interval_min = int(request.args.get('interval', 30))
    except ValueError:
        interval_min = 30
    if interval_min not in [10, 30, 60]:
        interval_min = 30

    # ── 1. Queue 長度 ──────────────────────────────────────────
    queue_stats = []
    monitored_queues = [
        "LotActions_task_queue",
        "LotActions_failed_queue",
        "LotActions_dispatched_log",
        "LotActions_processing_queue",
        "LineNotify_task_queue",
        "LineNotify_failed_queue",
        "prober_task_queue",
        "prober_failed_queue",
    ]
    history_counts = defaultdict(int)
    try:
        with pgConnect.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT category, status, count(*)
                    FROM task_history
                    GROUP BY category, status
                    """
                )
                for category_name, status, cnt in cur.fetchall():
                    history_counts[(category_name, status)] = cnt
    except Exception:
        pass

    for q in monitored_queues:
        length = 0
        if q.endswith('_dispatched_log') or q.endswith('_failed_queue'):
            if q.endswith('_dispatched_log'):
                category_name = q[:-len('_dispatched_log')]
                status = 'dispatched'
            else:
                category_name = q[:-len('_failed_queue')]
                status = 'failed'
            length = history_counts.get((category_name, status), 0)
        else:
            try:
                q_type = redisConnect.redis_master.type(q)
                length = redisConnect.redis_master.llen(q) if q_type == 'list' else 0
            except Exception:
                length = 0
        queue_stats.append({"name": q, "length": length})

    # ── 2. Worker 狀態 ─────────────────────────────────────────
    worker_status_queues = [
        "prober_worker_status",
        "LineNotify_worker_status",
        "LotActions_worker_status",
    ]
    workers = {}
    idle_count = busy_count = ghost_count = offline_count = 0
    try:
        for wq in worker_status_queues:
            all_w = redisConnect.redis_master.hgetall(wq)
            for k, v in all_w.items():
                k = k.decode() if isinstance(k, bytes) else k
                v = v.decode() if isinstance(v, bytes) else v
                try:
                    data   = json.loads(v)
                    status = data.get("status", "unknown").strip().lower()
                except Exception:
                    status = "unknown"
                workers[k] = status
                if status == "idle":    idle_count    += 1
                elif status == "busy":  busy_count    += 1
                elif status == "ghost": ghost_count   += 1
                else:                   offline_count += 1
    except Exception:
        pass

    # ── 3. 今日任務統計（改用 SQL 對 task_history 直接算，取代原本掃描 Redis list）──
    categories = ["LotActions", "LineNotify", "prober"]
    today_start = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    total_dispatched_today = 0
    total_failed_today     = 0

    try:
        with pgConnect.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, count(*)
                    FROM task_history
                    WHERE category = ANY(%s)
                      AND dispatch_time >= %s
                    GROUP BY status
                    """,
                    (categories, today_start)
                )
                for status, cnt in cur.fetchall():
                    if status == 'dispatched':
                        total_dispatched_today = cnt
                    elif status == 'failed':
                        total_failed_today = cnt
    except Exception:
        pass

    # ── 4. 趨勢圖資料（使用者選擇的 category，預設 LotActions）──
    category = request.args.get("category", "LotActions")

    all_labels = []
    bucket_origin = cutoff.replace(
        minute=(cutoff.minute // interval_min) * interval_min,
        second=0, microsecond=0
    )
    ptr = bucket_origin
    while ptr <= end_dt:
        all_labels.append(ptr.strftime('%H:%M'))
        ptr += timedelta(minutes=interval_min)

    disp_bucket = defaultdict(int)
    fail_bucket = defaultdict(int)
    try:
        with pgConnect.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        to_char(date_bin(%(interval)s, dispatch_time, %(origin)s) AT TIME ZONE 'Asia/Taipei', 'HH24:MI') AS bucket,
                        status,
                        count(*)
                    FROM task_history
                    WHERE category = %(category)s
                      AND dispatch_time BETWEEN %(start)s AND %(end)s
                    GROUP BY bucket, status
                    """,
                    {
                        'interval': timedelta(minutes=interval_min),
                        'origin': bucket_origin,
                        'category': category,
                        'start': cutoff,
                        'end': end_dt,
                    }
                )
                for bucket_label, status, cnt in cur.fetchall():
                    if status == 'dispatched':
                        disp_bucket[bucket_label] += cnt
                    elif status == 'failed':
                        fail_bucket[bucket_label] += cnt
    except Exception:
        pass

    chart_data = {
        "labels":            all_labels,
        "counts_dispatched": [disp_bucket.get(l, 0) for l in all_labels],
        "counts_failed":     [fail_bucket.get(l, 0) for l in all_labels],
    }

    return jsonify({
        "queue_stats":             queue_stats,
        "workers":                 workers,
        "worker_summary":          {
            "idle": idle_count, "busy": busy_count,
            "ghost": ghost_count, "offline": offline_count,
            "total": len(workers)
        },
        "total_dispatched_today":  total_dispatched_today,
        "total_failed_today":      total_failed_today,
        "chart_data":              chart_data,
        "category":                category,
        "generated_at":            now_dt.strftime('%H:%M:%S'),
    })


if __name__ == '__main__':
    threading.Thread(target=redisConnect.listen_for_failover, daemon=True)
    app.run(host='0.0.0.0', port=5000, debug=False)
    

# cd C:\Users\RPA_TEST\Downloads\a
# docker-compose up -d --build

# docker exec -it redis-master redis-cli
# KEYS *
# lrange task_queue 0 -1
# HSET LotActions_worker_status LotActions-RPA-BOT-01 '{"status": "busy", "floor": "2AF"}'

# lrange queue_history:prober_failed_queue 0 -1
