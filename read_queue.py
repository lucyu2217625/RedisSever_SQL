from flask import Flask, render_template, request, Response, redirect, url_for, flash, session,jsonify,render_template_string, send_from_directory
import redis
import json
import os
import redisConnect
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

def init_admin_user():
    try:
        if not redisConnect.redis_master.hexists('users', 'admin'):
            hashed = generate_password_hash('admin123')
            redisConnect.redis_master.hset('users', 'admin', f'{hashed}:admin')
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
                user_data = redisConnect.redis_master.hget('users', session['username'])
                if not user_data:
                    flash('使用者資料不存在', 'error')
                    return redirect(url_for('login'))
                user_role = user_data.rsplit(':',1)[1] if user_data and ':' in user_data else 'viewer'
                if user_role != role:
                    flash('您沒有權限執行此操作', 'error')
                    return redirect(url_for('index'))
                return f(*args, **kwargs)
            except (RedisError, ConnectionError) as e:
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
            user_data = redisConnect.redis_master.hget('users', username)
            if user_data and ':' in user_data:
                stored_hash, stored_role = user_data.rsplit(':', 1)  # 改這一行！
                if check_password_hash(stored_hash, password):
                    session['username'] = username
                    session['role'] = stored_role
                    # flash('登入成功！', 'success')
                    return redirect(url_for('index'))
                else:
                    return render_template('login.html', error=f'使用者名稱或密碼錯誤')
            else:
                return render_template('login.html', error='使用者名稱或密碼錯誤2')
        except (RedisError, ConnectionError):
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
    user_role = session.get('role', 'viewer')
    total_pages = 1 # Initialize total_pages here

    if request.method == 'POST':
        queue_name = request.form.get('queue_name', '').strip()
    else:
        queue_name = request.args.get('queue_name', '').strip()

    available_queues = ALLOWED_QUEUES

    if queue_name in ALLOWED_QUEUES:
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
        user_role=user_role,
        active_page='index'
    )

def parse_json(item):
    try:
        return json.loads(item)
    except json.JSONDecodeError:
        return item

@app.route('/download')
@login_required
def download():
    queue_name = request.args.get('queue_name', '').strip()

    if not queue_name:
        return "錯誤：請選擇有效的佇列名稱", 400

    try:
        data = []
        if redisConnect.redis_master.exists(queue_name):
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
    queue_name = 'equipments'
    edit_mode = request.form.get('edit_mode', '0')

    if edit_mode == '1':
        for key in redisConnect.redis_master.hkeys(queue_name):
            if request.form.get(f'delete_{key}'):
                redisConnect.redis_master.hdel(queue_name, key)
                continue

            eqptype = request.form.get(f'EQPTYPE_{key}', '')
            testerip = request.form.get(f'TESTERIP_{key}', '')
            proberip = request.form.get(f'PROBERIP_{key}', '')
            linegroup = request.form.get(f'LINEGROUP_{key}', '')
            floor = request.form.get(f'floor_{key}', '')
            action = request.form.get(f'ACTION_{key}', '')

            if eqptype and testerip and proberip and linegroup and floor and action:
                data = {
                    'EQPTYPE': eqptype,
                    'TESTERIP': testerip,
                    'PROBERIP': proberip,
                    'LINEGROUP': linegroup,
                    'floor': floor,
                    'Action': action
                }
                redisConnect.redis_master.hset(queue_name, key, json.dumps(data))

        new_eqpids = request.form.getlist('new_EQPID[]')
        new_eqptypes = request.form.getlist('new_EQPTYPE[]')
        new_testerips = request.form.getlist('new_TESTERIP[]')
        new_proberips = request.form.getlist('new_PROBERIP[]')
        new_linegroups = request.form.getlist('new_LINEGROUP[]')
        new_floors = request.form.getlist('new_floor[]')
        new_actions = request.form.getlist('new_ACTION[]') # Add this line

        for i in range(len(new_eqpids)):
            eqpid = new_eqpids[i].strip()
            if not eqpid:
                continue
            if redisConnect.redis_master.hexists(queue_name, eqpid):
                continue

            if (new_eqptypes[i].strip() and new_testerips[i].strip() and 
                new_proberips[i].strip() and new_linegroups[i].strip() and 
                new_floors[i].strip() and new_actions[i].strip()): # Add new_actions[i].strip() to condition
                data = {
                    'EQPTYPE': new_eqptypes[i].strip(),
                    'TESTERIP': new_testerips[i].strip(),
                    'PROBERIP': new_proberips[i].strip(),
                    'LINEGROUP': new_linegroups[i].strip(),
                    'floor': new_floors[i].strip(),
                    'Action': new_actions[i].strip() # Add this line
                }
                redisConnect.redis_master.hset(queue_name, eqpid, json.dumps(data))
        return redirect(url_for("equipments"))

    return redirect(url_for("equipments"))

@app.route('/register', methods=['GET', 'POST'])
@role_required('admin')
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        role = request.form['role']
        if redisConnect.redis_master.hexists('users', username):
            flash('使用者已存在', 'error')
        else:
            hashed = generate_password_hash(password)
            redisConnect.redis_master.hset('users', username, f'{hashed}:{role}')
            flash('使用者新增成功', 'success')
        return redirect(url_for('register'))

    # 回傳目前所有使用者
    user_list = []
    all_users = redisConnect.redis_master.hgetall('users')
    for uname, udata in all_users.items():
        role = udata.rsplit(':', 1)[-1]
        user_list.append({'username': uname, 'role': role})

    return render_template('register.html', user_list=user_list, active_page='register')


@app.route('/delete_user', methods=['POST'])
@role_required('admin')
def delete_user():
    username = request.form['username']
    if username == 'admin':
        flash('不能刪除 admin 使用者', 'error')
    else:
        redisConnect.redis_master.hdel('users', username)
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

    user_data = redisConnect.redis_master.hget('users', username)
    if not user_data or ':' not in user_data:
        flash('使用者不存在', 'error')
        return redirect(url_for('register'))

    role = user_data.rsplit(':', 1)[-1]
    hashed = generate_password_hash(new_password)
    redisConnect.redis_master.hset('users', username, f'{hashed}:{role}')
    flash(f'{username} 的密碼已更新', 'success')
    return redirect(url_for('register'))


@app.route('/equipments', methods=['GET', 'POST'])
def equipments():
    error = None
    hash_data = {}
    user_role = session.get('role', 'viewer')

    # Get filter criteria from request args, converting to uppercase for case-insensitive matching
    filter_floor = request.args.get('floor', '').strip().upper()
    filter_eqptype = request.args.get('eqptype', '').strip().upper()
    filter_eqpid = request.args.get('eqpid', '').strip().upper() # Add this line

    try:
        if not redisConnect.redis_master.exists('equipments'):
            error = "「equipments」沒有內容。"
            hash_data = {}
        else:
            all_hash_data = redisConnect.redis_master.hgetall('equipments')
            
            if not filter_floor and not filter_eqptype and not filter_eqpid:
                hash_data = all_hash_data
            else:
                hash_data = {} # Initialize hash_data here for the filtered results
                for key, val_str in all_hash_data.items():
                    try:
                        data = json.loads(val_str)
                        # Assume data is a dict, check if it matches filter criteria
                        floor_match = not filter_floor or data.get('floor', '').upper() == filter_floor
                        eqptype_match = not filter_eqptype or data.get('EQPTYPE', '').upper() == filter_eqptype
                        eqpid_match = not filter_eqpid or key.upper() == filter_eqpid # EQPID is the key itself
                        
                        if floor_match and eqptype_match and eqpid_match:
                            hash_data[key] = val_str
                    except (json.JSONDecodeError, AttributeError):
                        # Skip entries that are not valid JSON or not dicts
                        continue

            if not hash_data and (filter_floor or filter_eqptype or filter_eqpid):
                 error = "沒有找到符合篩選條件的資料。"
            elif not hash_data:
                error = "「equipments」沒有內容。"

    except (RedisError, ConnectionError) as e:
        error = "無法連接到資料庫，請稍後再試。"
        redisConnect.connect_to_master()
        hash_data = {}

    return render_template(
        'equipments.html',
        error=error,
        hash_data=hash_data,
        user_role=user_role,
        filter_floor=request.args.get('floor', '').strip(), # Pass original case back to template
        filter_eqptype=request.args.get('eqptype', '').strip(),
        filter_eqpid=request.args.get('eqpid', '').strip(), # Add this line
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
    queue_name = 'contacts'
    edit_mode = request.form.get('edit_mode', '0')

    if edit_mode == '1':
        for key in redisConnect.redis_master.hkeys(queue_name):
            if request.form.get(f'delete_{key}'):
                redisConnect.redis_master.hdel(queue_name, key)
                continue

            eqpid = request.form.get(f'EQPID_{key}', '')
            eqptype = request.form.get(f'EQPTYPE_{key}', '')
            action = request.form.get(f'ACTION_{key}', '')
            linegroup = request.form.get(f'LINEGROUP_{key}', '')
            floor = request.form.get(f'floor_{key}', '')

            if eqpid and eqptype and action and linegroup and floor:
                data = {
                    'EQPID': eqpid,
                    'EQPTYPE': eqptype,
                    'Action': action,
                    'LINEGROUP': linegroup,
                    'floor': floor
                }
                redisConnect.redis_master.hset(queue_name, key, json.dumps(data))

        new_eqpids = request.form.getlist('new_EQPID[]')
        new_eqptypes = request.form.getlist('new_EQPTYPE[]')
        new_actions = request.form.getlist('new_ACTION[]')
        new_linegroups = request.form.getlist('new_LINEGROUP[]')
        new_floors = request.form.getlist('new_floor[]')

        new_row_count = max(len(new_eqpids), len(new_eqptypes), len(new_actions), len(new_linegroups), len(new_floors))
        for i in range(new_row_count):
            eqpid = new_eqpids[i].strip() if i < len(new_eqpids) else ''
            eqptype = new_eqptypes[i].strip() if i < len(new_eqptypes) else ''
            action = new_actions[i].strip() if i < len(new_actions) else ''
            linegroup = new_linegroups[i].strip() if i < len(new_linegroups) else ''
            floor = new_floors[i].strip() if i < len(new_floors) else ''

            if all(not value for value in [eqpid, floor, eqptype, action, linegroup]):
                continue
            
            new_id = redisConnect.redis_master.incr('contacts_id_counter')
            data = {
                'EQPID': eqpid,
                'EQPTYPE': eqptype,
                'Action': action,
                'LINEGROUP': linegroup,
                'floor': floor
            }
            redisConnect.redis_master.hset(queue_name, new_id, json.dumps(data))
            
        return redirect(url_for("contacts"))

    return redirect(url_for("contacts"))


@app.route('/contacts', methods=['GET', 'POST'])
def contacts():
    error = None
    hash_data = {}
    user_role = session.get('role', 'viewer')

    # Get filter criteria from request args, converting to uppercase for case-insensitive matching
    filter_floor = request.args.get('floor', '').strip().upper()
    filter_eqptype = request.args.get('eqptype', '').strip().upper()
    filter_eqpid = request.args.get('eqpid', '').strip().upper()

    try:
        if not redisConnect.redis_master.exists('contacts'):
            error = "「contacts」沒有內容。"
            hash_data = {}
        else:
            all_hash_data = redisConnect.redis_master.hgetall('contacts')
            
            if not filter_floor and not filter_eqptype and not filter_eqpid:
                hash_data = all_hash_data
            else:
                hash_data = {} # Initialize hash_data here for the filtered results
                for key, val_str in all_hash_data.items():
                    try:
                        data = json.loads(val_str)
                        # Assume data is a dict, check if it matches filter criteria
                        floor_match = not filter_floor or data.get('floor', '').upper() == filter_floor
                        eqptype_match = not filter_eqptype or data.get('EQPTYPE', '').upper() == filter_eqptype
                        eqpid_match = not filter_eqpid or data.get('EQPID', '').upper() == filter_eqpid
                        
                        if floor_match and eqptype_match and eqpid_match:
                            hash_data[key] = val_str
                    except (json.JSONDecodeError, AttributeError):
                        # Skip entries that are not valid JSON or not dicts
                        continue

            if not hash_data and (filter_floor or filter_eqptype or filter_eqpid):
                 error = "沒有找到符合篩選條件的資料。"
            elif not hash_data:
                error = "「contacts」沒有內容。"

    except (RedisError, ConnectionError) as e:
        error = "無法連接到資料庫，請稍後再試。"
        redisConnect.connect_to_master()
        hash_data = {}

    return render_template(
        'contacts.html',
        error=error,
        hash_data=hash_data,
        user_role=user_role,
        filter_floor=request.args.get('floor', '').strip(),
        filter_eqptype=request.args.get('eqptype', '').strip(),
        filter_eqpid=request.args.get('eqpid', '').strip(),
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
    
    # 獲取所有 RPA 清單 (與 worker_status 來源一致)
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
            # 定義標籤軸
            all_labels = []
            ptr = cutoff.replace(minute=(cutoff.minute // interval_min) * interval_min, second=0, microsecond=0)
            while ptr <= end_dt:
                all_labels.append(ptr.strftime('%m-%d %H:%M'))
                ptr += timedelta(minutes=interval_min)

            def get_counts(q_suffix, target_category):
                potential_keys = [
                    f"queue_history:{target_category}_{q_suffix}",
                    f"{target_category}_{q_suffix}",
                    q_suffix
                ]
                raw_items = []
                for k in potential_keys:
                    if redisConnect.redis_master.exists(k):
                        raw_items = redisConnect.redis_master.lrange(k, 0, -1)
                        if raw_items: break
                
                if not raw_items: return defaultdict(int), 0
                
                bucket = defaultdict(int)
                count_sum = 0
                for raw in raw_items:
                    try:
                        item = json.loads(raw)
                        if isinstance(item, dict) and item.get('task_id') == '__INIT__': continue
                        if item.get('task_type') and item.get('task_type') != target_category: continue
                        
                        # RPA 名稱篩選
                        if worker_name_filter:
                            actual_worker = item.get('RPA_worker_name') or item.get('assigned_bot')
                            if actual_worker != worker_name_filter:
                                continue

                        ts_val = item.get(time_field)
                        if ts_val is None: ts_val = item.get('timestamp')
                        if ts_val is None: continue
                        
                        if isinstance(ts_val, (int, float)):
                            dt = datetime.fromtimestamp(ts_val, tz=local_tz)
                        else:
                            dt = safe_parse_datetime(str(ts_val))
                            if dt is None: continue # 無法解析則跳過
                            dt = dt.replace(tzinfo=local_tz) if dt.tzinfo is None else dt.astimezone(local_tz)
                        
                        if dt < cutoff or dt > end_dt: continue
                        
                        minute_rounded = (dt.minute // interval_min) * interval_min
                        bk = dt.replace(minute=minute_rounded, second=0, microsecond=0).strftime('%m-%d %H:%M')
                        bucket[bk] += 1
                        count_sum += 1
                    except: continue
                return bucket, count_sum

            disp_bucket, total_dispatched = get_counts("dispatched_log", category)
            fail_bucket, total_failed = get_counts("failed_queue", category)

            chart_data = {
                'labels': all_labels,
                'counts_dispatched': [disp_bucket.get(lbl, 0) for lbl in all_labels],
                'counts_failed': [fail_bucket.get(lbl, 0) for lbl in all_labels]
            }

        except Exception as e:
            error = f"查詢失敗：{str(e)}"
            redisConnect.connect_to_master()

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
    
    # 獲取所有機台編號供下拉選單使用
    eqp_list = []
    try:
        if redisConnect.redis_master.exists('equipments'):
            eqp_list = sorted(redisConnect.redis_master.hkeys('equipments'))
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
    cutoff   = now_dt - timedelta(hours=6)   # 趨勢圖固定顯示最近 6 小時
    interval_min = 30                         # 每 30 分鐘一個資料點

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
    # 只取 ALLOWED_QUEUES 裡有的
    for q in monitored_queues:
        if q not in ALLOWED_QUEUES:
            continue
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

    # ── 3. 今日任務統計（從 dispatched_log + failed_queue 計算）──
    categories = ["LotActions", "LineNotify", "prober"]
    today_start = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    total_dispatched_today = 0
    total_failed_today     = 0

    for cat in categories:
        for suffix, is_fail in [("dispatched_log", False), ("failed_queue", True)]:
            keys = [f"queue_history:{cat}_{suffix}", f"{cat}_{suffix}"]
            raw_items = []
            for k in keys:
                try:
                    if redisConnect.redis_master.exists(k):
                        raw_items = redisConnect.redis_master.lrange(k, 0, -1)
                        if raw_items:
                            break
                except Exception:
                    pass
            for raw in raw_items:
                try:
                    item = json.loads(raw)
                    if item.get("task_id") == "__INIT__":
                        continue
                    ts = item.get("dispatch_time") or item.get("timestamp")
                    if not ts:
                        continue
                    dt = safe_parse_datetime(str(ts))
                    if not dt: continue
                    dt = dt.replace(tzinfo=local_tz) if dt.tzinfo is None else dt.astimezone(local_tz)
                    if dt < today_start:
                        continue
                    if is_fail:
                        total_failed_today += 1
                    else:
                        total_dispatched_today += 1
                except Exception:
                    pass

    # ── 4. 趨勢圖資料（使用者選擇的 category，預設 LotActions）──
    category = request.args.get("category", "LotActions")

    all_labels = []
    ptr = cutoff.replace(
        minute=(cutoff.minute // interval_min) * interval_min,
        second=0, microsecond=0
    )
    while ptr <= now_dt:
        all_labels.append(ptr.strftime('%H:%M'))
        ptr += timedelta(minutes=interval_min)

    def _get_bucket(suffix):
        keys = [f"queue_history:{category}_{suffix}", f"{category}_{suffix}"]
        raw_items = []
        for k in keys:
            try:
                if redisConnect.redis_master.exists(k):
                    raw_items = redisConnect.redis_master.lrange(k, 0, -1)
                    if raw_items:
                        break
            except Exception:
                pass
        bucket = defaultdict(int)
        for raw in raw_items:
            try:
                item = json.loads(raw)
                if item.get("task_id") == "__INIT__":
                    continue
                ts = item.get("dispatch_time") or item.get("timestamp")
                if not ts:
                    continue
                dt = safe_parse_datetime(str(ts))
                if not dt: continue
                dt = dt.replace(tzinfo=local_tz) if dt.tzinfo is None else dt.astimezone(local_tz)

                if dt < cutoff or dt > now_dt:
                    continue
                m = (dt.minute // interval_min) * interval_min
                bk = dt.replace(minute=m, second=0, microsecond=0).strftime('%H:%M')
                bucket[bk] += 1
            except Exception:
                pass
        return bucket

    disp_bucket = _get_bucket("dispatched_log")
    fail_bucket = _get_bucket("failed_queue")

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
