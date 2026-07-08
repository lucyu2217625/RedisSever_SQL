import redis
import time
from redis.sentinel import Sentinel, MasterNotFoundError
import os

MASTER_NAME = 'mymaster'


# ① 從環境變數拿，如果沒設就 fallback 用 service name
sentinel_hosts = os.getenv(
    "REDIS_SENTINELS",
    "sentinel1:26379,sentinel2:26379,sentinel3:26379"
).split(",")

# ② 解析成 [(host, port), ...]，並把 port 轉成 int
SENTINELS = []
for item in sentinel_hosts:
    host, port = item.split(":")
    SENTINELS.append((host, int(port)))
    
# docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" sentinel1  看SENTINELS在docker的IP


sentinel = Sentinel(SENTINELS, socket_timeout=5)
redis_master = None


def connect_to_master(retry=10, delay=1):
    global redis_master
    for i in range(retry):
        try:
            host, port = sentinel.discover_master(MASTER_NAME) # 取得目前主節點的 IP 跟 Port
            print(f"🔍 dis-Sentinel 提供 master 資訊：{host}:{port}")
            redis_master = redis.StrictRedis(host=host, port=port, socket_timeout=60,retry_on_timeout=True,decode_responses=True) # 建立 Redis 連線
            redis_master.ping() # 確認這個 master 活著 , 沒回應就丟出錯誤 → 跳到 except
            print(f"✅ dis-Redis master 連線成功（第 {i+1} 次嘗試）")
            time.sleep(3)
            return
        except (MasterNotFoundError, redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            print(f"⏳ dis-Redis master 尚未就緒，第 {i+1}/{retry} 次重試中… {repr(e)}")
            time.sleep(delay)
    raise RuntimeError(f"❌ dis-在 {retry} 次重試後仍找不到 Redis master，請檢查 Sentinel 狀態。")

def listen_for_failover():
    idx = 0
    connect_to_master()
    while True:
        host, port = SENTINELS[idx % len(SENTINELS)]
        idx += 1
        try:

            sentinel_client = redis.StrictRedis(host=host, port=port, socket_timeout=60) # 建立 Redis 連線
            pubsub = sentinel_client.pubsub()  # 裝監聽器
            pubsub.subscribe('+switch-master') # 訂閱 Sentinel 專屬的頻道 +switch-master
            print(f"🔔 dis-訂閱 +switch-master 等待事件… {pubsub}\n")
            for message in pubsub.listen(): #　有收到訊息才進迴圈
                if message['type'] == 'message':
                    data = message['data'].decode() # 將bytes解碼成字串
                    print(f"⚡️ dis-收到事件: {data}")
                    parts = data.split()
                    if len(parts) >= 5:
                        connect_to_master()
                        print("✅ dis-Redis 連線更新完成")
        except Exception as e:
            print(f"⚠️ dis-監聽失敗，5 秒後重試…錯誤：{e}")
            time.sleep(5)


def exists(key):
    return redis_master.exists(key)

def type(key):
    return redis_master.type(key)

def llen(key):
    return redis_master.llen(key)

def lrange(key, start, end):
    return redis_master.lrange(key, start, end)

def hgetall(key):
    return redis_master.hgetall(key)