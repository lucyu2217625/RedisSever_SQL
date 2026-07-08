#!/bin/sh
set -e

# 等待 redis-master 可以解析（例如從 docker-compose DNS）
for i in $(seq 1 30); do
    IP=$(getent hosts redis-master | awk '{ print $1 }')
    if [ -n "$IP" ]; then
        echo "[INFO] redis-master 可解析，IP 為 $IP"
        break
    fi
    echo "[WAIT] redis-master 尚未可解析，等待中（$i 秒）..."
    sleep 1
done

if [ -z "$IP" ]; then
    echo "[ERROR] redis-master 無法解析，放棄啟動"
    exit 1
fi

# 👇 宣告自己對外應該使用的 IP
ANNOUNCE_IP=${ANNOUNCE_IP:-192.168.162.18}

# ✅ 正確的 Sentinel port 是 26379，不是 Redis port（6379）
cat <<EOF > /etc/sentinel.conf
port 26379
sentinel monitor mymaster $IP 6379 2
sentinel down-after-milliseconds mymaster 5000
sentinel parallel-syncs mymaster 1
sentinel failover-timeout mymaster 10000
sentinel announce-ip $ANNOUNCE_IP
sentinel announce-port 26379
EOF

echo "[INFO] 啟動 redis-sentinel..."

exec redis-server /etc/sentinel.conf --sentinel
# sentinel announce-ip $ANNOUNCE_IP
# sentinel announce-port 26379