# Redis 管理介面 - Docker Compose 部署文件

這是一份針對本專案 `docker-compose.yaml` 的技術說明文件。該設定檔用於快速部署一個包含 Redis 主從複製 (Master-Slave)、哨兵監控 (Sentinel) 以及 Web 管理介面的完整高可用性 (HA) 環境。

## 系統架構概覽

此 Docker Compose 設定檔會啟動以下 7 個服務，共同組成一個完整的 Redis 叢集與監控系統：

1.  **`web`**: 基於 Flask 的 Web 應用程式，提供一個圖形化介面來查看和管理 Redis 中的資料。
2.  **`redis-master`**: Redis 主節點，處理所有寫入請求。
3.  **`redis-slave1`, `redis-slave2`**: 兩個 Redis 從節點，從主節點複製資料，用於讀取負載分擔和故障備援。
4.  **`sentinel1`, `sentinel2`, `sentinel3`**: 三個 Redis Sentinel 節點，負責監控主從節點的健康狀態，並在主節點故障時自動執行故障轉移 (Failover)。

## 服務詳解

| 服務名稱       | 容器名稱 (`container_name`) | 映像檔 (`image`) | 對外開放埠 (`ports`) | 功能說明                                                     |
| :------------- | :-------------------------- | :--------------- | :------------------- | :----------------------------------------------------------- |
| **`web`**      | `queue_viewer`              | (從 Dockerfile 建置) | `5000:5000`          | Flask Web 應用程式，提供 Redis 資料的可視化管理介面。        |
| **`redis-master`** | `redis-master`              | `redis:7`        | `6379:6379`          | Redis 主節點，負責資料的寫入與主要讀取。                     |
| **`redis-slave1`** | `redis-slave1`              | `redis:7`        | `6380:6379`          | Redis 從節點 (副本)，同步主節點資料。                       |
| **`redis-slave2`** | `redis-slave2`              | `redis:7`        | `6381:6379`          | Redis 從節點 (副本)，同步主節點資料。                       |
| **`sentinel1`**  | `sentinel1`                 | (從 Dockerfile 建置) | `26379:26379`        | Redis Sentinel 哨兵節點，監控叢集狀態。                      |
| **`sentinel2`**  | `sentinel2`                 | (從 Dockerfile 建置) | `26380:26379`        | Redis Sentinel 哨兵節點，監控叢集狀態。                      |
| **`sentinel3`**  | `sentinel3`                 | (從 Dockerfile 建置) | `26381:26379`        | Redis Sentinel 哨兵節點，監控叢集狀態。                      |

## 部署步驟

在啟動容器之前，有**兩項關鍵的前置作業**必須完成。

### 步驟 1: 建立外部網路

所有服務都依賴一個名為 `redisnet` 的外部 Docker 網路。您必須手動建立此網路，才能讓容器之間透過靜態 IP 進行通訊。

**注意**：`docker-compose.yaml` 檔案中定義的 `ipam` 子網路 (`172.20.0.0/16`) 與服務中設定的靜態 IP (`172.18.0.x`) 不符。請使用以下**正確的指令**來建立網路，以確保靜態 IP 能正常運作：

```bash
docker network create --driver bridge --subnet 172.18.0.0/16 redisnet
```

### 步驟 2: 確認設定檔與資料路徑

1.  **設定檔**: 請確保以下檔案與 `docker-compose.yaml` 位於同一目錄：
    -   `redis-master.conf`
    -   `redis-slave1.conf`
    -   `redis-slave2.conf`
    -   `Dockerfile.sentinel`
    -   `Dockerfile.web`
    -   以及 `web` 服務所需的其他應用程式檔案。

2.  **Master 資料持久化**: `redis-master` 服務的資料目錄被對應到主機的 `/d/redis_data/master` 路徑。
    -   請確保您主機上的 `D:\redis_data\master` 目錄存在，或者將 `docker-compose.yaml` 中的路徑修改為您希望儲存資料的位置。
    -   `redis-slave` 服務則使用 Docker 的命名磁碟區 (named volumes)，不需手動建立資料夾。

### 步驟 3: 啟動所有服務

完成上述準備後，在 `docker-compose.yaml` 所在的目錄下執行以下指令：

```bash
docker-compose up -d --build
```

`--build` 參數會強制重新建置 `web` 和 `sentinel` 的映像檔，建議在初次啟動或程式碼有變更時使用。

### 步驟 4: 存取服務

-   **Web 管理介面**:
    -   打開瀏覽器，訪問 `http://localhost:5000`

-   **Redis 節點**:
    -   主節點: `localhost:6379`
    -   從節點 1: `localhost:6380`
    -   從節點 2: `localhost:6381`

-   **Sentinel 節點**:
    -   哨兵 1: `localhost:26379`
    -   哨兵 2: `localhost:26380`
    -   哨兵 3: `localhost:26381`

## 維運與管理

### 查看服務日誌

若要查看特定服務的即時輸出日誌，可使用以下指令：
```bash
# 查看 Web 應用的日誌
docker-compose logs -f web

# 查看 Redis 主節點的日誌
docker-compose logs -f redis-master
```

### 停止與移除容器

```bash
docker-compose down
```
此指令會停止並移除所有相關容器。若要一併移除 `redis-slave` 的命名磁碟區，可以加上 `-v` 參數：`docker-compose down -v`。

### 連線至 Redis 進行偵錯

-   **連線至 Master**:
    ```bash
    redis-cli -p 6379
    ```
-   **查看 Master 複製狀態**:
    ```bash
    redis-cli -p 6379 INFO replication
    ```
-   **連線至 Sentinel**:
    ```bash
    redis-cli -p 26379
    ```
-   **查看 Sentinel 狀態**:
    ```bash
    # 在 redis-cli 中執行
    INFO sentinel
    # 或直接從外部執行
    redis-cli -p 26379 INFO sentinel
    ```