# 多副本离线写入核对 API

接收 2～8 个副本离线产生的 1～120 条 put/delete 事件（可乱序），校验
向量时钟因果关系，并按向量时钟偏序解析出每个 key 的有效版本：

- 唯一极大事件是 put → 返回其值
- 唯一极大事件是 delete → 标记为已删除
- 存在多个极大事件（互不可比，即使值相同）→ 报告冲突

此外，`/api/catchup` 接口在同一批事件之上，为目标副本生成**可直接重放的
增量补传计划**：传入目标对各来源已连续接收的最高 seq（`known_clock`），
缺失事件按依赖分轮返回。

`/api/resolve-conflict` 接口把**已确认的冲突处理结果**写成一条新的、
可重放的事件：在写入副本上追加 put/delete，其时钟并入所有冲突版本及该副本
此前见过的历史；返回新事件与追加后该 key 的裁决。

上述三个接口完全无状态，**请求数据不落库**。

另外还提供一组**持久化历史**接口（`/api/histories`）：多个调用方先凭一份
合法批次创建历史并获得 ID，之后只按 ID 读取、汇入新的离线事件、或按 ID 提交
冲突处理参数（不必再传整个批次）。汇入按 `(replica_id, seq)` 合并：内容完全
相同的事件忽略，同标识异内容返回 **409**；冲突提交时会重新核对当前极大版本，
过期返回 **409**。历史保存在本地 SQLite（Docker 下位于命名卷），容器重启后
仍可读取。

此外还有一个**只读的撤除预演**接口：给定已提交历史的 ID 与一条事件的
`replica_id`、`seq`，预演把该离线事件撤除会波及哪些写入与 key——目标事件及
所有直接或间接依赖它的事件（跨 key）都会计入，无关并发事件保留；只返回预演
结果，**不实际删除、不重排序号或时钟、不改动已提交历史**。

最后是一组**导入会话**接口（`/api/histories/{id}/import-sessions`）：离线事件
分几次到达时，先按历史 ID 开一个空会话，把每批事件**暂存**进去（暂存对**新
标识**事件只校验单事件本身，允许前序或跨副本依赖尚未到达；对已暂存及同批
重复的 `(replica_id, seq)` 重传不做这些校验，而按 `key`、操作、值与**完整
clock**（键序无关）判等——同内容忽略，异内容 **409**，缺分量、多出零分量或
本分量改变都算异内容，不会误判为 422），收齐后一次**提交**——提交把暂存事件
与历史最新已提交状态按原规则合并（完整判等、因果校验、120 条总上限），成功即
封闭会话。会话容量按**已有暂存加本批去重后的不同标识**计数：一批发来 121 份
完全相同的合法事件只存一条；已满 120 条的会话重传已有事件仍成功，新标识超限
才 **422**。提交失败（缺依赖等）时开放会话与已暂存数据原样保留，补齐后可直接
重试。会话同样持久化在 SQLite 中，进程/容器重启后可继续暂存与提交。

会话还提供一个**只读缺口查询**接口
（`/api/histories/{id}/import-sessions/{sid}/missing-ranges`）：在**同一已提交
快照**上合并历史已提交事件与会话暂存事件，先按标识比对——暂存事件与已提交事件
同 `(replica_id, seq)` 但原完整内容不同即返回 **409**，一次性列出**全部**冲突
的 `replica_id`、`seq`（按二者排序），不返回缺口清单；无冲突时，对每个副本取
其本副本合并最大 seq 与所有事件 clock 对该副本分量的最大值作为需求上界，把
`1..上界` 中尚缺的序号合成**最大的连续闭区间**，按 `replica_id`、`start_seq`
排序返回。缺失序号**绝不逐个展开**——暂存 seq 为 `10^12` 也只返回一个区间；
上界为 0 的副本不产生区间。返回空列表只表示标识齐全，**不保证提交通过**（时钟
因果等仍在提交时校验）。封闭会话也可查询；查询不提交会话，也不改动历史、暂存
与状态。

## 启动

需要 Docker 与 Docker Compose v2（`docker compose` 子命令）。

```bash
docker compose up --build -d
```

停止：

```bash
docker compose down
```

## 访问地址与端口

| 用途 | 地址 |
| --- | --- |
| 健康检查 | <http://localhost:8000/health> |
| 裁决接口 | `POST http://localhost:8000/api/resolve` |
| 冲突处理事件接口 | `POST http://localhost:8000/api/resolve-conflict` |
| 增量补传接口 | `POST http://localhost:8000/api/catchup` |
| 创建持久历史 | `POST http://localhost:8000/api/histories` |
| 读取历史（批次+裁决） | `GET http://localhost:8000/api/histories/{id}` |
| 汇入离线事件 | `POST http://localhost:8000/api/histories/{id}/events` |
| 提交冲突处理 | `POST http://localhost:8000/api/histories/{id}/resolve-conflict` |
| 撤除预演（只读，不落任何写入） | `GET http://localhost:8000/api/histories/{id}/withdraw-preview?replica_id=r1&seq=1` |
| 创建导入会话 | `POST http://localhost:8000/api/histories/{id}/import-sessions` |
| 查询会话状态与暂存事件 | `GET http://localhost:8000/api/histories/{id}/import-sessions/{sid}` |
| 暂存一批事件 | `POST http://localhost:8000/api/histories/{id}/import-sessions/{sid}/events` |
| 提交会话（汇入并封闭） | `POST http://localhost:8000/api/histories/{id}/import-sessions/{sid}/commit` |
| 查询仍需补取的缺口区间（只读，不提交） | `GET http://localhost:8000/api/histories/{id}/import-sessions/{sid}/missing-ranges` |
| Swagger UI | <http://localhost:8000/docs> |
| OpenAPI 文档 | <http://localhost:8000/openapi.json> |

容器内监听 `0.0.0.0:8000`，映射到宿主机 **8000** 端口。

## 请求格式

```json
{
  "replica_ids": ["r1", "r2"],
  "events": [
    {
      "replica_id": "r1",
      "seq": 1,
      "key": "name",
      "put": "alice",
      "clock": {"r1": 1, "r2": 0}
    },
    {
      "replica_id": "r2",
      "seq": 1,
      "key": "name",
      "delete": "delete",
      "clock": {"r1": 0, "r2": 1}
    }
  ]
}
```

约束：

- `replica_ids`：2～8 个非空且唯一的副本 ID。
- 每个事件属于其中某个副本；`seq` 从 1 开始，同一副本内不重复且必须连续
  （事件在请求中可乱序）。
- `put`（字符串，允许空串）与 `delete`（值必须是字符串 `"delete"`）二选一。
- `clock` 必须覆盖**全部**副本且无多余分量，分量为非负整数；
  本副本分量必须等于本事件 `seq`。
- 时钟任何分量都不得指向不存在的事件（0 表示尚不知道该副本的事件）。
- 本副本上一事件（`seq - 1`）以及其他副本分量所指的最新事件，其时钟必须
  逐分量 `<=` 当前时钟且至少一个分量严格更小。
- 任何违例整批返回 **HTTP 422**，错误详情中带 `event_index`（请求 `events`
  数组下标）指出具体事件。

## 可执行 HTTP 示例

### 1. 冲突：两个副本并发改同一个 key（值相同也冲突）

```bash
curl -sS -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }'
```

响应（结果按 key 排序；冲突事件按 `replica_id`、`seq` 排序）：

```json
{
  "results": [
    {
      "key": "name",
      "state": "conflict",
      "events": [
        {"replica_id": "r1", "seq": 1, "op": "put", "value": "alice",
         "clock": {"r1": 1, "r2": 0}},
        {"replica_id": "r2", "seq": 1, "op": "put", "value": "alice",
         "clock": {"r1": 0, "r2": 1}}
      ]
    }
  ]
}
```

### 2. 同步后再写入：put 生效、旧 delete 被遮蔽

```bash
curl -sS -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r1", "seq": 2, "key": "session", "delete": "delete",
       "clock": {"r1": 2, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}},
      {"replica_id": "r1", "seq": 3, "key": "name", "put": "alice2",
       "clock": {"r1": 3, "r2": 1}}
    ]
  }'
```

响应：

```json
{
  "results": [
    {
      "key": "name",
      "state": "put",
      "value": "alice2",
      "event": {"replica_id": "r1", "seq": 3, "op": "put", "value": "alice2",
                "clock": {"r1": 3, "r2": 1}}
    },
    {
      "key": "session",
      "state": "deleted",
      "event": {"replica_id": "r1", "seq": 2, "op": "delete", "value": null,
                "clock": {"r1": 2, "r2": 0}}
    }
  ]
}
```

### 3. 非法批次：时钟本副本分量不等于 seq → 422

```bash
curl -sS -i -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "k", "put": "v",
       "clock": {"r1": 2, "r2": 0}}
    ]
  }'
```

返回 `422 Unprocessable Entity`，body 例如：

```json
{
  "detail": [
    {
      "code": "clock_self_component",
      "message": "clock['r1'] = 2 must equal seq = 1",
      "event_index": 0
    }
  ]
}
```

## 冲突处理事件接口 `POST /api/resolve-conflict`

把确认过的冲突处理结果写成一条**可重放事件**。请求沿用裁决接口的
`replica_ids`、`events`（先按原规则整批校验），并额外携带：

- `key`：要消解的冲突 key（非空字符串）；
- `replica_id`：写入副本，必须属于 `replica_ids`；
- `versions`：当时看到的冲突版本列表，每项含 `replica_id`、`seq`；
  列表须无重复且**恰好等于**该 key 当前全部极大事件，且至少两项
  （列表顺序任意）；
- 处理结果二选一：`put`（字符串，允许空串）或 `delete`（值必须为
  字符串 `"delete"`）。

接口行为：

- 新事件的 `seq` 接续写入副本现有末尾（该副本此前无事件时为 `1`）；
- 新事件时钟逐分量取所有冲突版本时钟与写入副本此前历史的最大值，
  再把写入副本自身分量推进到新 `seq`——因此新事件严格支配全部冲突版本；
- 响应包含完整新事件 `event`（与事件请求线格式一致：`put` 或
  `delete` 二选一），以及追加后的该 key 裁决 `result`
  （形状与 `/api/resolve` 的单条结果一致）；
- 即使各冲突版本的值相同，冲突也必须经此接口显式消解；其他 key 的
  裁决不受影响；
- 新事件并入 `events` 后可直接通过原批次校验，并能由 `/api/catchup`
  按依赖安排补传。

状态码：

- **200**：冲突已按确认结果消解；
- **409**：`versions` 已过期/不完整（`conflict_versions_mismatch`），
  或该 key 当前已不冲突/批次中不存在（`key_not_conflicted`）；
  返回 409 时不生成事件；
- **422**：原批次校验失败，或写入副本不在 `replica_ids`
  （`unknown_replica_id`）、版本列表重复
  （`duplicate_conflict_version`）、原批次已满 120 条（`batch_full`）
  等结构性错误。

### 4. 消解并发冲突：追加支配两分支的 put

`r1/1` 与 `r2/1` 并发写入 `name`（两分支互不可比），确认采用 `r1` 写入
的 `"alice2"`；版本列表可以乱序：

```bash
curl -sS -X POST http://localhost:8000/api/resolve-conflict \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ],
    "key": "name",
    "replica_id": "r1",
    "versions": [
      {"replica_id": "r2", "seq": 1},
      {"replica_id": "r1", "seq": 1}
    ],
    "put": "alice2"
  }'
```

响应（新事件为 `r1/2`，时钟并入两个冲突分支）：

```json
{
  "event": {
    "replica_id": "r1",
    "seq": 2,
    "key": "name",
    "put": "alice2",
    "clock": {"r1": 2, "r2": 1}
  },
  "result": {
    "key": "name",
    "state": "put",
    "value": "alice2",
    "event": {"replica_id": "r1", "seq": 2, "op": "put", "value": "alice2",
              "clock": {"r1": 2, "r2": 1}}
  }
}
```

把返回的 `event` 追加到原批次后再请求 `/api/resolve`，`name` 即变为
普通 `put`；请求 `/api/catchup` 时它会排在所依赖的冲突版本之后补传。

### 5. 冲突已被新写入遮蔽（版本列表过期）→ 409

批次中 `r1/2` 因果上支配 `r2/1`，`name` 已不再冲突。仍按旧冲突提交时
返回 `409 Conflict`，且不生成事件：

```bash
curl -sS -i -X POST http://localhost:8000/api/resolve-conflict \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "other", "put": "x",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r1", "seq": 2, "key": "name", "put": "alice",
       "clock": {"r1": 2, "r2": 1}}
    ],
    "key": "name",
    "replica_id": "r1",
    "versions": [
      {"replica_id": "r1", "seq": 2},
      {"replica_id": "r2", "seq": 1}
    ],
    "put": "alice2"
  }'
```

```json
{
  "detail": [
    {
      "code": "key_not_conflicted",
      "message": "key 'name' is no longer in conflict"
    }
  ]
}
```

若 key 仍冲突但列表缺少某个极大版本、列入了非极大版本，或指向不存在的
`(replica_id, seq)`，返回 `409` 且错误码为 `conflict_versions_mismatch`。

## 增量补传接口 `POST /api/catchup`

请求沿用裁决接口的 `replica_ids`、`events`（按同样规则整批校验），额外携带
`known_clock`：目标对**每个**来源已连续接收的最高 seq。

```json
{
  "replica_ids": ["r1", "r2"],
  "known_clock": {"r1": 1, "r2": 0},
  "events": [ ... ]
}
```

`known_clock` 规则（违例返回 **422**）：

- 必须**恰好覆盖**所有 `replica_ids`，无缺失、无多余键；
- 每个分量为非负整数；
- 分量不得超过该来源在本批次中的最高 seq（批次中没有事件的来源只能填 `0`）；
- 若某个**已接收事件**（自身 `seq <= known_clock[本副本]`）的时钟任一分量
  超过对应的 `known_clock`，返回 `clock_exceeds_known_clock` 并附该事件的
  `event_index`。

缺失事件（`seq > known_clock[本副本]`）按**轮次**返回：

- 每一轮纳入当时所有依赖已满足的事件——其本副本上一事件（`seq - 1`）以及
  其他副本时钟分量所指事件，均已在目标端或在更早轮次中传输；
- 轮内事件按 `replica_id`、`seq` 排序；
- 事件保留原操作（`op`）、`value`（delete 时为 `null`）、`key` 和 `clock`；
- `progress` 为补传完成后目标对各来源的进度（即批次中各来源最高 seq）；
- 全部事件均已接收时 `rounds` 为空数组；
- 并发写入即使值相同也分别补传，不做去重或冲突合并。

### 6. 分轮补传：跨副本依赖逐轮解锁

目标已接收 `r1/1`（时钟 `{r1:1, r2:0}`），缺失 `r2/1`（依赖 `r1/1`）与
`r1/2`（依赖 `r2/1`），因此分为两轮：

```bash
curl -sS -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 1, "r2": 0},
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}},
      {"replica_id": "r1", "seq": 2, "key": "name", "put": "alice2",
       "clock": {"r1": 2, "r2": 1}}
    ]
  }'
```

响应（第 1 轮先补 `r2/1`，第 2 轮再补依赖它的 `r1/2`）：

```json
{
  "rounds": [
    [
      {"replica_id": "r2", "seq": 1, "key": "name", "op": "put",
       "value": "alice", "clock": {"r1": 1, "r2": 1}}
    ],
    [
      {"replica_id": "r1", "seq": 2, "key": "name", "op": "put",
       "value": "alice2", "clock": {"r1": 2, "r2": 1}}
    ]
  ],
  "progress": {"r1": 2, "r2": 1}
}
```

### 7. 并发同值写入也分别补传

```bash
curl -sS -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 0, "r2": 0},
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }'
```

两个事件互不依赖，进入同一轮（按 `replica_id` 排序），即使值相同也都补传：

```json
{
  "rounds": [
    [
      {"replica_id": "r1", "seq": 1, "key": "name", "op": "put",
       "value": "alice", "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "op": "put",
       "value": "alice", "clock": {"r1": 0, "r2": 1}}
    ]
  ],
  "progress": {"r1": 1, "r2": 1}
}
```

### 8. 全部已接收：轮次为空

```bash
curl -sS -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 1, "r2": 1},
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}}
    ]
  }'
```

```json
{
  "rounds": [],
  "progress": {"r1": 1, "r2": 1}
}
```

### 9. 非法请求：已接收事件的时钟超出 known_clock → 422

目标声称只连续收到 `r1/1` 且不知道 `r2` 的任何事件，但该事件时钟里
`r2 = 1`，与目标认知矛盾：

```bash
curl -sS -i -X POST http://localhost:8000/api/catchup \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "known_clock": {"r1": 1, "r2": 0},
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}}
    ]
  }'
```

返回 `422 Unprocessable Entity`，body：

```json
{
  "detail": [
    {
      "code": "clock_exceeds_known_clock",
      "message": "event already received (replica 'r1', seq 1 <= known_clock 1) but its clock {'r1': 1, 'r2': 1} exceeds known_clock; components beyond target knowledge: {'r2': 1}",
      "event_index": 0
    }
  ]
}
```

其余 `known_clock` 违例的错误码：`known_clock_coverage`（缺失/多余分量）、
`known_clock_ahead_of_batch`（分量超过批次最高 seq）。

## 持久化历史接口 `/api/histories`

让多个调用方在**同一份持久历史**上汇入离线事件、提交冲突处理。数据库为本地
SQLite 文件，路径由环境变量 `HISTORY_DB_PATH` 决定；`docker compose` 启动时
固定为容器内 `/data/history.db` 并挂载命名卷 `history-data`，因此
`docker compose restart` / 容器重建后历史仍可读取。原三个无状态接口与
`docker compose up --build -d` 启动方式保持不变。

接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/histories` | 用合法的 `replica_ids` + `events` 创建历史，返回 `{"id": ...}`（201） |
| `GET` | `/api/histories/{id}` | 读取完整原格式批次（事件按 `replica_id`、`seq` 排序）及原裁决，二者来自同一已提交状态 |
| `POST` | `/api/histories/{id}/events` | 按 ID 汇入一批事件（请求体只有 `events`） |
| `POST` | `/api/histories/{id}/resolve-conflict` | 按 ID 提交原冲突处理参数（`key`、`replica_id`、`versions`、`put`/`delete`），不再传整个批次 |
| `GET` | `/api/histories/{id}/withdraw-preview?replica_id=...&seq=...` | **只读**预演撤除一条事件的波及范围（撤除集合、剩余事件、前后裁决），不改动历史 |

汇入规则：

- 按 `(replica_id, seq)` 合并：同标识且内容完全相同的事件忽略（可重复发送，
  `clock` 键的顺序不影响相等）；同标识但 `key`、操作、值或**完整 `clock`**
  任一不同，返回 **409**（`event_content_mismatch`，错误中带汇入批次的
  `event_index`）——`clock` 缺少分量、多出分量（即使多出的分量为 `0`）、或
  任一分量值不同，都算异内容；
- 合并后的整份历史重新执行原校验（因果、连续性、clock 覆盖等），失败
  **422**；因此**未重复的新事件**若 `clock` 覆盖不合法（缺/多分量等），
  仍返回 **422** 而非 409；事件总数上限仍为 **120**（超出为 `batch_full`，422）；
- 每次写入**全部生效或全部不生效**（单事务），失败时历史保持原样。

冲突提交规则：

- 服务端在写锁内重新读取当前历史，`versions` 必须**恰好等于**该 key 当前全部
  极大事件；过期/不完整/key 已不冲突 → **409**（与无状态接口同一套错误码），
  且不生成事件；
- 新事件 `seq` 接续写入副本现有末尾（此前无事件时为 1），时钟并入全部冲突
  版本与写入副本历史，随后随整份历史一起原子写入；
- 并发提交同一冲突时只有一个能成功，其余拿到 409，不会重复占用序号。

不存在的 ID 在四个按 ID 访问的接口上都返回 **404**（`history_not_found`）；
撤除预演在历史存在但目标事件不存在（含 `replica_id` 不属于该历史）时另返回
`event_not_found`，同样是 **404**。

### 10. 创建历史并获得 ID

```bash
curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }'
```

```json
{ "id": "f82cdf528c99461c9a6becef9edddd1f" }
```

创建请求的约束与 `/api/resolve` 完全一致；非法批次返回 **422**，不会生成
历史。

### 11. 按 ID 读取：完整原格式批次 + 原裁决（同一已提交状态）

```bash
curl -sS http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f
```

事件在响应中按 `replica_id`、`seq` 排序（与创建时的提交顺序无关）：

```json
{
  "id": "f82cdf528c99461c9a6becef9edddd1f",
  "replica_ids": ["r1", "r2"],
  "events": [
    {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
     "clock": {"r1": 1, "r2": 0}},
    {"replica_id": "r2", "seq": 1, "key": "name", "put": "bob",
     "clock": {"r1": 0, "r2": 1}}
  ],
  "results": [
    {
      "key": "name",
      "state": "conflict",
      "events": [
        {"replica_id": "r1", "seq": 1, "op": "put", "value": "alice",
         "clock": {"r1": 1, "r2": 0}},
        {"replica_id": "r2", "seq": 1, "op": "put", "value": "bob",
         "clock": {"r1": 0, "r2": 1}}
      ]
    }
  ]
}
```

### 12. 汇入离线事件：重复忽略、新事件合并

请求体只需要 `events`（不再传 `replica_ids` 或整个批次）。下面把已有的
`r1/1`（内容相同，忽略）与新事件 `r2/2` 一起汇入：

```bash
curl -sS -X POST \
  http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f/events \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r2", "seq": 2, "key": "other", "put": "x",
       "clock": {"r1": 0, "r2": 2}}
    ]
  }'
```

响应与 `GET` 形状一致（`id`、`replica_ids`、排序后的完整 `events`、
`results`）。同标识异内容返回 **409**，且整批（含批次内其他合法事件）都不
写入：

```json
{
  "detail": [
    {
      "code": "event_content_mismatch",
      "message": "event with replica_id 'r1', seq 1 already exists in this history with different content; ...",
      "event_index": 0
    }
  ]
}
```

合并后违反因果/连续性等原校验，或超出 120 条上限时返回 **422**。

### 13. 汇入重复事件：完整 clock 逐字相等才忽略，否则 409 且整批不写入

判等比较的是事件的**完整内容**（`key`、操作、值与完整 `clock`）。先看一次
完全相同的重发——仅 `clock` 键的书写顺序不同仍然相等，事件被忽略：

```bash
curl -sS -X POST \
  http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f/events \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r2": 0, "r1": 1}}
    ]
  }'
```

返回 200，历史事件数不变。但同一 `(replica_id, seq)` 的重发只要 `clock`
多出一个分量——**即使多出的分量值为 `0`**——就算异内容。下例把异内容的
`r1/1` 与一条同批合法新事件 `r1/2` 一起汇入：

```bash
curl -sS -i -X POST \
  http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f/events \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0, "r3": 0}},
      {"replica_id": "r1", "seq": 2, "key": "other", "put": "x",
       "clock": {"r1": 2, "r2": 1}}
    ]
  }'
```

返回 `409 Conflict`，整批（包括同批合法的 `r1/2`）都不写入：

```json
{
  "detail": [
    {
      "code": "event_content_mismatch",
      "message": "event with replica_id 'r1', seq 1 already exists in this history with different content; stored {'replica_id': 'r1', 'seq': 1, 'key': 'name', 'put': 'alice', 'clock': {'r1': 1, 'r2': 0}}, incoming {'replica_id': 'r1', 'seq': 1, 'key': 'name', 'put': 'alice', 'clock': {'r1': 1, 'r2': 0, 'r3': 0}}",
      "event_index": 0
    }
  ]
}
```

同样地，重发事件的 `clock` **缺少**某个分量、或某个分量值与已存事件不同
（例如已存为 `{"r1": 0, "r2": 1}`，重发为 `{"r1": 1, "r2": 1}`），也返回
同样的 **409** 与 `event_index`，不会因缺键而报 500。

注意：覆盖校验只针对**未重复的新事件**。若新身份的事件自身 `clock` 覆盖
不合法（缺少或多出副本分量），仍由合并后的整批校验返回 **422**
（`clock_replica_coverage`，带该事件的 `event_index`），而不是 409。

### 14. 按 ID 提交冲突处理：核对当前 versions、接续序号

请求体就是原冲突处理参数（与 `/api/resolve-conflict` 相比少了
`replica_ids`、`events`）：

```bash
curl -sS -X POST \
  http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f/resolve-conflict \
  -H 'Content-Type: application/json' \
  -d '{
    "key": "name",
    "replica_id": "r1",
    "versions": [
      {"replica_id": "r2", "seq": 1},
      {"replica_id": "r1", "seq": 1}
    ],
    "put": "chosen"
  }'
```

```json
{
  "id": "f82cdf528c99461c9a6becef9edddd1f",
  "event": {
    "replica_id": "r1",
    "seq": 2,
    "key": "name",
    "put": "chosen",
    "clock": {"r1": 2, "r2": 1}
  },
  "result": {
    "key": "name",
    "state": "put",
    "value": "chosen",
    "event": {"replica_id": "r1", "seq": 2, "op": "put", "value": "chosen",
              "clock": {"r1": 2, "r2": 1}}
  }
}
```

生成的事件已持久化到历史中（`GET` 即可看到，事件按 `replica_id`、`seq`
排序）。再用旧的 `versions` 提交会返回 **409**（`key_not_conflicted` 或
`conflict_versions_mismatch`），不生成事件、不占用 `r2/2` 序号。

### 15. 重启容器后历史仍在

数据在命名卷 `history-data` 中，重启/重建容器（不改卷）后按原 ID 仍可读取：

```bash
docker compose restart
curl -sS http://localhost:8000/api/histories/f82cdf528c99461c9a6becef9edddd1f
```

如需连数据一起清除：`docker compose down -v`。

## 撤除预演接口 `GET /api/histories/{id}/withdraw-preview`

在**已提交的持久历史**上预演：如果撤除某一条离线事件，会连带波及哪些事件与
key。接口为**只读**：不删除任何事件、不修改、不重排序号或时钟，前后结果取自
同一次读取的同一已提交历史；旧接口与库中数据均不受影响。

查询参数：

- `replica_id`：目标事件所属副本（非空字符串）；
- `seq`：目标事件在该副本的序号（正整数）。

波及范围按与批次校验相同的**直接依赖边**反向闭包计算。一条事件的直接依赖是：

- 本副本的上一事件（`seq - 1`，存在时）；
- 时钟中**其他副本**的非零分量所指向的事件（分量为 `0` 表示不依赖该副本）。

撤除集合 = 目标事件 + 所有直接或间接依赖它的事件，**跨 key 也计入**；
与其无因果关系的**并发事件一律保留**（即使写的是同一个 key）。保留事件的
`seq`、`clock` 原样不变，不做重编。

响应：

- `id`：历史 ID；
- `withdrawn`：将被撤除事件的 `replica_id`、`seq`，按 `replica_id`、`seq`
  排序（目标本身包含在内）；
- `events`：剩余事件的**原线格式**（`put`/`delete` 二选一、原始 `clock`），
  顺序仍按 `replica_id`、`seq`；
- `changes`：裁决发生变化的 key，按 key 排序，每项含完整的前后裁决结构
  `before`/`after`（形状与 `/api/resolve` 的单条结果一致，按完整结构判变）；
  某 key 没有剩余事件时 `after` 为 `null`（表示该 key 无事件，**不等同于
  `deleted`**）；裁决结构不变的 key 不出现在 `changes` 中。

即使所有事件都被撤除，也返回 **200**，此时 `events` 为空数组。

状态码：

- **200**：预演完成（历史或 key 被完全撤空同样是 200）；
- **404**：历史不存在（`history_not_found`），或历史存在但没有该
  `replica_id`/`seq` 的事件（`event_not_found`，包括该 `replica_id` 不属于
  本历史的副本集合）；
- **422**：参数非法，如缺少/非数字/非正整数的 `seq`、空的 `replica_id`。

### 16. 创建历史拿到 ID，预演撤除，再读取确认历史未改动

先创建一份含三个副本、因果成链的历史：`r1/1` 写 `a`；`r2/1` 同步过 `r1/1`
后写 `b`（时钟 `r1 = 1`）；`r1/2` 依赖本副本上一事件和 `r2/1`，删除 `a`；
`r3/1` 完全离线并发写 `c`（各分量为 0），与前三者无因果关系。

```bash
curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2", "r3"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "a", "put": "x",
       "clock": {"r1": 1, "r2": 0, "r3": 0}},
      {"replica_id": "r2", "seq": 1, "key": "b", "put": "y",
       "clock": {"r1": 1, "r2": 1, "r3": 0}},
      {"replica_id": "r1", "seq": 2, "key": "a", "delete": "delete",
       "clock": {"r1": 2, "r2": 1, "r3": 0}},
      {"replica_id": "r3", "seq": 1, "key": "c", "put": "z",
       "clock": {"r1": 0, "r2": 0, "r3": 1}}
    ]
  }'
```

```json
{ "id": "0df8277d554f49629f627be82ea06322" }
```

用返回的 ID 预演撤除 `r1/1`（以下示例需要 `python3`；也可直接把上一步的 ID
手工填入 URL）：

```bash
HID=0df8277d554f49629f627be82ea06322
curl -sS -G "http://localhost:8000/api/histories/$HID/withdraw-preview" \
  --data-urlencode 'replica_id=r1' \
  --data-urlencode 'seq=1'
```

`r1/1`、依赖它的 `r2/1`、以及再依赖二者的 `r1/2` 全部计入撤除（跨 `a`、`b`
两个 key）；无关并发的 `r3/1` 原样保留。`a`、`b` 无剩余事件，`after` 为
`null`；`c` 的裁决不变，故不出现：

```json
{
  "id": "0df8277d554f49629f627be82ea06322",
  "withdrawn": [
    {"replica_id": "r1", "seq": 1},
    {"replica_id": "r1", "seq": 2},
    {"replica_id": "r2", "seq": 1}
  ],
  "events": [
    {"replica_id": "r3", "seq": 1, "key": "c", "put": "z",
     "clock": {"r1": 0, "r2": 0, "r3": 1}}
  ],
  "changes": [
    {
      "key": "a",
      "before": {
        "key": "a", "state": "deleted",
        "event": {"replica_id": "r1", "seq": 2, "op": "delete", "value": null,
                  "clock": {"r1": 2, "r2": 1, "r3": 0}}
      },
      "after": null
    },
    {
      "key": "b",
      "before": {
        "key": "b", "state": "put", "value": "y",
        "event": {"replica_id": "r2", "seq": 1, "op": "put", "value": "y",
                  "clock": {"r1": 1, "r2": 1, "r3": 0}}
      },
      "after": null
    }
  ]
}
```

预演不落任何写入——再读一次历史，4 个事件、裁决均与预演前完全一致：

```bash
curl -sS "http://localhost:8000/api/histories/$HID"
```

```json
{
  "id": "0df8277d554f49629f627be82ea06322",
  "replica_ids": ["r1", "r2", "r3"],
  "events": [
    {"replica_id": "r1", "seq": 1, "key": "a", "put": "x",
     "clock": {"r1": 1, "r2": 0, "r3": 0}},
    {"replica_id": "r1", "seq": 2, "key": "a", "delete": "delete",
     "clock": {"r1": 2, "r2": 1, "r3": 0}},
    {"replica_id": "r2", "seq": 1, "key": "b", "put": "y",
     "clock": {"r1": 1, "r2": 1, "r3": 0}},
    {"replica_id": "r3", "seq": 1, "key": "c", "put": "z",
     "clock": {"r1": 0, "r2": 0, "r3": 1}}
  ],
  "results": [ "……与创建时相同，未被修改……" ]
}
```

并发分支的对照：同一历史预演撤除 `r3/1` 只会撤掉它自己，`a`、`b` 不受影响；
若某 key 的两个极大事件互不可比（冲突），撤除其中一支只会让另一支成为唯一
裁决，另一支**不会**进入 `withdrawn`。错误示例：

```bash
# 历史不存在 -> 404 history_not_found
curl -sS -i -G "http://localhost:8000/api/histories/00000000000000000000000000000000/withdraw-preview" \
  --data-urlencode 'replica_id=r1' --data-urlencode 'seq=1'

# 事件不存在（r1 没有 seq 9）-> 404 event_not_found
curl -sS -i -G "http://localhost:8000/api/histories/$HID/withdraw-preview" \
  --data-urlencode 'replica_id=r1' --data-urlencode 'seq=9'

# seq 非正整数 -> 422
curl -sS -i -G "http://localhost:8000/api/histories/$HID/withdraw-preview" \
  --data-urlencode 'replica_id=r1' --data-urlencode 'seq=0'
```

## 导入会话接口 `/api/histories/{id}/import-sessions`

离线事件**分几次到达**时，不必每批都直接汇入历史：先按历史 ID 开一个**导入
会话**，把各批事件暂存进去，收齐后一次提交。会话与历史一样持久化在 SQLite
中，进程/容器重启后可继续暂存与提交；暂存中的事件**不影响**历史的读取、裁决
与撤除预演，只有提交成功才会改变已提交历史。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/histories/{id}/import-sessions` | 在该历史上创建空会话，返回 `{"id": ...}`（201） |
| `GET` | `/api/histories/{id}/import-sessions/{sid}` | 查询会话状态（`open`/`closed`）与暂存事件（原格式，按 `replica_id`、`seq` 排序） |
| `POST` | `/api/histories/{id}/import-sessions/{sid}/events` | 暂存一批事件（请求体只有 `events`；同标识重传按完整内容判等去重，原始条数可超过 120），返回更新后的会话状态 |
| `POST` | `/api/histories/{id}/import-sessions/{sid}/commit` | 把暂存事件一次汇入历史并封闭会话，返回与 `GET /api/histories/{id}` 相同的结构 |
| `GET` | `/api/histories/{id}/import-sessions/{sid}/missing-ranges` | **只读**查询仍需从来源补取的 `(replica_id, seq)` 缺口，合成连续闭区间返回；不提交、不改任何数据，封闭会话也可查 |

暂存规则（每次追加**全部生效或全部不生效**，失败不改暂存）：

- **新标识**事件（本批第一次出现、且尚未暂存的 `(replica_id, seq)`）只做
  **单事件**校验：字段合法（`put`/`delete` 二选一等）、副本属于该历史、
  `clock` 恰好覆盖全部副本、本副本分量等于 `seq`；违例 **422**；
- **允许前序事件或跨副本依赖尚未到达**（连续性、因果、依赖存在性都留到提交时
  检查），事件可按任意顺序、任意批次追加；
- **重传**事件（标识已暂存，或本批更早出现过）**不做**上面的单事件校验，而是
  按 `(replica_id, seq)` 与先到的那份做**完整内容**判等：比较 `key`、操作
  （put/delete）、值与**完整 `clock`**（键序无关）。内容完全相同的重复追加被
  忽略；`key`/操作/值不同，或 `clock` **缺少分量、多出分量（即使多出的分量为
  `0`）、任一分量值不同（含本副本分量不等于 `seq`）**，都算异内容，返回
  **409**（`event_content_mismatch`，带追加批次的 `event_index`）而**非 422**；
  因此重传一个缺/多分量的时钟不会再被误判为时钟覆盖错误；
- 每个会话至多暂存 **120 个不同标识**，容量只计**已暂存标识**加上**本批去重后
  的新标识**：121 份完全相同的合法事件只暂存一条；已满 120 条的会话再重传已有
  事件（哪怕一批超过 120 条原始数据）仍然成功；再加入任何新标识才超出容量，
  返回 **422**（`batch_full`）。注意：每次追加请求本身仍要求 `events` 至少
  1 条，但**没有**旧汇入接口那种单批 120 条的上限（上限按去重后的标识计）。

提交规则：

- 在写锁内读取历史**最新已提交**状态，把暂存事件按原汇入规则合并：与已提交
  事件同标识同内容的忽略，异内容 **409**；合并后重新执行原整批校验（连续性、
  因果、依赖存在性、clock 覆盖等）与 **120 条总上限**，违例 **422**；
- **失败时开放会话与已暂存数据原样保留**，补齐缺失事件后可对同一会话重试；
- 成功时汇入与封闭**在同一事务中生效**（与旧写接口并发也不会丢事件），响应
  就是历史的读取结构（`id`、`replica_ids`、排序后的完整 `events`、`results`）；
- 封闭后的会话再追加或再提交返回 **409**（`session_closed`），会话本身仍可
  查询；
- 历史不存在（`history_not_found`）或会话不存在（`session_not_found`，含会话
  属于其他历史）都返回 **404**。

### 17. 乱序分批暂存，收齐后一次汇入

历史上已提交 `r1/1`（`name = alice`）。`r1/3` 比它的前序 `r1/2` 先到达——
直接汇入历史会被拒（缺 `r1/2`），但可以先暂存：

```bash
# 创建历史（已有 r1/1）
HID=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

# 开导入会话
SID=$(curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

# 第一批：r1/3 先到（它的前序 r1/2 还没到）——暂存接受
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 3, "key": "name", "put": "alice3",
       "clock": {"r1": 3, "r2": 0}}
    ]
  }'

# 第二批：前序 r1/2 到达
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 2, "key": "name", "put": "alice2",
       "clock": {"r1": 2, "r2": 0}}
    ]
  }'
```

每次追加都返回更新后的会话状态；也可以随时查询（暂存事件按 `replica_id`、
`seq` 排序，与到达顺序无关）：

```bash
curl -sS "http://localhost:8000/api/histories/$HID/import-sessions/$SID"
```

```json
{
  "id": "9c1f…",
  "history_id": "f82c…",
  "status": "open",
  "events": [
    {"replica_id": "r1", "seq": 2, "key": "name", "put": "alice2",
     "clock": {"r1": 2, "r2": 0}},
    {"replica_id": "r1", "seq": 3, "key": "name", "put": "alice3",
     "clock": {"r1": 3, "r2": 0}}
  ]
}
```

收齐后一次提交，响应就是历史的读取结构，会话随之封闭：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/commit"
```

```json
{
  "id": "f82c…",
  "replica_ids": ["r1", "r2"],
  "events": [
    {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
     "clock": {"r1": 1, "r2": 0}},
    {"replica_id": "r1", "seq": 2, "key": "name", "put": "alice2",
     "clock": {"r1": 2, "r2": 0}},
    {"replica_id": "r1", "seq": 3, "key": "name", "put": "alice3",
     "clock": {"r1": 3, "r2": 0}}
  ],
  "results": [
    {
      "key": "name",
      "state": "put",
      "value": "alice3",
      "event": {"replica_id": "r1", "seq": 3, "op": "put", "value": "alice3",
                "clock": {"r1": 3, "r2": 0}}
    }
  ]
}
```

### 18. 缺依赖提交失败（422），补齐后对同一会话重试

暂存只校验单事件，依赖是否齐全要到**提交**时才检查。下面暂存的 `r2/1` 时钟
指向 `r1 = 4`，但 `r1/4` 既不在历史里也不在会话中：

```bash
SID2=$(curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID2/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "other", "put": "x",
       "clock": {"r1": 4, "r2": 1}}
    ]
  }'

# 提交：依赖的 r1/4 不存在 -> 422
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID2/commit"
```

返回 `422 Unprocessable Entity`，且**开放会话与暂存数据原样保留**：

```json
{
  "detail": [
    {
      "code": "clock_points_to_missing_event",
      "message": "clock['r1'] = 4, but replica 'r1' has no event with seq 4",
      "event_index": 3
    }
  ]
}
```

（`event_index` 是合并后整批——已提交事件加暂存事件——中的下标。）

把缺失的 `r1/4` 补进**同一会话**，重新提交即成功（`r2/1` 依赖的 `r1/4` 随
同一批汇入）：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID2/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 4, "key": "name", "put": "alice4",
       "clock": {"r1": 4, "r2": 0}}
    ]
  }'

# 重试提交 -> 200，会话封闭
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID2/commit"

# 封闭后再追加或再提交 -> 409
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID2/commit"
```

```json
{
  "detail": [
    {
      "code": "session_closed",
      "message": "import session '…' is already closed; it accepts no further appends or commits"
    }
  ]
}
```

暂存期间历史的读取与裁决完全不受会话影响；只有提交成功后，
`GET /api/histories/{id}` 才会包含汇入的事件。会话与历史一样落在 SQLite
中，`docker compose restart` 后开放会话可继续暂存与提交。

### 19. 暂存重传：按完整内容判等，缺/多零分量或本分量改变是 409；121 份相同只存一条

暂存对**已暂存及同批重复的标识**只做完整内容判等，不再套用时钟覆盖等单事件
校验。先准备一份历史（已提交 `r1/1`）并开会话、暂存一条合法的 `r1/2`：

```bash
HID=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "k", "put": "a",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
SID=$(curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 2, "key": "k", "put": "b",
       "clock": {"r1": 2, "r2": 1}}
    ]
  }'
```

**完全相同的重传被忽略**（仅 `clock` 键的书写顺序不同也算相同），暂存条数
不变：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 2, "key": "k", "put": "b",
       "clock": {"r2": 1, "r1": 2}}
    ]
  }'
```

**同一标识的重传只要 `clock` 多出一个零分量、缺少分量，或本副本分量被改写，
都算异内容 → 409（而不是时钟覆盖/本分量的 422）**：

```bash
# 多出零分量 r3=0
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 2, "key": "k", "put": "b",
       "clock": {"r1": 2, "r2": 1, "r3": 0}}
    ]
  }'

# 缺少 r2 分量
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"replica_id": "r1", "seq": 2, "key": "k", "put": "b",
     "clock": {"r1": 2}}
  ]}'

# 本副本分量被改成 3（!= seq 2）
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"replica_id": "r1", "seq": 2, "key": "k", "put": "b",
     "clock": {"r1": 3, "r2": 1}}
  ]}'
```

三次都返回 `409 Conflict`，错误形状相同，并指出是追加批次的第 0 条：

```json
{
  "detail": [
    {
      "code": "event_content_mismatch",
      "message": "event with replica_id 'r1', seq 2 is already present in this session with different content; prior {...}, incoming {...}",
      "event_index": 0
    }
  ]
}
```

对比：**新标识**事件的时钟缺/多分量仍是 **422**（`clock_replica_coverage`），
因为单事件校验只对新标识生效：

```bash
curl -sS -i -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"replica_id": "r1", "seq": 3, "key": "k", "put": "c",
     "clock": {"r1": 3}}
  ]}'
# 422 Unprocessable Entity，code=clock_replica_coverage
```

**121 份完全相同的合法新事件只暂存一条**（暂存容量按去重后的不同标识计数，
追加请求本身没有单批 120 条上限；下面用 `python3` 生成 121 份的请求体）：

```bash
python3 - <<'EOF' > /tmp/dup121.json
import json
one = {"replica_id": "r2", "seq": 1, "key": "q", "put": "z",
       "clock": {"r1": 1, "r2": 1}}
print(json.dumps({"events": [one] * 121}))
EOF

curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' -d @/tmp/dup121.json
```

返回 200，会话中只有此前暂存的 `r1/2` 与这一条 `r2/1`，共 **2** 条（不是 122
条）。同理，已经存满 120 个不同标识的会话，再重传这 120 条（哪怕一批 121 份）
依然返回 200；只有再出现一个**新标识**才返回 **422** `batch_full`。任一追加
失败时，本次批次（连同其中合法的其他事件）全部不落，已暂存事件、会话状态与
历史都保持原样。

## 导入缺口查询接口 `GET /api/histories/{id}/import-sessions/{sid}/missing-ranges`

分批暂存时，可以**只读**地查询：相对来源，这个会话还缺哪些 `(replica_id, seq)`
事件需要补取。接口**不提交会话**、不封闭会话，也不改动历史、暂存与状态；开放
与**已封闭**的会话都可以查询。

计算在**同一已提交快照**上进行：

1. 先把历史的已提交事件与会话的暂存事件按 `(replica_id, seq)` 合并，逐份取
   同一已提交状态。同一标识在历史和暂存中都存在时，按**原完整内容**
   （`key`、操作、值与完整 `clock`，键序无关）判等；内容相同视为同一条，
   内容不同则返回 **409**（`event_content_mismatch`），一次性列出**全部**冲突
   的 `replica_id`、`seq`（按二者排序），此时**不返回缺口清单**。
2. 没有冲突时，对每个副本计算需求**上界** = 合并事件中该副本自身 `seq` 的
   最大值，与所有合并事件 `clock` 对该副本分量的最大值，两者取大。
3. 把闭区间 `1..上界` 中**已有标识之外**的序号合成为**最大的连续区间**
   （`start_seq`、`end_seq` 均为闭区间端点），按 `replica_id`、`start_seq`
   排序后放在 `missing_ranges` 中返回。

缺口序号**绝不逐个展开**：即使暂存事件的 `seq`/clock 分量为 `10^12`，响应也
只有一个区间。没有任何事件引用、自身也没有事件的副本上界为 `0`，不产生区间。
**无缺口时返回 200 与空列表——这只表示标识齐全，不保证随后提交通过**（时钟
因果、连续性、120 条上限等仍在提交时检查）。

状态码：

- **200**：返回 `{"missing_ranges": [...]}`（无缺口时为空数组）；
- **409**：暂存与已提交事件存在同标识异内容，`detail` 列出全部冲突标识，
  不含缺口清单；
- **404**：历史不存在（`history_not_found`）或会话不存在
  （`session_not_found`，含会话属于其他历史）。

### 20. 查缺口 → 补齐 → 再查为空 → 提交

历史上已提交 `r1/1`（clock `{r1:1, r2:0}`）。开一个导入会话，先暂存乱序到达的
`r1/3` 与 `r2/1`，其中 `r2/1` 的时钟声称已知道 `r1` 到 `4`：

```bash
# 创建历史（已提交 r1/1）
HID=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "k", "put": "a",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

# 开导入会话
SID=$(curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

# 暂存 r1/3 与 r2/1（r2/1 的 clock 引用了 r1/4）
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 3, "key": "k", "put": "c",
       "clock": {"r1": 3, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "k", "put": "b",
       "clock": {"r1": 4, "r2": 1}}
    ]
  }' > /dev/null

# 第一次查缺口
curl -sS "http://localhost:8000/api/histories/$HID/import-sessions/$SID/missing-ranges"
```

`r1` 的上界取自身最大 seq `3` 与 clock 分量最大值 `4` 的较大者 `4`；`1..4` 中
已有 `1`、`3`，故缺 `2` 与 `4`，合成两个闭区间；`r2` 上界为 `1` 且已有 `1`，
无缺口。区间按 `replica_id`、`start_seq` 排序：

```json
{
  "missing_ranges": [
    {"replica_id": "r1", "start_seq": 2, "end_seq": 2},
    {"replica_id": "r1", "start_seq": 4, "end_seq": 4}
  ]
}
```

把缺的 `r1/2`、`r1/4` 补进**同一会话**后再查，得到空列表（仅表示标识齐全）：

```bash
curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {"replica_id": "r1", "seq": 2, "key": "k", "put": "b2",
       "clock": {"r1": 2, "r2": 0}},
      {"replica_id": "r1", "seq": 4, "key": "k", "put": "d",
       "clock": {"r1": 4, "r2": 1}}
    ]
  }' > /dev/null

curl -sS "http://localhost:8000/api/histories/$HID/import-sessions/$SID/missing-ranges"
```

```json
{"missing_ranges": []}
```

整个过程中查询不产生任何写入（可随时再 `GET` 历史或会话核对）。标识齐全后再
正常提交；空列表本身不保证提交成功——若补齐事件的时钟违反因果，提交仍会返回
**422**，开放会话与暂存原样保留。

### 21. 大序号不展开：`10^12` 也只是一个区间

缺口只以区间返回，响应大小与缺口长度无关。暂存一条引用 `r1 = 1000000000000`
的事件，缺口仍是单个闭区间 `[2, 1000000000000]`，而不是一长串序号：

```bash
HID=$(curl -sS -X POST http://localhost:8000/api/histories \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "k", "put": "a",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
SID=$(curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

curl -sS -X POST "http://localhost:8000/api/histories/$HID/import-sessions/$SID/events" \
  -H 'Content-Type: application/json' \
  -d '{"events": [
    {"replica_id": "r2", "seq": 1, "key": "k", "put": "b",
     "clock": {"r1": 1000000000000, "r2": 1}}
  ]}' > /dev/null

curl -sS "http://localhost:8000/api/histories/$HID/import-sessions/$SID/missing-ranges"
```

```json
{
  "missing_ranges": [
    {"replica_id": "r1", "start_seq": 2, "end_seq": 1000000000000}
  ]
}
```

### 22. 同标识异内容：409 列出全部冲突，不返回清单

暂存了一条与已提交事件同 `(replica_id, seq)` 但值不同的事件时，缺口查询返回
**409**；若同时存在多处冲突（甚至伴随其他合法暂存事件），`detail` 会按
`replica_id`、`seq` 排序列出**全部**冲突标识，且响应中没有 `missing_ranges`：

```bash
curl -sS -i "http://localhost:8000/api/histories/$HID/import-sessions/$SID/missing-ranges"
```

```json
{
  "detail": [
    {
      "code": "event_content_mismatch",
      "message": "staged event with replica_id 'r1', seq 1 differs in content from the event with the same identity already committed in this history; resolve the conflicting retransmission before the session can be completed",
      "replica_id": "r1",
      "seq": 1
    }
  ]
}
```

封闭后的会话仍可查询其缺口；历史或会话不存在时分别返回 **404**
`history_not_found` / `session_not_found`（会话属于其他历史同样按不存在处理）。

## 本地开发（不使用 Docker）

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

本地运行时持久化历史默认写入当前目录的 `history.db`（可用环境变量
`HISTORY_DB_PATH` 指定其他路径，例如 `HISTORY_DB_PATH=/tmp/history.db`）；
三个无状态接口不依赖数据库。
