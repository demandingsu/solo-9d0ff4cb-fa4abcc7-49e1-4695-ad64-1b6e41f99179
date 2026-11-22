# 病害数据入仓服务(养护中心)

用**同一张字典**汇总各区裂缝、坑槽等路段病害数据的入仓网页。各区按批上传
"路段病害明细 + 本批代码对照表"两份 CSV,服务把明细里的本批代码替换为全仓统一
的**永久整数码**;下游按批下载编码明细和字典增补。

## 快速启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

数据持久化在命名卷 `warehouse-data`(容器内 `/data`)。

本地开发(不用 Docker):

```bash
python3 -m venv .venv && .venv/bin/pip install -r app/requirements.txt
cd app && DATA_DIR=./data ../.venv/bin/gunicorn --bind 127.0.0.1:8000 --workers 1 --threads 8 main:app
```

## CSV 格式(UTF-8,带表头)

**本批代码对照表**(列名可用环境变量覆盖):

| local_code | disease_name |
|---|---|
| A1 | 裂缝 |
| A2 | 坑槽 |

**路段病害明细**:列任意,但必须含代码列(默认 `local_code`):

| segment | station | local_code | area |
|---|---|---|---|
| G104-01 | K12+300 | A1 | 3.5 |

**编码明细输出**:与明细同构,代码列改名为 `code`,值为全仓统一整数码。

**字典增补**:`code,disease_name,version` 三列。

## 使用与接口

网页表单上传(批次号 + 两份 CSV),批次列表自动刷新,每批提供两个下载链接。

| 方法/路径 | 说明 |
|---|---|
| `POST /batches` | 上传入仓(multipart:`batch_no`、`details`、`mapping`)。`202` 受理;`200` 同号同内容幂等沿用原发布;`409` 同号不同内容冲突 |
| `GET /batches/<批次号>/details.csv` | 该批编码明细(流式下载) |
| `GET /batches/<批次号>/dict_delta.csv?since=<版本>` | 字典增补:版本区间 `(since, 该批发布时版本]` |
| `GET /api/batches` / `GET /api/batches/<批次号>` | 批次状态 JSON |
| `GET /dictionary.csv` | 全仓字典当前全量 |

下游用法:首次用 `since=0` 拉全量(截至该批发布时),之后传自己所持版本号拉增量;
**重领旧批结果不变**——每批发布时的字典版本已冻结,增补区间不随后续批次变化。

## 关键设计

- **统一字典**:`dictionary(code INTEGER PK AUTOINCREMENT, name UNIQUE, version)`。
  病害名称 → 永久整数码,全仓共用;码只增不改不重用(允许有空洞,不保证连续)。
- **并发共用一码**:发码在发布事务(`BEGIN IMMEDIATE`)内 `INSERT OR IGNORE` +
  `SELECT` 完成,并发批次对同一新名称必然落到同一码。
- **准备/发布分离**:准备阶段(解析对照表、流式扫描明细、写暂存文件)只读字典
  快照、不持写锁;发布是单事务(发码 + 明细入库 + 落版本)。因此**大批准备期间
  小批可先发布**。
- **大明细流式处理**:上传分块落盘、准备逐行扫描、发布分块入库(5000 行/批)、
  下载分块读出,全程内存有界。实测 221 MB / 500 万行明细,服务进程 RSS 峰值
  仅 47 MB。
- **失败原子性**:未知代码在准备阶段即失败(尚未写任何字典/明细);发布阶段任何
  失败单事务整体回滚——本批新增编码和明细均不生效。失败批次可修正后同号重传。
- **幂等/冲突**:按两份 CSV 的 SHA256 判定。同号同内容 → 沿用原发布;同号不同
  内容 → `409`;失败批次 → 允许重传。
- **崩溃恢复**:重启时自动把 `preparing/prepared` 状态的批次重新投入流水线
  (准备是幂等的,发布是事务)。
- **存储**:SQLite(WAL),单写多读;发布事务串行,下载不阻塞。

## 配置(环境变量)

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `../data`(容器内 `/data`) | 数据目录 |
| `MAP_CODE_COL` / `MAP_NAME_COL` | `local_code` / `disease_name` | 对照表列名 |
| `DETAIL_CODE_COL` | `local_code` | 明细代码列名 |
| `OUT_CODE_COL` | `code` | 输出代码列名 |
| `PREPARE_WORKERS` | `4` | 后台准备并发度 |
| `PREPARE_DELAY_SECONDS` | `0` | 测试用:人为拉长准备阶段 |

## 验证

```bash
# 服务已启动(建议 PREPARE_DELAY_SECONDS=1 以放大准备/发布重叠窗口)
.venv/bin/python tests/e2e_test.py      # 34 项:入仓/解码/幂等/冲突/并发共用一码/小批先发布/未知代码回滚/旧批重领
.venv/bin/python tests/race_test.py     # 同号并发上传竞争
.venv/bin/python tests/bigfile_test.py  # 500 万行流式入仓 + 内存观测
```
