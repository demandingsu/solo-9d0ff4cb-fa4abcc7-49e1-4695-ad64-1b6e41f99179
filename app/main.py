"""病害数据入仓服务(养护中心数据仓库)。

功能概述:
- 网页上传: 批次号 + 路段病害明细 CSV + 本批代码对照表 CSV。
- 按本批对照表把明细中的本批代码替换为全仓统一的永久整数码;
  并发批次遇到同一新病害名称时共用一码(发布事务内 get-or-create)。
- 明细可大于内存: 上传、校验、暂存、发布、下载全程流式处理。
- 准备(慢, 不持写锁)与发布(单事务)分离: 大批准备期间小批可先发布。
- 下游按批下载编码明细与字典增补; 增补范围 = (下游所持版本, 该批发布时版本],
  旧批重领结果不随后续批次变化。
- 同号同内容重传 = 幂等沿用原发布; 同号不同内容 = 409 冲突。
- 未知代码或发布失败 => 本批新增编码与明细均不生效(准备期不写库, 发布单事务回滚)。
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import shutil
import sqlite3
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from uuid import uuid4

from flask import Flask, Response, abort, jsonify, render_template, request

# ---------------------------------------------------------------- 配置

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"))
DATA_DIR = os.path.abspath(DATA_DIR)
DB_PATH = os.path.join(DATA_DIR, "warehouse.db")
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")   # 原始上传文件(按批次号归档)
STAGE_DIR = os.path.join(DATA_DIR, "staged")     # 准备阶段产出的暂存文件

MAP_CODE_COL = os.environ.get("MAP_CODE_COL", "local_code")        # 对照表: 本批代码列名
MAP_NAME_COL = os.environ.get("MAP_NAME_COL", "disease_name")      # 对照表: 病害名称列名
DETAIL_CODE_COL = os.environ.get("DETAIL_CODE_COL", "local_code")  # 明细: 本批代码列名
OUT_CODE_COL = os.environ.get("OUT_CODE_COL", "code")              # 编码明细输出: 统一代码列名

# 测试/演示用: 人为拉长准备阶段, 便于观察"大批准备期间小批先发布"。生产置 0。
PREPARE_DELAY_SECONDS = float(os.environ.get("PREPARE_DELAY_SECONDS", "0"))

INSERT_CHUNK = 5000          # 发布时明细分块入库的行数
FETCH_CHUNK = 2000           # 下载时分块读取的行数
MAX_UNKNOWN_REPORT = 10      # 未知代码错误信息中最多列出的条数

BATCH_NO_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")

app = Flask(__name__)
_executor = ThreadPoolExecutor(max_workers=int(os.environ.get("PREPARE_WORKERS", "4")))
_tls = threading.local()

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_no        TEXT NOT NULL UNIQUE,
  status          TEXT NOT NULL,              -- preparing/prepared/published/failed
  details_sha256  TEXT NOT NULL,
  mapping_sha256  TEXT NOT NULL,
  header_csv      TEXT,                       -- 编码明细输出表头(代码列已改名)
  code_idx        INTEGER,                    -- 代码列在明细中的下标
  new_names_json  TEXT,                       -- 本批新病害名称(暂存序号顺序)
  row_count       INTEGER,
  dict_version    INTEGER,                    -- 发布时的字典版本
  error           TEXT,
  created_at      TEXT NOT NULL,
  published_at    TEXT
);
CREATE TABLE IF NOT EXISTS dictionary (
  code    INTEGER PRIMARY KEY AUTOINCREMENT,  -- 全仓统一永久整数码
  name    TEXT NOT NULL UNIQUE,               -- 病害名称
  version INTEGER NOT NULL                    -- 该条目生效时的字典版本
);
CREATE TABLE IF NOT EXISTS details (
  batch_id INTEGER NOT NULL,
  row_no   INTEGER NOT NULL,
  code     INTEGER NOT NULL,                  -- 统一整数码
  line     TEXT NOT NULL,                     -- 编码后的整行 CSV
  PRIMARY KEY (batch_id, row_no)
);
"""


class BatchError(Exception):
    """可预期的批次失败原因(未知代码、格式错误等), 信息直接展示给用户。"""


# ---------------------------------------------------------------- 基础工具

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.isolation_level = None  # 自动提交; 事务一律显式 BEGIN/COMMIT
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")        # 读写并发: 发布期间下载不阻塞
    conn.execute("PRAGMA busy_timeout=60000")      # 多发布事务串行等待
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def db() -> sqlite3.Connection:
    """每线程一条连接。"""
    conn = getattr(_tls, "conn", None)
    if conn is None:
        conn = _connect()
        _tls.conn = conn
    return conn


def init_db() -> None:
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    os.makedirs(STAGE_DIR, exist_ok=True)
    conn = _connect()
    try:
        conn.executescript(SCHEMA)
        conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES ('dict_version', '0')")
    finally:
        conn.close()


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _sha256_path(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _save_stream(stream, path: str) -> None:
    with open(path, "wb") as f:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)


def _csv_line(fields) -> str:
    buf = io.StringIO()
    csv.writer(buf, lineterminator="").writerow(fields)
    return buf.getvalue()


# ---------------------------------------------------------------- 批次流水线

def _prepare(conn, batch) -> None:
    """准备阶段: 解析对照表 -> 流式扫描明细 -> 写暂存文件。

    只读字典快照 + 一次短 UPDATE, 不持有写事务, 因此大批准备期间
    其他(小)批次可以正常发布。
    """
    batch_id = batch["id"]
    batch_no = batch["batch_no"]
    if PREPARE_DELAY_SECONDS > 0:
        time.sleep(PREPARE_DELAY_SECONDS)

    upload_dir = os.path.join(UPLOAD_DIR, batch_no)
    mapping_path = os.path.join(upload_dir, "mapping.csv")
    details_path = os.path.join(upload_dir, "details.csv")
    if not (os.path.exists(mapping_path) and os.path.exists(details_path)):
        raise BatchError("上传文件缺失(服务可能在入仓途中重启过), 请重新上传")

    # 1) 本批代码对照表: 本批代码 -> 病害名称
    code2name: dict[str, str] = {}
    with open(mapping_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            raise BatchError("对照表为空")
        header = [h.strip() for h in header]
        if MAP_CODE_COL not in header or MAP_NAME_COL not in header:
            raise BatchError(
                f"对照表缺少必需列 {MAP_CODE_COL!r}/{MAP_NAME_COL!r}, 实际表头: {header}")
        ci, ni = header.index(MAP_CODE_COL), header.index(MAP_NAME_COL)
        for ln, row in enumerate(reader, start=2):
            if not row or all(not c.strip() for c in row):
                continue
            if len(row) <= max(ci, ni):
                raise BatchError(f"对照表第 {ln} 行列数不足")
            code, name = row[ci].strip(), row[ni].strip()
            if not code or not name:
                raise BatchError(f"对照表第 {ln} 行存在空代码或空名称")
            old = code2name.get(code)
            if old is not None and old != name:
                raise BatchError(f"对照表代码 {code!r} 对应多个名称: {old!r} 与 {name!r}")
            code2name[code] = name

    # 2) 字典快照(只读): 病害名称 -> 已有统一码
    name2code = {r["name"]: r["code"] for r in conn.execute("SELECT name, code FROM dictionary")}

    # 3) 流式扫描明细, 代码列替换为令牌: 已知码 -> "K<code>", 新名称 -> "N<序号>"
    staged_path = os.path.join(STAGE_DIR, f"{batch_id}.csv")
    new_names: list[str] = []
    new_idx: dict[str, int] = {}
    unknown: list[str] = []
    row_count = 0
    with open(details_path, newline="", encoding="utf-8-sig") as f, \
            open(staged_path, "w", newline="", encoding="utf-8") as out:
        reader = csv.reader(f)
        writer = csv.writer(out, lineterminator="\n")
        header = next(reader, None)
        if header is None:
            raise BatchError("明细文件为空")
        header = [h.strip() for h in header]
        if DETAIL_CODE_COL not in header:
            raise BatchError(f"明细缺少代码列 {DETAIL_CODE_COL!r}, 实际表头: {header}")
        code_idx = header.index(DETAIL_CODE_COL)
        out_header = list(header)
        out_header[code_idx] = OUT_CODE_COL
        for ln, row in enumerate(reader, start=2):
            if not row:
                continue
            if len(row) != len(header):
                raise BatchError(f"明细第 {ln} 行列数 {len(row)} 与表头列数 {len(header)} 不一致")
            local_code = row[code_idx].strip()
            name = code2name.get(local_code)
            if name is None:
                unknown.append(f"{local_code!r}(第{ln}行)")
                if len(unknown) >= MAX_UNKNOWN_REPORT:
                    raise BatchError("明细包含对照表中不存在的代码: " + ", ".join(unknown) + " ...")
                continue
            code = name2code.get(name)
            if code is not None:
                token = f"K{code}"
            else:
                idx = new_idx.get(name)
                if idx is None:
                    idx = len(new_names)
                    new_names.append(name)
                    new_idx[name] = idx
                token = f"N{idx}"
            row[code_idx] = token
            writer.writerow(row)
            row_count += 1
        if unknown:
            raise BatchError("明细包含对照表中不存在的代码: " + ", ".join(unknown))

    conn.execute(
        "UPDATE batches SET status='prepared', new_names_json=?, row_count=?, header_csv=?, code_idx=? "
        "WHERE id=?",
        (json.dumps(new_names, ensure_ascii=False), row_count, _csv_line(out_header), code_idx, batch_id),
    )


def _publish(conn, batch) -> None:
    """发布阶段: 单事务完成 新名称发码 + 明细入库 + 批次落版。

    任一步失败整体回滚 => 本批新增编码和明细均不生效。
    BEGIN IMMEDIATE 使并发发布串行化, 同名新病害 get-or-create 共用一码。
    """
    batch_id = batch["id"]
    new_names = json.loads(batch["new_names_json"])
    code_idx = batch["code_idx"]
    staged_path = os.path.join(STAGE_DIR, f"{batch_id}.csv")

    conn.execute("BEGIN IMMEDIATE")
    try:
        # 1) 新病害名称 get-or-create; 真正新增的条目先标记 version=-1
        codes: list[int] = []
        added_any = False
        for name in new_names:
            cur = conn.execute("INSERT OR IGNORE INTO dictionary(name, version) VALUES (?, -1)", (name,))
            if cur.rowcount:
                added_any = True
            row = conn.execute("SELECT code FROM dictionary WHERE name=?", (name,)).fetchone()
            codes.append(row["code"])

        # 2) 有新条目才推进字典版本; 本批记录发布时的版本
        version = int(conn.execute("SELECT value FROM meta WHERE key='dict_version'").fetchone()["value"])
        if added_any:
            version += 1
            conn.execute("UPDATE dictionary SET version=? WHERE version=-1", (version,))
            conn.execute("UPDATE meta SET value=? WHERE key='dict_version'", (str(version),))

        # 3) 暂存文件 -> 编码明细, 流式分块入库
        inserts: list[tuple] = []
        row_no = 0
        with open(staged_path, newline="", encoding="utf-8") as f:
            for fields in csv.reader(f):
                token = fields[code_idx]
                code = codes[int(token[1:])] if token[0] == "N" else int(token[1:])
                fields[code_idx] = str(code)
                inserts.append((batch_id, row_no, code, _csv_line(fields)))
                row_no += 1
                if len(inserts) >= INSERT_CHUNK:
                    conn.executemany(
                        "INSERT INTO details(batch_id, row_no, code, line) VALUES (?,?,?,?)", inserts)
                    inserts.clear()
        if inserts:
            conn.executemany(
                "INSERT INTO details(batch_id, row_no, code, line) VALUES (?,?,?,?)", inserts)

        conn.execute(
            "UPDATE batches SET status='published', dict_version=?, published_at=?, row_count=? WHERE id=?",
            (version, _utcnow(), row_no, batch_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    # 提交成功后清理暂存文件(尽力而为)
    try:
        os.remove(staged_path)
    except OSError:
        pass


def _run_batch(batch_id: int) -> None:
    conn = db()
    try:
        batch = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if batch is None or batch["status"] == "published":
            return
        _prepare(conn, batch)
        batch = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        _publish(conn, batch)
    except Exception as exc:  # noqa: BLE001 - 任何失败都落为批次失败
        if isinstance(exc, BatchError):
            msg = str(exc)
        else:
            msg = f"{type(exc).__name__}: {exc}"
            traceback.print_exc()
        try:
            conn.execute(
                "UPDATE batches SET status='failed', error=? WHERE id=? AND status != 'published'",
                (msg, batch_id))
        except Exception:
            traceback.print_exc()
        try:
            os.remove(os.path.join(STAGE_DIR, f"{batch_id}.csv"))
        except OSError:
            pass


def _recover_incomplete() -> None:
    """重启后把尚未发布也未失败的批次重新投入流水线(准备是幂等的)。"""
    rows = db().execute("SELECT id FROM batches WHERE status IN ('preparing','prepared')").fetchall()
    for r in rows:
        _executor.submit(_run_batch, r["id"])


# ---------------------------------------------------------------- 网页 / API

def _find_batch(batch_no: str):
    b = db().execute("SELECT * FROM batches WHERE batch_no=?", (batch_no,)).fetchone()
    if b is None:
        abort(404)
    return b


def _batch_json(b) -> dict:
    return {
        "batch_no": b["batch_no"],
        "status": b["status"],
        "row_count": b["row_count"],
        "dict_version": b["dict_version"],
        "error": b["error"],
        "created_at": b["created_at"],
        "published_at": b["published_at"],
        "links": {
            "details": f"/batches/{b['batch_no']}/details.csv",
            "dict_delta": f"/batches/{b['batch_no']}/dict_delta.csv",
        },
    }


@app.get("/")
def index():
    return render_template(
        "index.html",
        map_code_col=MAP_CODE_COL, map_name_col=MAP_NAME_COL,
        detail_code_col=DETAIL_CODE_COL, out_code_col=OUT_CODE_COL,
    )


@app.get("/healthz")
def healthz():
    return "ok"


@app.post("/batches")
def create_batch():
    batch_no = (request.form.get("batch_no") or "").strip()
    if not BATCH_NO_RE.match(batch_no):
        return jsonify(error="批次号缺失或含非法字符(允许字母/数字/._-, 最长100字符)"), 400
    details = request.files.get("details")
    mapping = request.files.get("mapping")
    if details is None or mapping is None:
        return jsonify(error="需要同时上传 details(明细) 与 mapping(对照表) 两个 CSV 文件"), 400

    # 先落临时目录并算哈希, 再据哈希做幂等/冲突判定
    tmp_dir = os.path.join(UPLOAD_DIR, f"tmp-{uuid4().hex}")
    os.makedirs(tmp_dir)
    try:
        d_path = os.path.join(tmp_dir, "details.csv")
        m_path = os.path.join(tmp_dir, "mapping.csv")
        _save_stream(details.stream, d_path)
        _save_stream(mapping.stream, m_path)
        d_hash = _sha256_path(d_path)
        m_hash = _sha256_path(m_path)

        conn = db()
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT * FROM batches WHERE batch_no=?", (batch_no,)).fetchone()
            if existing is not None:
                same = (existing["details_sha256"] == d_hash
                        and existing["mapping_sha256"] == m_hash)
                if same:  # 同号同内容: 幂等沿用原发布
                    payload = _batch_json(existing)
                    payload["idempotent"] = True
                    result = jsonify(payload), 200
                    conn.commit()
                    return result
                if existing["status"] != "failed":  # 同号不同内容: 冲突
                    result = (jsonify(error="批次号已存在且内容不一致(冲突)",
                                      batch=_batch_json(existing)), 409)
                    conn.commit()
                    return result
                # 失败批次允许修正后重传
                conn.execute("DELETE FROM details WHERE batch_id=?", (existing["id"],))
                conn.execute("DELETE FROM batches WHERE id=?", (existing["id"],))
            conn.execute(
                "INSERT INTO batches(batch_no, status, details_sha256, mapping_sha256, created_at) "
                "VALUES (?,?,?,?,?)",
                (batch_no, "preparing", d_hash, m_hash, _utcnow()))
            conn.commit()
        except sqlite3.IntegrityError:
            # 并发同号上传: 以先入库者为准重新判定
            conn.rollback()
            existing = db().execute(
                "SELECT * FROM batches WHERE batch_no=?", (batch_no,)).fetchone()
            if (existing is not None
                    and existing["details_sha256"] == d_hash
                    and existing["mapping_sha256"] == m_hash):
                payload = _batch_json(existing)
                payload["idempotent"] = True
                return jsonify(payload), 200
            return jsonify(error="批次号已存在且内容不一致(冲突)"), 409
        except Exception:
            conn.rollback()
            raise

        # 只有成功插入批次记录的一方会走到这里
        final_dir = os.path.join(UPLOAD_DIR, batch_no)
        os.makedirs(final_dir, exist_ok=True)
        os.replace(d_path, os.path.join(final_dir, "details.csv"))
        os.replace(m_path, os.path.join(final_dir, "mapping.csv"))
        batch = db().execute("SELECT * FROM batches WHERE batch_no=?", (batch_no,)).fetchone()
        _executor.submit(_run_batch, batch["id"])
        payload = _batch_json(batch)
        payload["idempotent"] = False
        return jsonify(payload), 202
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


@app.get("/api/batches")
def list_batches():
    rows = db().execute("SELECT * FROM batches ORDER BY id").fetchall()
    return jsonify([_batch_json(r) for r in rows])


@app.get("/api/batches/<batch_no>")
def get_batch(batch_no):
    return jsonify(_batch_json(_find_batch(batch_no)))


@app.get("/batches/<batch_no>/details.csv")
def download_details(batch_no):
    b = _find_batch(batch_no)
    if b["status"] != "published":
        return jsonify(error=f"批次未发布(当前状态 {b['status']})"), 409
    batch_id, header = b["id"], b["header_csv"]

    def gen():
        conn = _connect()
        try:
            yield header + "\n"
            cur = conn.execute(
                "SELECT line FROM details WHERE batch_id=? ORDER BY row_no", (batch_id,))
            while True:
                rows = cur.fetchmany(FETCH_CHUNK)
                if not rows:
                    break
                for r in rows:
                    yield r["line"] + "\n"
        finally:
            conn.close()

    return Response(gen(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{batch_no}-details.csv"'})


@app.get("/batches/<batch_no>/dict_delta.csv")
def download_dict_delta(batch_no):
    """字典增补: 版本区间 (since, 该批发布时的版本] 内的字典条目。

    since = 下游所持字典版本(默认 0)。该批版本在发布时冻结,
    因此旧批重领的结果不随后续批次变化。
    """
    b = _find_batch(batch_no)
    if b["status"] != "published":
        return jsonify(error=f"批次未发布(当前状态 {b['status']})"), 409
    try:
        since = int(request.args.get("since", "0"))
        if since < 0:
            raise ValueError
    except ValueError:
        return jsonify(error="since 必须是 >= 0 的整数"), 400
    to_version = b["dict_version"]

    def gen():
        conn = _connect()
        try:
            yield _csv_line(["code", "disease_name", "version"]) + "\n"
            cur = conn.execute(
                "SELECT code, name, version FROM dictionary "
                "WHERE version > ? AND version <= ? ORDER BY code",
                (since, to_version))
            while True:
                rows = cur.fetchmany(FETCH_CHUNK)
                if not rows:
                    break
                for r in rows:
                    yield _csv_line([r["code"], r["name"], r["version"]]) + "\n"
        finally:
            conn.close()

    return Response(gen(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{batch_no}-dict-delta.csv"'})


@app.get("/dictionary.csv")
def download_dictionary():
    """全仓字典当前全量(调试用)。"""
    def gen():
        conn = _connect()
        try:
            yield _csv_line(["code", "disease_name", "version"]) + "\n"
            cur = conn.execute("SELECT code, name, version FROM dictionary ORDER BY code")
            while True:
                rows = cur.fetchmany(FETCH_CHUNK)
                if not rows:
                    break
                for r in rows:
                    yield _csv_line([r["code"], r["name"], r["version"]]) + "\n"
        finally:
            conn.close()

    return Response(gen(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition": 'attachment; filename="dictionary.csv"'})


# ---------------------------------------------------------------- 启动

init_db()
_recover_incomplete()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), threaded=True)
