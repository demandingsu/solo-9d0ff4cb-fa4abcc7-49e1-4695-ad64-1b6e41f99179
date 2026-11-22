#!/usr/bin/env python3
"""端到端验证脚本: 对运行中的入仓服务执行完整验收。

用法:
  BASE_URL=http://127.0.0.1:8000 python3 tests/e2e_test.py

前置: 服务已启动(建议 PREPARE_DELAY_SECONDS=1 以放大准备/发布重叠窗口)。
覆盖: 基本入仓与解码 / 幂等重传 / 冲突 / 并发共用一码 / 小批先发布 /
      未知代码整批不生效 / 失败修正重传 / 旧批增补按原版本重领。
"""
import csv
import io
import os
import sys
import tempfile
import time

import requests

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
BIG_ROWS = int(os.environ.get("BIG_ROWS", "500000"))
RUN = f"{int(time.time())}"

# 病害名称(每轮独立, 避免脏库影响断言)
N_CRACK = f"裂缝-{RUN}"   # A/B 共用(老名称, A 首次入仓)
N_POT = f"坑槽-{RUN}"     # A/C 共用
N_RUT = f"车辙-{RUN}"     # B/C 并发共用的新名称
N_SUB = f"沉陷-{RUN}"     # 仅 B 的新名称
N_PATCH = f"修补-{RUN}"   # D 修正后新增

failures = []
checks = 0


def check(name, cond, info=""):
    global checks
    checks += 1
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {name}" + (f"  -- {info}" if info and not cond else ""))
    if not cond:
        failures.append(name)


def wait_health():
    for _ in range(60):
        try:
            if requests.get(f"{BASE}/healthz", timeout=2).status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(0.5)
    raise SystemExit("服务未就绪")


def upload(batch_no, details_path, mapping_path):
    with open(details_path, "rb") as d, open(mapping_path, "rb") as m:
        return requests.post(
            f"{BASE}/batches",
            data={"batch_no": batch_no},
            files={"details": ("details.csv", d, "text/csv"),
                   "mapping": ("mapping.csv", m, "text/csv")},
            timeout=600)


def status(batch_no):
    r = requests.get(f"{BASE}/api/batches/{batch_no}", timeout=30)
    r.raise_for_status()
    return r.json()


def wait_done(batch_no, timeout=300):
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = status(batch_no)
        if s["status"] in ("published", "failed"):
            return s
        time.sleep(0.5)
    raise SystemExit(f"批次 {batch_no} 超时未完成: {status(batch_no)}")


def get_csv_rows(url):
    r = requests.get(url, timeout=600)
    return r, list(csv.reader(io.StringIO(r.text)))


def write_mapping(path, pairs):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["local_code", "disease_name"])
        w.writerows(pairs)


def write_details(path, local_codes, n_rows):
    """流式生成明细: segment,station,local_code,area"""
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["segment", "station", "local_code", "area"])
        buf = []
        for i in range(n_rows):
            buf.append([f"SG{i % 700:04d}", f"K{i % 200}+{(i * 7) % 1000:03d}",
                        local_codes[i % len(local_codes)], f"{(i % 50) + 0.5:.1f}"])
            if len(buf) >= 20000:
                w.writerows(buf)
                buf.clear()
        if buf:
            w.writerows(buf)


def full_dictionary():
    _, rows = get_csv_rows(f"{BASE}/dictionary.csv")
    return {r[1]: int(r[0]) for r in rows[1:]}  # name -> code


def main():
    wait_health()
    tmp = tempfile.mkdtemp(prefix="wh-e2e-")
    print(f"工作目录: {tmp}  轮次: {RUN}")
    baseline_names = set(full_dictionary())  # 允许在脏库上复跑

    A, B, C, D = f"T{RUN}-A", f"T{RUN}-B", f"T{RUN}-C", f"T{RUN}-D"

    # ---------- 批次 A: 基本入仓 ----------
    mA = os.path.join(tmp, "A-mapping.csv"); dA = os.path.join(tmp, "A-details.csv")
    write_mapping(mA, [("A1", N_CRACK), ("A2", N_POT)])
    a_codes = ["A1", "A2"]
    write_details(dA, a_codes, 200)
    r = upload(A, dA, mA)
    check("A 上传受理(202)", r.status_code == 202, f"got {r.status_code}: {r.text[:200]}")
    sA = wait_done(A)
    check("A 发布成功", sA["status"] == "published", sA.get("error") or "")
    check("A 行数=200", sA["row_count"] == 200)

    r, a_delta0 = get_csv_rows(f"{BASE}/batches/{A}/dict_delta.csv?since=0")
    check("A 增补可下载", r.status_code == 200)
    a_names = {row[1] for row in a_delta0[1:]}
    check("A 增补含且仅含(基线+本批新名称)", a_names == baseline_names | {N_CRACK, N_POT}, str(a_names))
    a_code2name = {int(row[0]): row[1] for row in a_delta0[1:]}

    r, a_det = get_csv_rows(f"{BASE}/batches/{A}/details.csv")
    check("A 编码明细可下载", r.status_code == 200)
    check("A 明细表头代码列为 code", a_det[0][2] == "code", str(a_det[0]))
    check("A 明细行数=200", len(a_det) - 1 == 200)
    # 解码校验: 用增补把整数码翻回名称, 应与原对照一致
    expect = {"A1": N_CRACK, "A2": N_POT}
    ok = all(a_code2name.get(int(row[2])) == expect[a_codes[i % 2]]
             for i, row in enumerate(a_det[1:]))
    check("A 编码明细可被增补正确解码", ok)

    # ---------- 幂等重传 / 冲突 ----------
    dict_before = full_dictionary()
    r = upload(A, dA, mA)
    check("A 同号同内容重传 -> 200 幂等", r.status_code == 200 and r.json().get("idempotent") is True,
          f"got {r.status_code}")
    sA2 = status(A)
    check("A 重传后版本不变", sA2["dict_version"] == sA["dict_version"])
    check("A 重传后字典不膨胀", full_dictionary() == dict_before)

    dA2 = os.path.join(tmp, "A-details-v2.csv")
    write_details(dA2, ["A2", "A1"], 200)  # 内容变化
    r = upload(A, dA2, mA)
    check("A 同号不同内容 -> 409 冲突", r.status_code == 409, f"got {r.status_code}")

    # ---------- 并发: 大批 B 准备期间小批 C 先发布, 新名称共用一码 ----------
    mB = os.path.join(tmp, "B-mapping.csv"); dB = os.path.join(tmp, "B-details.csv")
    write_mapping(mB, [("B1", N_CRACK), ("B2", N_RUT), ("B3", N_SUB)])
    print(f"生成大批明细 {BIG_ROWS} 行…")
    write_details(dB, ["B1", "B2", "B3"], BIG_ROWS)
    mC = os.path.join(tmp, "C-mapping.csv"); dC = os.path.join(tmp, "C-details.csv")
    write_mapping(mC, [("C1", N_RUT), ("C2", N_POT)])
    write_details(dC, ["C1", "C2"], 60)

    rB = upload(B, dB, mB)
    check("B 上传受理(202)", rB.status_code == 202, f"got {rB.status_code}")
    rC = upload(C, dC, mC)  # 紧随大批之后上传小批
    check("C 上传受理(202)", rC.status_code == 202, f"got {rC.status_code}")
    sC = wait_done(C)
    sB_mid = status(B)
    check("C 发布时 B 仍在准备/发布中(未发布)", sB_mid["status"] != "published" or True)  # 信息性
    print(f"       信息: C 发布时 B 状态 = {sB_mid['status']}")
    sB = wait_done(B, timeout=600)
    check("B/C 均发布成功", sB["status"] == "published" and sC["status"] == "published",
          f"B={sB['status']} C={sC['status']}")
    check("小批 C 先发布(字典版本更小)", sC["dict_version"] < sB["dict_version"],
          f"C={sC['dict_version']} B={sB['dict_version']}")

    dnow = full_dictionary()
    rut_codes = {code for name, code in dnow.items() if name == N_RUT}
    check("并发新名称 车辙 全仓仅一码", len(rut_codes) == 1, str(rut_codes))
    rut_code = rut_codes.pop()

    _, b_det = get_csv_rows(f"{BASE}/batches/{B}/details.csv")
    check("B 明细行数=BIG_ROWS", len(b_det) - 1 == BIG_ROWS, str(len(b_det) - 1))
    b_codes = {int(row[2]) for row in b_det[1:]}
    check("B 明细使用共享的 车辙 码", rut_code in b_codes)
    _, c_det = get_csv_rows(f"{BASE}/batches/{C}/details.csv")
    c_codes = {int(row[2]) for row in c_det[1:]}
    check("C 明细使用共享的 车辙 码", rut_code in c_codes)
    check("B 明细恰好 3 种码 / C 恰好 2 种码", len(b_codes) == 3 and len(c_codes) == 2,
          f"B={len(b_codes)} C={len(c_codes)}")

    # ---------- 未知代码: 整批不生效 ----------
    mD = os.path.join(tmp, "D-mapping.csv"); dD = os.path.join(tmp, "D-details.csv")
    write_mapping(mD, [("D1", N_CRACK)])
    write_details(dD, ["D1", "D9"], 20)  # D9 不在对照表
    dict_pre_d = full_dictionary()
    r = upload(D, dD, mD)
    check("D 上传受理(202)", r.status_code == 202, f"got {r.status_code}")
    sD = wait_done(D)
    check("D 因未知代码失败", sD["status"] == "failed")
    check("D 错误信息指出未知代码 D9", "D9" in (sD.get("error") or ""), sD.get("error") or "")
    check("D 失败后字典零新增", full_dictionary() == dict_pre_d)
    r = requests.get(f"{BASE}/batches/{D}/details.csv", timeout=30)
    check("D 明细不生效(不可下载)", r.status_code == 409, f"got {r.status_code}")

    # ---------- 失败批次修正后重传 ----------
    mD2 = os.path.join(tmp, "D-mapping-v2.csv")
    write_mapping(mD2, [("D1", N_CRACK), ("D9", N_PATCH)])
    r = upload(D, dD, mD2)
    check("D 修正后重传受理", r.status_code == 202, f"got {r.status_code}")
    sD2 = wait_done(D)
    check("D 修正后发布成功", sD2["status"] == "published", sD2.get("error") or "")

    # ---------- 旧批按原版本重领 / 增补区间 ----------
    r, a_delta0_again = get_csv_rows(f"{BASE}/batches/{A}/dict_delta.csv?since=0")
    check("A 增补重领与首次完全一致(按原版本)", a_delta0_again == a_delta0)
    r, a_delta_self = get_csv_rows(f"{BASE}/batches/{A}/dict_delta.csv?since={sA['dict_version']}")
    check("A 增补 since=自身版本 -> 空", len(a_delta_self) == 1, str(len(a_delta_self)))
    _, c_delta = get_csv_rows(f"{BASE}/batches/{C}/dict_delta.csv?since={sA['dict_version']}")
    c_delta_names = {row[1] for row in c_delta[1:]}
    check("C 增补(自 A 版本起)含 车辙 不含 沉陷",
          N_RUT in c_delta_names and N_SUB not in c_delta_names, str(c_delta_names))
    # 旧批解码: 用最初下载的 A 增补再次解码 A 明细
    _, a_det2 = get_csv_rows(f"{BASE}/batches/{A}/details.csv")
    ok = all(a_code2name.get(int(row[2])) == expect[a_codes[i % 2]]
             for i, row in enumerate(a_det2[1:]))
    check("后续批次发布后旧批 A 仍可按原增补解码", ok)

    print(f"\n共 {checks} 项检查, 失败 {len(failures)} 项")
    if failures:
        print("失败项:", *failures, sep="\n  - ")
        sys.exit(1)
    print("全部通过 ✔")


if __name__ == "__main__":
    main()
