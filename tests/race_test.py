#!/usr/bin/env python3
"""并发竞争专项: 同号同内容并发上传 => 恰好一个受理, 其余幂等; 同号不同内容 => 一受理一冲突。"""
import csv
import os
import sys
import tempfile
import threading
import time

import requests

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
RUN = f"{int(time.time())}"
failures = []


def check(name, cond, info=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  -- {info}" if info and not cond else ""))
    if not cond:
        failures.append(name)


def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def upload(batch_no, d, m, results, idx):
    with open(d, "rb") as df, open(m, "rb") as mf:
        r = requests.post(f"{BASE}/batches", data={"batch_no": batch_no},
                          files={"details": ("d.csv", df), "mapping": ("m.csv", mf)}, timeout=120)
    results[idx] = r.status_code


def wait_done(batch_no, timeout=120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = requests.get(f"{BASE}/api/batches/{batch_no}", timeout=10).json()
        if s["status"] in ("published", "failed"):
            return s
        time.sleep(0.3)
    raise SystemExit(f"{batch_no} 超时")


def main():
    tmp = tempfile.mkdtemp(prefix="wh-race-")
    name1, name2 = f"松散-{RUN}", f"泛油-{RUN}"

    # --- 同号同内容 x6 并发 ---
    d1 = os.path.join(tmp, "d1.csv"); m1 = os.path.join(tmp, "m1.csv")
    write_csv(m1, ["local_code", "disease_name"], [["X1", name1]])
    write_csv(d1, ["segment", "local_code"], [[f"S{i}", "X1"] for i in range(50)])
    batch = f"R{RUN}-same"
    results = [None] * 6
    threads = [threading.Thread(target=upload, args=(batch, d1, m1, results, i)) for i in range(6)]
    for t in threads: t.start()
    for t in threads: t.join()
    n202, n200 = results.count(202), results.count(200)
    check(f"同号同内容并发: 恰好1个202, 其余200幂等 (实际 {results})",
          n202 == 1 and n200 == 5)
    s = wait_done(batch)
    check("并发同号批次发布成功且行数=50(未重复)", s["status"] == "published" and s["row_count"] == 50)
    r = requests.get(f"{BASE}/batches/{batch}/details.csv", timeout=30)
    check("编码明细恰好50行(未重复)", len(r.text.strip().splitlines()) == 51)

    # --- 同号不同内容并发: 一受理一冲突 ---
    d2 = os.path.join(tmp, "d2.csv"); m2 = os.path.join(tmp, "m2.csv")
    write_csv(m2, ["local_code", "disease_name"], [["Y1", name2]])
    write_csv(d2, ["segment", "local_code"], [[f"T{i}", "Y1"] for i in range(30)])
    batch2 = f"R{RUN}-diff"
    results2 = [None] * 2
    t1 = threading.Thread(target=upload, args=(batch2, d1, m1, results2, 0))
    t2 = threading.Thread(target=upload, args=(batch2, d2, m2, results2, 1))
    t1.start(); t2.start(); t1.join(); t2.join()
    accepted = results2.count(202) + results2.count(200)
    check(f"同号不同内容并发: 一方受理一方409 (实际 {results2})",
          accepted == 1 and results2.count(409) == 1)

    print(f"\n失败 {len(failures)} 项")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
