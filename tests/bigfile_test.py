#!/usr/bin/env python3
"""大文件流式处理验证: 生成数百万行明细, 跟踪服务进程 RSS, 证明全程不整文件入内存。"""
import csv
import os
import subprocess
import sys
import tempfile
import threading
import time

import requests

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
ROWS = int(os.environ.get("HUGE_ROWS", "5000000"))
RUN = f"{int(time.time())}"


def rss_mb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1]) / 1024
    return 0.0


def worker_pid():
    out = subprocess.check_output(["pgrep", "-f", "gunicorn.*main:app"], text=True).split()
    # 取 RSS 最大的进程(worker)
    return max((int(p) for p in out), key=lambda p: rss_mb(p))


def main():
    tmp = tempfile.mkdtemp(prefix="wh-huge-")
    m = os.path.join(tmp, "m.csv"); d = os.path.join(tmp, "d.csv")
    with open(m, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows([["local_code", "disease_name"], ["H1", f"裂缝-{RUN}"], ["H2", f"坑槽-{RUN}"]])
    print(f"生成 {ROWS} 行明细…")
    with open(d, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["segment", "station", "local_code", "area", "note"])
        buf = []
        for i in range(ROWS):
            buf.append([f"SG{i%900:05d}", f"K{i%300}+{(i*13)%1000:03d}",
                        "H1" if i % 2 else "H2", f"{(i%80)+0.25:.2f}", f"巡检记录{i}"])
            if len(buf) >= 50000:
                w.writerows(buf); buf.clear()
        if buf:
            w.writerows(buf)
    fsize = os.path.getsize(d) / 1024 / 1024
    print(f"明细文件大小: {fsize:.0f} MB")

    pid = worker_pid()
    base_rss = rss_mb(pid)
    peak = [base_rss]
    stop = threading.Event()

    def watch():
        while not stop.is_set():
            peak[0] = max(peak[0], rss_mb(pid))
            time.sleep(0.1)

    tw = threading.Thread(target=watch); tw.start()
    batch = f"H{RUN}"
    t0 = time.time()
    with open(d, "rb") as df, open(m, "rb") as mf:
        r = requests.post(f"{BASE}/batches", data={"batch_no": batch},
                          files={"details": ("d.csv", df), "mapping": ("m.csv", mf)}, timeout=900)
    assert r.status_code == 202, r.text[:200]
    while True:
        s = requests.get(f"{BASE}/api/batches/{batch}", timeout=30).json()
        if s["status"] in ("published", "failed"):
            break
        time.sleep(1)
    stop.set(); tw.join()
    dt = time.time() - t0
    print(f"状态={s['status']} 行数={s['row_count']} 耗时={dt:.1f}s")
    print(f"RSS: 空闲 {base_rss:.0f} MB -> 峰值 {peak[0]:.0f} MB (文件 {fsize:.0f} MB)")

    ok = True
    ok &= s["status"] == "published" and s["row_count"] == ROWS
    # 流式下载侧也不应整文件入内存: 边下边数行
    n = 0
    with requests.get(f"{BASE}/batches/{batch}/details.csv", stream=True, timeout=900) as resp:
        for _ in resp.iter_lines():
            n += 1
    print(f"下载行数(含表头): {n}")
    ok &= n == ROWS + 1
    # RSS 峰值应远小于整文件+解析对象入内存的开销(经验上 5M 行解析对象 >2GB)
    ok &= peak[0] < 1024
    print("PASS 大文件流式入仓与下载" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
