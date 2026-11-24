"""端到端自测：已完成工单留存卷的缺件保护（XOR 奇偶卷 + 单卷重建）。

覆盖需求：
  1. 仅 DONE 工单可启用；/docs 路由可用，状态可查；保护数据完整落盘并读回核对后才显示已保护
  2. 开单 SHA-256 作为所有核对基准；启用中断（重启）后可重试，临时正文不累积
  3. 演练只在停服时改动正文（所有改动经服务 API/停服移动文件完成）
  4. 移走任一留存文件后重启，仍可从原下载链接取回该卷，内容与开单 SHA-256 一致
  5. 重建只用现存留存卷 + 保护卷；整卷通过开单 SHA-256 才发送正文
  6. 同单缺两件 -> 缺件下载报错；重建校验失败 -> 报错且无临时正文残留
  7. 同一工单同时只处理一次启用/缺件下载（并发一个 409）；现存卷照常下载
  8. 保护正文 + 临时正文 <= 3 个卷大小；一次缺件下载读取 <= 4 个卷大小（含回读口径）；
     正文缓冲 <= 1 MiB（分块 128 KiB，峰值 4 块 = 512 KiB）

运行：python3 tests/selftest_protection.py
"""
from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
PORT = 8766
BASE = f"http://127.0.0.1:{PORT}"

FAILURES: list[str] = []
CHECKS = 0
VOL_SIZE = 300_000          # 四个等长留存卷
N_VOL = 4


def check(name: str, cond: bool, detail: str = "") -> None:
    global CHECKS
    CHECKS += 1
    if cond:
        print(f"  PASS  {name}")
    else:
        FAILURES.append(f"{name}: {detail}")
        print(f"  FAIL  {name}  {detail}")


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def start_server(data_dir: Path) -> subprocess.Popen:
    env = dict(os.environ, DATA_DIR=str(data_dir))
    log = open(data_dir / "server.log", "ab")
    p = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(PORT)],
        cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 30
    while time.time() < deadline:
        if p.poll() is not None:
            raise RuntimeError(f"server exited early, see {data_dir}/server.log")
        try:
            if httpx.get(f"{BASE}/sim/cluster", timeout=2).status_code == 200:
                return p
        except httpx.TransportError:
            time.sleep(0.3)
    p.kill()
    raise RuntimeError("server did not become ready")


def stop_server(p: subprocess.Popen) -> None:
    p.terminate()
    try:
        p.wait(timeout=10)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=5)


def list_tmp_files(data_dir: Path, tid: str) -> list[Path]:
    out = []
    tdir = data_dir / "retained" / tid
    for f in tdir.rglob("*.tmp"):
        out.append(f)
    return out


def main() -> int:
    data_dir = Path(tempfile.mkdtemp(prefix="prot-drill-"))
    print(f"DATA_DIR={data_dir}")
    server = start_server(data_dir)
    c = httpx.Client(base_url=BASE, timeout=30)
    try:
        names = ["vol-a", "vol-b", "vol-c", "vol-d"]
        print("\n== 1. 造数 + 开单 + 推进至 DONE（四个等长留存卷） ==")
        seed = c.post("/sim/seed", json={
            "namespace": "demo",
            "pvcs": [{"name": n, "size_bytes": VOL_SIZE, "reclaim_policy": "Delete"}
                     for n in names],
        })
        check("seed -> 201", seed.status_code == 201, seed.text)
        want = {p["name"]: p["sha256"] for p in seed.json()["pvcs"]}
        t = c.post("/tickets", json={"namespace": "demo", "retain_pvcs": names})
        check("开单 -> 201", t.status_code == 201, t.text)
        tid = t.json()["ticket"]["id"]
        done = c.post(f"/tickets/{tid}/advance").json()
        check("工单 DONE", done["ticket"]["state"] == "DONE", done["ticket"]["state"])
        for n in names:
            body = c.get(f"/tickets/{tid}/retained/{n}").content
            check(f"DONE 后 {n} 可正常下载且 sha 与开单一致", sha(body) == want[n])

        print("\n== 2. 非 DONE/未启用的保护边界 ==")
        ps0 = c.get(f"/tickets/{tid}/protection")
        check("保护状态可查（未启用）", ps0.status_code == 200
              and ps0.json()["protection"] is None, ps0.text)
        miss = c.get(f"/tickets/{tid}/retained/vol-a")  # 先确认文件在
        check("现存卷照常下载 -> 200", miss.status_code == 200)

        print("\n== 3. 启用缺件保护 ==")
        en = c.post(f"/tickets/{tid}/protection/enable")
        check("启用 -> 200", en.status_code == 200, en.text)
        enj = en.json()
        check("完整落盘核对后才显示 protected",
              enj["protection"]["state"] == "protected", str(enj["protection"]))
        check("保护卷 sha256 已登记", len(enj["protection"]["parity_sha256"]) == 64)
        check("启用读取量 = (N+1)*V（生成 NV + 暂存回读 V，回读计入）",
              enj["enable"]["read_bytes"] == (N_VOL + 1) * VOL_SIZE
              and enj["enable"]["read_budget_bytes"] == (N_VOL + 1) * VOL_SIZE,
              str(enj["enable"]))
        check("保护正文落盘 = 1V，临时正文为 0（<=3V）",
              enj["disk"]["protection_plus_temp_bytes"] == VOL_SIZE
              and enj["disk"]["cap_bytes"] == 3 * VOL_SIZE, str(enj["disk"]))
        check("四卷均存活、无缺失",
              all(v["present"] for v in enj["volumes"])
              and enj["missing_volumes"] == [], str(enj["volumes"]))
        parity = data_dir / "retained" / tid / ".protection" / "parity.bin"
        check("保护卷文件存在且与卷等长",
              parity.exists() and parity.stat().st_size == VOL_SIZE)
        check("无临时正文残留", list_tmp_files(data_dir, tid) == [])
        # 重复启用幂等
        en2 = c.post(f"/tickets/{tid}/protection/enable")
        check("重复启用幂等 -> protected",
              en2.status_code == 200 and en2.json()["protection"]["state"] == "protected")

        print("\n== 4. 重启服务：保护态持久、无需人工干预 ==")
        stop_server(server)
        server = start_server(data_dir)
        ps = c.get(f"/tickets/{tid}/protection").json()
        check("重启后仍为 protected", ps["protection"]["state"] == "protected")
        check("重启后保护卷仍在", parity.exists())
        check("重启清理后无临时文件", list_tmp_files(data_dir, tid) == [])

        print("\n== 5. 移走一个留存文件后重启，原链接取回（缺件下载重建） ==")
        victim_dir = data_dir / "retained" / tid / "vol-b"
        victim = victim_dir / "data.bin"
        victim.rename(data_dir / "stolen-vol-b.bin")   # 停服演练动作
        check("文件确已移走", not victim.exists())
        stop_server(server)
        server = start_server(data_dir)
        ps = c.get(f"/tickets/{tid}/protection").json()
        check("状态显示 vol-b 缺失且可单件重建",
              ps["missing_volumes"] == ["vol-b"] and ps["can_rebuild_single"] is True,
              str(ps["missing_volumes"]))
        r = c.get(f"/tickets/{tid}/retained/vol-b")
        check("原链接取回成功 -> 200", r.status_code == 200, r.text)
        check("重建正文 sha 与开单 SHA-256 完全一致", sha(r.content) == want["vol-b"])
        check("重建后文件恢复在原路径", victim.exists()
              and victim.stat().st_size == VOL_SIZE)
        ps = c.get(f"/tickets/{tid}/protection").json()
        check("取回后缺失列表清空、can_rebuild 恢复",
              ps["missing_volumes"] == [] and ps["can_rebuild_single"] is True)
        io = ps["last_rebuild_io"]
        check("一次缺件下载读取 = 4V（4 输入卷；内联整卷哈希，无额外回读）",
              io["read_bytes"] == 4 * VOL_SIZE
              and io["read_budget_bytes"] == 4 * VOL_SIZE, str(io))
        check("分块 128KiB、4 块同时在内存 = 512KiB <= 1MiB 正文缓冲",
              io["chunk_size"] == 128 * 1024 and 4 * io["chunk_size"] <= 1024 * 1024)
        check("重建后无临时正文残留", list_tmp_files(data_dir, tid) == [])
        check("保护+临时正文始终 <= 3V",
              ps["disk"]["protection_plus_temp_bytes"] <= ps["disk"]["cap_bytes"])

        print("\n== 6. 同单缺两件：缺件下载报错，现存卷照常下载 ==")
        (data_dir / "retained" / tid / "vol-c" / "data.bin").rename(
            data_dir / "stolen-vol-c.bin")
        (data_dir / "retained" / tid / "vol-d" / "data.bin").rename(
            data_dir / "stolen-vol-d.bin")
        stop_server(server)
        server = start_server(data_dir)
        ps = c.get(f"/tickets/{tid}/protection").json()
        check("状态列出两个缺失卷且判定不可重建",
              sorted(ps["missing_volumes"]) == ["vol-c", "vol-d"]
              and ps["can_rebuild_single"] is False, str(ps["missing_volumes"]))
        rc = c.get(f"/tickets/{tid}/retained/vol-c")
        check("缺两件下载 -> 409 报错", rc.status_code == 409 and "缺失 2" in rc.text,
              rc.text)
        ra = c.get(f"/tickets/{tid}/retained/vol-a")
        check("现存卷 vol-a 照常下载且 sha 正确",
              ra.status_code == 200 and sha(ra.content) == want["vol-a"])
        rb = c.get(f"/tickets/{tid}/retained/vol-b")
        check("已重建卷 vol-b 照常下载且 sha 正确",
              rb.status_code == 200 and sha(rb.content) == want["vol-b"])
        check("报错路径无临时正文残留", list_tmp_files(data_dir, tid) == [])

        print("\n== 7. 重建校验失败：污染保护卷，重建报错且不落临时正文 ==")
        # 先恢复 vol-c，只保留 vol-d 缺失，制造“可重建但输出错误”的场景
        (data_dir / "stolen-vol-c.bin").rename(
            data_dir / "retained" / tid / "vol-c" / "data.bin")
        stop_server(server)
        server = start_server(data_dir)
        pb = parity.read_bytes()
        corrupted = pb[:-1] + bytes([pb[-1] ^ 0x01])
        parity.write_bytes(corrupted)
        rd = c.get(f"/tickets/{tid}/retained/vol-d")
        check("校验失败 -> 409 且提示 SHA-256 失败",
              rd.status_code == 409 and "SHA-256" in rd.text, rd.text)
        check("失败后 vol-d 仍缺失",
              c.get(f"/tickets/{tid}/protection").json()["missing_volumes"] == ["vol-d"])
        check("失败后无临时正文残留", list_tmp_files(data_dir, tid) == [])
        check("保护+临时正文仍 <= 3V",
              c.get(f"/tickets/{tid}/protection").json()["disk"][
                  "protection_plus_temp_bytes"] <= 3 * VOL_SIZE)
        # 恢复保护卷 -> 重建成功（失败可重试）
        parity.write_bytes(pb)
        rd2 = c.get(f"/tickets/{tid}/retained/vol-d")
        check("恢复后重试重建成功且 sha 正确",
              rd2.status_code == 200 and sha(rd2.content) == want["vol-d"], rd2.text)
        check("成功后无临时正文残留", list_tmp_files(data_dir, tid) == [])

        print("\n== 8. 并发：同一工单同时只处理一次缺件下载 ==")
        (data_dir / "retained" / tid / "vol-a" / "data.bin").rename(
            data_dir / "stolen-vol-a.bin")
        stop_server(server)
        server = start_server(data_dir)

        # 8a HTTP 层：缺失卷经原链接取回；处理期间/之后现存卷照常下载
        r = c.get(f"/tickets/{tid}/retained/vol-a")
        check("HTTP 缺件下载成功且 sha 正确",
              r.status_code == 200 and sha(r.content) == want["vol-a"], r.text)
        check("HTTP 重建后无临时正文", list_tmp_files(data_dir, tid) == [])

        # 8b 确定性引擎级：在“持锁的异或阶段”插入同步点，保证第二线程必在第一线程持锁期间抢锁
        sys.path.insert(0, str(ROOT))
        from app import engine as _eng_mod
        from app.engine import Engine as _Engine
        from app.simcluster import ConflictError as _ConflictError
        big_seed = c.post("/sim/seed", json={
            "namespace": "big",
            "pvcs": [{"name": f"b{i}", "size_bytes": 20 * 1024 * 1024,
                      "reclaim_policy": "Delete"} for i in range(4)],
        })
        big_want = {p["name"]: p["sha256"] for p in big_seed.json()["pvcs"]}
        btid = c.post("/tickets", json={
            "namespace": "big", "retain_pvcs": [f"b{i}" for i in range(4)]}
        ).json()["ticket"]["id"]
        c.post(f"/tickets/{btid}/advance")
        c.post(f"/tickets/{btid}/protection/enable")
        eng_local = _Engine(data_dir)
        victim_b = data_dir / "retained" / btid / "b1" / "data.bin"
        victim_b.rename(data_dir / "stolen-b1.bin")
        row_b1 = next(r for r in eng_local._selected_pvc_rows(btid) if r["name"] == "b1")
        lock_outcomes: list[str] = []
        inside = threading.Event()      # 第一线程已进入持锁异或区
        release = threading.Event()      # 放行第一线程
        real_xor = _eng_mod.prot.xor_streams

        def gated_xor(inputs, output, meter, expected_size):
            inside.set()
            release.wait(timeout=10)
            return real_xor(inputs, output, meter, expected_size)

        def first_job():
            _eng_mod.prot.xor_streams = gated_xor
            try:
                p = eng_local._rebuild_missing(btid, row_b1)
                lock_outcomes.append("ok:" + ("good" if sha(p.read_bytes())
                                               == big_want["b1"] else "bad"))
            finally:
                _eng_mod.prot.xor_streams = real_xor

        def second_job():
            inside.wait(timeout=10)      # 等第一线程确定持锁
            try:
                eng_local._rebuild_missing(btid, row_b1)
                lock_outcomes.append("ok:unexpected")
            except _ConflictError:
                lock_outcomes.append("conflict")
            finally:
                release.set()           # 放行第一线程完成重建

        t1 = threading.Thread(target=first_job)
        t2 = threading.Thread(target=second_job)
        t1.start()
        t2.start()
        t1.join(timeout=30)
        t2.join(timeout=30)
        check("同单并发缺件下载：持锁中第二个请求立即 409，第一个成功",
              sorted(lock_outcomes) == ["conflict", "ok:good"], str(lock_outcomes))
        check("冲突后文件唯一且无临时正文",
              victim_b.exists() and list_tmp_files(data_dir, btid) == [])
        check("大卷重建读取量仍精确为 4V",
              eng_local.protection_status(btid)["last_rebuild_io"]["read_bytes"]
              == 4 * 20 * 1024 * 1024)
        rb_during = c.get(f"/tickets/{tid}/retained/vol-b")
        check("其他工单/现存卷照常下载且 sha 正确",
              rb_during.status_code == 200 and sha(rb_during.content) == want["vol-b"])

        print("\n== 9. 启用中断后重试、临时正文不累积 ==")
        # 造一张新工单，制造中断态（enabling 行 + 临时文件），重启后应可重试成功
        seed2 = c.post("/sim/seed", json={
            "namespace": "demo2",
            "pvcs": [{"name": n, "size_bytes": VOL_SIZE, "reclaim_policy": "Delete"}
                     for n in names],
        })
        want2 = {p["name"]: p["sha256"] for p in seed2.json()["pvcs"]}
        t2 = c.post("/tickets", json={"namespace": "demo2", "retain_pvcs": names})
        tid2 = t2.json()["ticket"]["id"]
        c.post(f"/tickets/{tid2}/advance")
        check("第二单 DONE",
              c.get(f"/tickets/{tid2}").json()["ticket"]["state"] == "DONE")
        stop_server(server)
        # 手动注入中断现场：临时保护正文 + enabling 状态
        pdir2 = data_dir / "retained" / tid2 / ".protection"
        pdir2.mkdir(parents=True, exist_ok=True)
        (pdir2 / "parity.tmp").write_bytes(b"stale-temp-body")
        (data_dir / "retained" / tid2 / "vol-a" / ".rebuild.tmp").write_bytes(b"x")
        import sqlite3
        conn = sqlite3.connect(str(data_dir / "app.db"))
        conn.execute(
            "INSERT INTO protection(ticket_id,state,volume_size,volume_count,"
            "parity_name,parity_sha256,enabled_at,updated_at) "
            "VALUES(?,?,?,?,?,?,NULL,?)",
            (tid2, "enabling", VOL_SIZE, N_VOL, "parity.bin", "", "2026-01-01T00:00:00Z"))
        conn.commit()
        conn.close()
        server = start_server(data_dir)
        ps2 = c.get(f"/tickets/{tid2}/protection").json()
        check("重启后中断态被标记为 interrupted",
              ps2["protection"]["state"] == "interrupted", str(ps2["protection"]))
        check("中断遗留临时正文已清除（不累积）", list_tmp_files(data_dir, tid2) == [])
        en3 = c.post(f"/tickets/{tid2}/protection/enable")
        check("中断后重试启用成功 -> protected",
              en3.status_code == 200 and en3.json()["protection"]["state"] == "protected",
              en3.text)
        check("重试后只有正式保护卷、无临时文件",
              (pdir2 / "parity.bin").exists() and list_tmp_files(data_dir, tid2) == [])
        # 第二单也能缺件重建
        (data_dir / "retained" / tid2 / "vol-c" / "data.bin").rename(
            data_dir / "stolen2-vol-c.bin")
        stop_server(server)
        server = start_server(data_dir)
        rc2 = c.get(f"/tickets/{tid2}/retained/vol-c")
        check("第二单缺件重建成功且 sha 与开单一致",
              rc2.status_code == 200 and sha(rc2.content) == want2["vol-c"], rc2.text)

        print("\n== 10. 资源纪律总复核 ==")
        for tidx in (tid, tid2, btid):
            ps = c.get(f"/tickets/{tidx}/protection").json()
            check(f"工单 {tidx[:6]} 保护+临时 <= 3V",
                  ps["disk"]["protection_plus_temp_bytes"] <= ps["disk"]["cap_bytes"],
                  str(ps["disk"]))
            check(f"工单 {tidx[:6]} 无任何 .tmp 残留",
                  list_tmp_files(data_dir, tidx) == [])
            check(f"工单 {tidx[:6]} 正文缓冲上限 1MiB 已声明",
                  ps["disk"]["body_buffer_cap_bytes"] == 1024 * 1024)
        check("/docs 可打开", c.get("/docs").status_code == 200)

        print("\n== 11. 原有清退自测回归（保护功能不影响主流程） ==")
        cl = c.get("/sim/cluster").json()
        check("三命名空间均已清退", len(cl["namespaces"]) == 0, str(cl["namespaces"]))
        check("工单列表含三单", len(c.get("/tickets").json()["tickets"]) == 3)
    finally:
        stop_server(server)

    print(f"\n{'=' * 50}")
    if FAILURES:
        print(f"FAILED {len(FAILURES)}/{CHECKS}:")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print(f"ALL CHECKS PASSED ({CHECKS})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
