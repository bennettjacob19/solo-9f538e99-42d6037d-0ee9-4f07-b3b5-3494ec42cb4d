"""清退引擎：按工单推进命名空间清理的状态机。

阶段：VALIDATING -> RETAINING -> DELETING_NAMESPACE -> DONE（异常：FAILED）
阻塞：blocked_reason 字段记录当前阻塞原因，推进时自动清除并重试。

关键不变量：
  * 先留存、验证（在原命名空间之外独立读回校验 sha256），再删除对应源 PVC。
  * 任一时刻 原卷字节 + 留存副本字节 <= 开单基线 + 最大选中卷大小。
  * 所有删除按开单时快照的 UID 寻址；同名重建的命名空间 UID 不同，不受影响。
  * 删除操作写入 ops 日志（幂等键）；已确认(confirmed)的删除绝不重发；
    结果未明(unknown)的删除先 GET 核对：目标已消失则标记确认，仍在才重发。
"""
from __future__ import annotations

import json
import os
import shutil
import threading
import uuid
from pathlib import Path

from . import protection as prot
from .db import DB
from .simcluster import (ConflictError, DroppedResponseError, NotFoundError,
                         SimCluster)
from .util import now_iso, sha256_file, write_json

MAX_STEPS_PER_ADVANCE = 1000
MAX_PROTECTED_VOLUMES = 4          # 一次缺件下载最多读 4 个卷大小
REBUILD_READ_BUDGET_VOLUMES = 4    # 预算：4V（含暂存回读）
PROTECTED_DISK_BUDGET_VOLUMES = 3  # 保护正文 + 临时正文 <= 3V


class Engine:
    def __init__(self, data_dir: Path | str):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.sim = SimCluster(self.data_dir / "cluster")
        self.retained_root = self.data_dir / "retained"
        self.retained_root.mkdir(parents=True, exist_ok=True)
        self.db = DB(self.data_dir / "app.db")
        self._lock = threading.RLock()
        # 同一工单的“启用保护 / 缺件下载”互斥（非阻塞抢占，抢不到即 409）；
        # 现存卷的正常下载不走该锁，互不影响。
        self._job_locks: dict[str, threading.Lock] = {}
        self._job_locks_guard = threading.Lock()
        self._recover_protection()

    # ---------------- 开单 ----------------
    def create_ticket(self, namespace: str, retain_pvcs: list[str], note: str | None = None) -> dict:
        with self._lock:
            if not retain_pvcs:
                raise ValueError("至少选择一个需保留的 PVC")
            ns = self.sim.get_namespace_by_name(namespace)
            if not ns:
                raise NotFoundError(f"命名空间 {namespace!r} 不存在")
            pvcs = self.sim.list_pvcs(ns_uid=ns["uid"])
            by_name = {p["name"]: p for p in pvcs}
            missing = [n for n in retain_pvcs if n not in by_name]
            if missing:
                raise NotFoundError(f"命名空间 {namespace!r} 中不存在 PVC: {missing}")
            tid = uuid.uuid4().hex[:12]
            selected = [by_name[n] for n in retain_pvcs]
            baseline = sum(p["size_bytes"] for p in pvcs)
            max_sel = max(p["size_bytes"] for p in selected)
            self.db.execute(
                "INSERT INTO tickets(id,created_at,namespace_name,namespace_uid,state,"
                "blocked_reason,note,baseline_bytes,max_selected_bytes,peak_bytes) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (tid, now_iso(), ns["name"], ns["uid"], "VALIDATING", None, note,
                 baseline, max_sel, baseline))
            for p in pvcs:
                self.db.execute(
                    "INSERT INTO pvcs(ticket_id,uid,name,pv_name,size_bytes,reclaim_policy,"
                    "selected,sha256,copy_state,delete_state) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (tid, p["uid"], p["name"], p["pv_name"], p["size_bytes"],
                     p["reclaim_policy"], 1 if p["name"] in retain_pvcs else 0,
                     p["sha256"], "pending", "pending"))
            return self.status(tid)

    # ---------------- 查询 ----------------
    def _ticket(self, tid: str) -> dict:
        t = self.db.one("SELECT * FROM tickets WHERE id=?", (tid,))
        if not t:
            raise NotFoundError(f"工单 {tid} 不存在")
        return t

    def list_tickets(self) -> list[dict]:
        return self.db.query("SELECT * FROM tickets ORDER BY created_at DESC")

    def _current_bytes(self, t: dict) -> int:
        """原卷现存字节 + 本单留存副本字节。"""
        total = sum(p["size_bytes"] for p in self.sim.list_pvcs(ns_uid=t["namespace_uid"]))
        rdir = self.retained_root / t["id"]
        if rdir.exists():
            total += sum(f.stat().st_size for f in rdir.rglob("data.bin"))
        return total

    def retained_items(self, tid: str) -> list[dict]:
        rdir = self.retained_root / tid
        out = []
        if rdir.exists():
            for d in sorted(rdir.iterdir()):
                meta = d / "meta.json"
                data = d / "data.bin"
                if meta.exists() and data.exists():
                    m = json.loads(meta.read_text(encoding="utf-8"))
                    out.append({**m, "data_bytes_on_disk": data.stat().st_size})
        return out

    def retained_file(self, tid: str, pvc_name: str) -> Path:
        with self._lock:
            self._ticket(tid)
            row = self.db.one(
                "SELECT * FROM pvcs WHERE ticket_id=? AND name=? AND selected=1", (tid, pvc_name))
            if not row:
                raise NotFoundError(f"工单 {tid} 中不存在选中 PVC {pvc_name!r}")
            path = self.retained_root / tid / pvc_name / "data.bin"
            meta = self.retained_root / tid / pvc_name / "meta.json"
            if path.exists():
                return path  # 现存卷照常下载：不占任务锁，与启用/重建互不阻塞
            if not meta.exists() and not self._protection_row(tid):
                raise NotFoundError(f"工单 {tid} 的 PVC {pvc_name!r} 尚未完成留存")
        # 留存正文缺失 -> 走缺件保护重建（同单串行，抢锁失败即 409）
        return self._rebuild_missing(tid, row)

    def status(self, tid: str) -> dict:
        t = self._ticket(tid)
        pvcs = self.db.query("SELECT * FROM pvcs WHERE ticket_id=? ORDER BY name", (tid,))
        ops = self.db.query("SELECT * FROM ops WHERE ticket_id=? ORDER BY rowid", (tid,))
        current = self._current_bytes(t)
        cap = t["baseline_bytes"] + t["max_selected_bytes"]
        leftover_pvs = []
        for p in pvcs:
            pv = self.sim.get_pv(p["pv_name"])
            if pv:
                leftover_pvs.append({
                    "pv_name": pv["name"], "pvc_name": p["name"], "state": pv["state"],
                    "reclaim_policy": pv["reclaim_policy"], "size_bytes": pv["size_bytes"],
                })
        return {
            "ticket": {**t, "cap_bytes": cap},
            "pvcs": pvcs,
            "ops": ops,
            "storage": {
                "baseline_bytes": t["baseline_bytes"],
                "max_selected_bytes": t["max_selected_bytes"],
                "cap_bytes": cap,
                "current_bytes": current,
                "peak_bytes": t["peak_bytes"],
                "headroom_bytes": cap - current,
            },
            "retained": self.retained_items(tid),
            "leftover_pvs": leftover_pvs,
            "cluster": {
                "namespace_exists": self.sim.get_namespace_by_uid(t["namespace_uid"]) is not None,
                "remaining_pvcs": self.sim.list_pvcs(ns_uid=t["namespace_uid"]),
            },
        }

    # ---------------- 推进 ----------------
    def advance(self, tid: str) -> dict:
        with self._lock:
            t = self._ticket(tid)
            if t["state"] in ("DONE", "FAILED"):
                return self.status(tid)
            self._set_blocked(tid, None)
            # 先核对上一回合可能结果未明的删除
            self.reconcile(tid)
            if self.db.one("SELECT 1 AS x FROM ops WHERE ticket_id=? AND state='unknown'", (tid,)):
                self._set_blocked(tid, "存在结果未明的删除，核对后仍未确认（可能再次丢包）；请再次推进")
                return self.status(tid)
            for _ in range(MAX_STEPS_PER_ADVANCE):
                t = self._ticket(tid)
                if t["state"] in ("DONE", "FAILED") or t["blocked_reason"]:
                    break
                if t["state"] == "VALIDATING":
                    progressed = self._validate(t)
                elif t["state"] == "RETAINING":
                    progressed = self._retain_step(t)
                elif t["state"] == "DELETING_NAMESPACE":
                    progressed = self._delete_ns_step(t)
                else:
                    progressed = False
                if not progressed:
                    break
                self._bump_peak(self._ticket(tid))
            return self.status(tid)

    def _validate(self, t: dict) -> bool:
        tid = t["id"]
        ns = self.sim.get_namespace_by_uid(t["namespace_uid"])
        if not ns:
            self._set_blocked(
                tid,
                f"开单时的命名空间 {t['namespace_name']}(uid={t['namespace_uid']}) 已不存在；"
                "若系同名重建，其 UID 不同，本单不会作用于新命名空间。请人工核查后处理。")
            return False
        missing = [p["name"] for p in self.db.query(
            "SELECT * FROM pvcs WHERE ticket_id=? AND selected=1", (tid,))
            if self.sim.get_pvc(p["uid"]) is None]
        if missing:
            self._set_blocked(tid, f"选中 PVC 已不存在，无法留存: {missing}")
            return False
        self.db.execute("UPDATE tickets SET state='RETAINING' WHERE id=?", (tid,))
        return True

    def _retain_step(self, t: dict) -> bool:
        tid = t["id"]
        rows = self.db.query(
            "SELECT * FROM pvcs WHERE ticket_id=? AND selected=1 ORDER BY name", (tid,))
        todo = [r for r in rows
                if not (r["copy_state"] == "verified" and r["delete_state"] == "confirmed")]
        if not todo:
            self.db.execute("UPDATE tickets SET state='DELETING_NAMESPACE' WHERE id=?", (tid,))
            return True
        r = todo[0]
        if r["copy_state"] == "pending":
            current = self._current_bytes(t)
            cap = t["baseline_bytes"] + t["max_selected_bytes"]
            if current + r["size_bytes"] > cap:
                self._set_blocked(
                    tid,
                    f"存储上限保护：当前 {current} + 待拷 {r['size_bytes']} 将超过上限 {cap}"
                    f"（基线 {t['baseline_bytes']} + 最大选中卷 {t['max_selected_bytes']}），暂停拷贝")
                return False
            self._copy_to_retained(t, r)
            return True
        if r["copy_state"] == "copied":
            return self._verify_retained(t, r)
        # 已验证 -> 删除源 PVC（按 PV 回收策略由模拟端处理）
        return self._send_delete(t, "pvc", r["uid"], r["name"])

    def _delete_ns_step(self, t: dict) -> bool:
        tid = t["id"]
        if not self._send_delete(t, "namespace", t["namespace_uid"], t["namespace_name"]):
            return False
        ns = self.sim.get_namespace_by_uid(t["namespace_uid"])
        remaining = self.sim.list_pvcs(ns_uid=t["namespace_uid"])
        if ns is None and not remaining:
            self.db.execute("UPDATE tickets SET state='DONE', done_at=? WHERE id=?",
                            (now_iso(), tid))
            return True
        self._set_blocked(tid, "命名空间或未选卷尚未完全移除，等待核对")
        return False

    # ---------------- 留存 ----------------
    def _copy_to_retained(self, t: dict, r: dict) -> None:
        tid = t["id"]
        pvc = self.sim.get_pvc(r["uid"])
        if not pvc:
            self._set_blocked(tid, f"选中 PVC {r['name']}(uid={r['uid']}) 已消失，无法留存")
            return
        src = self.sim.pv_data_path(pvc["pv_name"])
        dst_dir = self.retained_root / tid / r["name"]
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / "data.bin"
        shutil.copyfile(src, dst)
        with open(dst, "rb") as f:
            os.fsync(f.fileno())
        write_json(dst_dir / "meta.json", {
            "ticket_id": tid, "pvc_name": r["name"], "pvc_uid": r["uid"],
            "source_namespace": t["namespace_name"],
            "source_namespace_uid": t["namespace_uid"],
            "pv_name": r["pv_name"], "size_bytes": r["size_bytes"],
            "sha256": r["sha256"], "retained_at": now_iso(),
        })
        self.db.execute("UPDATE pvcs SET copy_state='copied' WHERE ticket_id=? AND uid=?",
                        (tid, r["uid"]))

    def _verify_retained(self, t: dict, r: dict) -> bool:
        """独立读回校验：只读留存目录中的副本，与开单时快照的 sha256 比对。"""
        tid = t["id"]
        dst = self.retained_root / tid / r["name"] / "data.bin"
        if not dst.exists():
            self.db.execute("UPDATE tickets SET state='FAILED', blocked_reason=? WHERE id=?",
                            (f"留存副本缺失: {dst}", tid))
            return False
        actual = sha256_file(dst)
        if actual != r["sha256"] or dst.stat().st_size != r["size_bytes"]:
            self.db.execute("UPDATE tickets SET state='FAILED', blocked_reason=? WHERE id=?",
                            (f"留存副本读回校验失败：{r['name']} sha256/大小与开单快照不符，"
                             "已中止（对应源 PVC 未删除）", tid))
            return False
        self.db.execute("UPDATE pvcs SET copy_state='verified' WHERE ticket_id=? AND uid=?",
                        (tid, r["uid"]))
        return True

    # ---------------- 删除（带幂等日志） ----------------
    def _send_delete(self, t: dict, kind: str, target_uid: str, target_name: str) -> bool:
        tid = t["id"]
        key = f"{tid}:{kind}:{target_uid}"
        op = self.db.one("SELECT * FROM ops WHERE idempotency_key=?", (key,))
        if op and op["state"] == "confirmed":
            return True  # 已确认的删除不再发送
        if op is None:
            self.db.execute(
                "INSERT INTO ops(idempotency_key,ticket_id,kind,target_uid,target_name,"
                "state,attempts,last_error,updated_at) VALUES(?,?,?,?,?,?,0,NULL,?)",
                (key, tid, kind, target_uid, target_name, "pending", now_iso()))
        try:
            if kind == "pvc":
                self.sim.delete_pvc(target_uid, key)
            else:
                self.sim.delete_namespace(target_uid, key)
        except DroppedResponseError as e:
            self.db.execute(
                "UPDATE ops SET state='unknown', attempts=attempts+1, last_error=?, "
                "updated_at=? WHERE idempotency_key=?", (str(e), now_iso(), key))
            if kind == "pvc":
                self.db.execute(
                    "UPDATE pvcs SET delete_state='unknown' WHERE ticket_id=? AND uid=?",
                    (tid, target_uid))
            self._set_blocked(
                tid,
                f"删除回包丢失：{kind} {target_name}(uid={target_uid}) 服务端可能已生效，"
                "结果未明；将在核对后续办（已确认的删除不会重发）")
            return False
        except NotFoundError as e:
            # 目标已不存在 -> 删除实际已生效，幂等确认为止
            self.db.execute(
                "UPDATE ops SET state='confirmed', attempts=attempts+1, last_error=?, "
                "updated_at=? WHERE idempotency_key=?",
                (f"target already gone: {e}", now_iso(), key))
            if kind == "pvc":
                self.db.execute(
                    "UPDATE pvcs SET delete_state='confirmed' WHERE ticket_id=? AND uid=?",
                    (tid, target_uid))
            return True
        self.db.execute(
            "UPDATE ops SET state='confirmed', attempts=attempts+1, updated_at=? "
            "WHERE idempotency_key=?", (now_iso(), key))
        if kind == "pvc":
            self.db.execute(
                "UPDATE pvcs SET delete_state='confirmed' WHERE ticket_id=? AND uid=?",
                (tid, target_uid))
        return True

    # ---------------- 核对（启动时与推进前） ----------------
    def reconcile(self, tid: str | None = None) -> int:
        """核对结果未明(unknown)的删除：目标已消失则标记确认（不重发），仍在则重发。"""
        resolved = 0
        with self._lock:
            if tid:
                rows = self.db.query(
                    "SELECT * FROM ops WHERE state='unknown' AND ticket_id=?", (tid,))
            else:
                rows = self.db.query("SELECT * FROM ops WHERE state='unknown'")
            for op in rows:
                if op["kind"] == "pvc":
                    exists = self.sim.get_pvc(op["target_uid"]) is not None
                else:
                    exists = self.sim.get_namespace_by_uid(op["target_uid"]) is not None
                if not exists:
                    self.db.execute(
                        "UPDATE ops SET state='confirmed', last_error=?, updated_at=? "
                        "WHERE idempotency_key=?",
                        ("reconciled: target already gone, delete took effect; NOT resent",
                         now_iso(), op["idempotency_key"]))
                    if op["kind"] == "pvc":
                        self.db.execute(
                            "UPDATE pvcs SET delete_state='confirmed' "
                            "WHERE ticket_id=? AND uid=?", (op["ticket_id"], op["target_uid"]))
                    resolved += 1
                else:
                    t = self._ticket(op["ticket_id"])
                    if self._send_delete(t, op["kind"], op["target_uid"], op["target_name"]):
                        resolved += 1
        return resolved

    # ---------------- 缺件保护（XOR 奇偶卷） ----------------
    def _job_lock(self, tid: str) -> threading.Lock:
        with self._job_locks_guard:
            lk = self._job_locks.get(tid)
            if lk is None:
                lk = threading.Lock()
                self._job_locks[tid] = lk
            return lk

    def _protection_dir(self, tid: str) -> Path:
        return self.retained_root / tid / ".protection"

    def _parity_path(self, tid: str) -> Path:
        return self._protection_dir(tid) / "parity.bin"

    def _parity_tmp(self, tid: str) -> Path:
        return self._protection_dir(tid) / "parity.tmp"

    def _rebuild_tmp(self, pvc_dir: Path) -> Path:
        return pvc_dir / ".rebuild.tmp"

    def _protection_row(self, tid: str) -> dict | None:
        return self.db.one("SELECT * FROM protection WHERE ticket_id=?", (tid,))

    def _selected_pvc_rows(self, tid: str) -> list[dict]:
        return self.db.query(
            "SELECT * FROM pvcs WHERE ticket_id=? AND selected=1 ORDER BY name", (tid,))

    def _protection_body_bytes(self, tid: str) -> int:
        """保护正文 + 临时正文的落盘字节。

        含保护目录内全部正文（parity.bin/parity.tmp）以及各卷目录下的重建临时正文；
        正式留存卷 data.bin 不计入本预算（其水位由工单 storage 口径统计）。
        """
        total = 0
        pdir = self._protection_dir(tid)
        if pdir.exists():
            total += sum(f.stat().st_size for f in pdir.iterdir() if f.is_file())
        tdir = self.retained_root / tid
        if tdir.exists():
            for d in tdir.iterdir():
                if d.is_dir():
                    for f in d.iterdir():
                        if f.is_file() and f.name.startswith(".") and f.name.endswith(".tmp"):
                            total += f.stat().st_size
        return total

    def _assert_protection_disk_budget(self, tid: str, volume_size: int) -> None:
        used = self._protection_body_bytes(tid)
        cap = PROTECTED_DISK_BUDGET_VOLUMES * volume_size
        if used > cap:
            raise ConflictError(
                f"保护存储上限保护：保护正文+临时正文 {used} 超过 {cap} 字节"
                f"（3 个卷大小），本次操作中止")

    def _recover_protection(self) -> None:
        """启动恢复：清掉中断遗留的临时正文（不累积），把 enabling 置为 interrupted。"""
        with self._lock:
            for row in self.db.query("SELECT * FROM protection"):
                tid = row["ticket_id"]
                for tmp in (self._parity_tmp(tid),):
                    if tmp.exists():
                        tmp.unlink()
                tdir = self.retained_root / tid
                if tdir.exists():
                    for d in tdir.iterdir():
                        rt = self._rebuild_tmp(d)
                        if d.is_dir() and rt.exists():
                            rt.unlink()
                if row["state"] == "enabling":
                    self.db.execute(
                        "UPDATE protection SET state='interrupted', updated_at=? WHERE ticket_id=?",
                        (now_iso(), tid))

    def enable_protection(self, tid: str) -> dict:
        """为已完成工单的等长留存卷启用缺件保护（可重试，临时正文不累积）。"""
        job = self._job_lock(tid)
        if not job.acquire(blocking=False):
            raise ConflictError(
                f"工单 {tid} 已有启用保护或缺件下载在处理中；同一工单同时只处理一次")
        try:
            with self._lock:
                t = self._ticket(tid)
                if t["state"] != "DONE":
                    raise ConflictError(f"工单 {tid} 尚未 DONE，不能启用缺件保护")
                rows = self._selected_pvc_rows(tid)
                if not (2 <= len(rows) <= MAX_PROTECTED_VOLUMES):
                    raise ValueError(
                        f"缺件保护要求 2~{MAX_PROTECTED_VOLUMES} 个留存卷，当前 {len(rows)} 个")
                data_paths = [self.retained_root / tid / r["name"] / "data.bin"
                              for r in rows]
                missing = [r["name"] for r, p in zip(rows, data_paths) if not p.exists()]
                if missing:
                    raise NotFoundError(
                        f"留存卷缺失，无法启用保护（请先通过缺件下载取回）: {missing}")
                sizes = {p.stat().st_size for p in data_paths}
                if len(sizes) != 1:
                    raise ValueError("留存卷大小不一致：缺件保护仅支持等长卷")
                v = sizes.pop()
                existing = self._protection_row(tid)
                if existing and existing["state"] == "protected" \
                        and self._parity_path(tid).exists():
                    return self.protection_status(tid)  # 已保护：幂等返回
                # 重试纪律：先清掉任何中断遗留的临时正文，保证不累积
                if self._parity_tmp(tid).exists():
                    self._parity_tmp(tid).unlink()
                self.db.execute(
                    "INSERT INTO protection(ticket_id,state,volume_size,volume_count,"
                    "parity_name,parity_sha256,enabled_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,NULL,?) ON CONFLICT(ticket_id) DO UPDATE SET "
                    "state=excluded.state, updated_at=excluded.updated_at",
                    (tid, "enabling", v, len(rows), "parity.bin", "", now_iso()))
                pdir = self._protection_dir(tid)
                pdir.mkdir(parents=True, exist_ok=True)
                tmp = self._parity_tmp(tid)
                final = self._parity_path(tid)
                # 启用读取预算：N 个留存卷生成 + 1 个保护卷暂存回读 = (N+1)V <= 5V
                meter = prot.ReadMeter((len(rows) + 1) * v)
                try:
                    parity_sha = prot.xor_streams(data_paths, tmp, meter, v)
                    self._assert_protection_disk_budget(tid, v)
                    # 保护数据完整落盘并读回核对通过后才改名、才显示已保护
                    prot.readback_verify(tmp, v, parity_sha, meter)
                    prot.durable_write_finish(tmp, final)
                    self.db.execute(
                        "UPDATE protection SET state='protected', parity_sha256=?, "
                        "enabled_at=?, updated_at=? WHERE ticket_id=?",
                        (parity_sha, now_iso(), now_iso(), tid))
                except Exception:
                    if tmp.exists():
                        tmp.unlink()
                    self.db.execute(
                        "UPDATE protection SET state='interrupted', updated_at=? WHERE ticket_id=?",
                        (now_iso(), tid))
                    raise
                status = self.protection_status(tid)
                status["enable"] = {
                    "read_bytes": meter.read_bytes,
                    "read_budget_bytes": meter.budget,
                    "parity_sha256": parity_sha,
                }
                return status
        finally:
            job.release()

    def _rebuild_missing(self, tid: str, row: dict) -> Path:
        """用保护卷 + 其余存活卷重建缺失的单个留存卷。"""
        job = self._job_lock(tid)
        if not job.acquire(blocking=False):
            raise ConflictError(
                f"工单 {tid} 已有启用保护或缺件下载在处理中；同一工单同时只处理一次")
        try:
            with self._lock:
                t = self._ticket(tid)
                if t["state"] != "DONE":
                    raise ConflictError(f"工单 {tid} 尚未 DONE，不提供缺件下载")
                p = self._protection_row(tid)
                if not p or p["state"] != "protected":
                    raise NotFoundError(
                        f"工单 {tid} 未启用缺件保护，无法取回缺失卷 {row['name']!r}")
                v = p["volume_size"]
                all_rows = self._selected_pvc_rows(tid)
                present, absent = [], []
                for r in all_rows:
                    dp = self.retained_root / tid / r["name"] / "data.bin"
                    (present if dp.exists() else absent).append((r, dp))
                target = self.retained_root / tid / row["name"] / "data.bin"
                if target.exists():
                    return target  # 等待锁期间已被别的请求重建好
                # 同单缺两件（或更多）：XOR 单奇偶卷数学上不可恢复
                if len(absent) >= 2:
                    raise ConflictError(
                        f"同单缺失 {len(absent)} 个留存卷 {[r['name'] for r, _ in absent]}，"
                        "缺件保护仅能重建单件，缺件下载失败")
                parity = self._parity_path(tid)
                if not parity.exists():
                    raise ConflictError(
                        "保护卷本身缺失，无法重建（保护数据不完整），缺件下载失败")
                inputs = [parity] + [dp for _, dp in present]
                if len(inputs) != len(all_rows):
                    raise ConflictError(
                        f"重建输入不足：需要 {len(all_rows)} 个等长卷，"
                        f"现存 {len(present)} 个 + 保护卷 1 个，缺件下载失败")
                bad_size = [str(dp) for _, dp in present if dp.stat().st_size != v]
                if bad_size or parity.stat().st_size != v:
                    raise ConflictError(
                        "现存留存卷/保护卷长度与开单快照不符，拒绝在不可信输入上重建")
                pvc_dir = target.parent
                pvc_dir.mkdir(parents=True, exist_ok=True)
                tmp = self._rebuild_tmp(pvc_dir)
                if tmp.exists():  # 上次中断的暂存正文：清掉重来，不累积
                    tmp.unlink()
                # 一次缺件下载最多读取 4V（N 个输入卷；N=4 时已读满，无独立回读预算）
                meter = prot.ReadMeter(REBUILD_READ_BUDGET_VOLUMES * v)
                try:
                    # 异或输出流上内联计算整卷 SHA-256：每个重建字节发送前均经哈希，
                    # 无需暂存回读即完成“整卷按开单 SHA-256 校验”
                    rebuilt_sha = prot.xor_streams(inputs, tmp, meter, v)
                    self._assert_protection_disk_budget(tid, v)
                    with open(tmp, "rb") as f:
                        os.fsync(f.fileno())
                    if tmp.stat().st_size != v or rebuilt_sha != row["sha256"]:
                        raise prot.ProtectionIntegrityError(
                            f"重建卷长度或 SHA-256 与开单快照不符："
                            f"期望 sha={row['sha256']} size={v}，"
                            f"实得 sha={rebuilt_sha} size={tmp.stat().st_size}")
                    prot.durable_write_finish(tmp, target)
                    # meta.json 若随正文一起被移走，按开单快照补写（数据以 DB 快照为准）
                    meta = pvc_dir / "meta.json"
                    if not meta.exists():
                        write_json(meta, {
                            "ticket_id": tid, "pvc_name": row["name"], "pvc_uid": row["uid"],
                            "source_namespace": t["namespace_name"],
                            "source_namespace_uid": t["namespace_uid"],
                            "pv_name": row["pv_name"], "size_bytes": row["size_bytes"],
                            "sha256": row["sha256"], "retained_at": now_iso(),
                            "rebuilt_from_protection": True,
                        })
                except prot.ProtectionIntegrityError as e:
                    if tmp.exists():
                        tmp.unlink()
                    raise ConflictError(
                        f"重建卷 {row['name']!r} 未通过开单 SHA-256 校验，缺件下载失败：{e}")
                except prot.ReadBudgetExceeded:
                    if tmp.exists():
                        tmp.unlink()
                    raise
                self._last_rebuild_io = {
                    "ticket_id": tid, "pvc_name": row["name"],
                    "read_bytes": meter.read_bytes, "read_budget_bytes": meter.budget,
                    "chunk_size": prot.CHUNK_SIZE,
                }
                return target
        finally:
            job.release()

    def protection_status(self, tid: str) -> dict:
        """缺件保护状态（/docs 可查）：保护态、各卷存活/缺失、落盘与预算水位。"""
        with self._lock:
            t = self._ticket(tid)
            rows = self._selected_pvc_rows(tid)
            p = self._protection_row(tid)
            volumes = []
            for r in rows:
                dp = self.retained_root / tid / r["name"] / "data.bin"
                volumes.append({
                    "pvc_name": r["name"], "size_bytes": r["size_bytes"],
                    "ticket_sha256": r["sha256"],
                    "present": dp.exists(),
                    "on_disk_bytes": dp.stat().st_size if dp.exists() else 0,
                })
            v = p["volume_size"] if p else (rows[0]["size_bytes"] if rows else 0)
            missing = [x["pvc_name"] for x in volumes if not x["present"]]
            parity_path = self._parity_path(tid)
            out = {
                "ticket_id": tid,
                "ticket_state": t["state"],
                "protection": dict(p) if p else None,
                "volumes": volumes,
                "missing_volumes": missing,
                "can_rebuild_single": (p is not None and p["state"] == "protected"
                                       and parity_path.exists() and len(missing) <= 1),
                "disk": {
                    "protection_plus_temp_bytes": self._protection_body_bytes(tid),
                    "cap_bytes": PROTECTED_DISK_BUDGET_VOLUMES * v if v else 0,
                    "cap_volumes": PROTECTED_DISK_BUDGET_VOLUMES,
                    "rebuild_read_budget_bytes":
                        REBUILD_READ_BUDGET_VOLUMES * v if v else 0,
                    "body_buffer_cap_bytes": 1024 * 1024,
                    "chunk_size": prot.CHUNK_SIZE,
                },
            }
            last = getattr(self, "_last_rebuild_io", None)
            if last and last["ticket_id"] == tid:
                out["last_rebuild_io"] = last
            return out

    # ---------------- 辅助 ----------------
    def _set_blocked(self, tid: str, reason: str | None) -> None:
        self.db.execute("UPDATE tickets SET blocked_reason=? WHERE id=?", (reason, tid))

    def _bump_peak(self, t: dict) -> None:
        current = self._current_bytes(t)
        self.db.execute("UPDATE tickets SET peak_bytes=MAX(peak_bytes, ?) WHERE id=?",
                        (current, t["id"]))

    def reset(self) -> None:
        with self._lock:
            self.sim.reset()
            shutil.rmtree(self.retained_root, ignore_errors=True)
            self.retained_root.mkdir(parents=True, exist_ok=True)
            for table in ("ops", "pvcs", "protection", "tickets"):
                self.db.execute(f"DELETE FROM {table}")
