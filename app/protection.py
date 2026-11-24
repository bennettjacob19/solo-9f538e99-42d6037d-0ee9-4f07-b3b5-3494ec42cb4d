"""缺件保护：XOR 奇偶校验卷的生成、读回核对与单卷重建。

存储布局（均在留存根目录下，与原留存副本同级，不触碰模拟集群）：
  retained/<tid>/.protection/parity.bin   保护卷（异或奇偶校验卷，长度与留存卷等长）
  retained/<tid>/.protection/parity.tmp   生成保护卷时的临时正文（核对通过后原子改名）
  retained/<tid>/<pvc>/.rebuild.tmp       缺件重建时的临时正文（核对通过后原子改名）

资源约束（演练硬性要求，全部在代码中计量并强制执行）：
  * 保护正文 + 临时正文合计 <= 3 个卷的大小（3V），每个阶段边界检查；
  * 一次缺件下载最多读取 4 个卷大小的正文（含暂存回读），ReadMeter 逐字节计量，
    超限即 ReadBudgetExceeded，本次重建中止、临时正文清除；
  * 正文缓冲最多 1 MiB：分块 CHUNK=128KiB，重建时最多同时持有 4 个输入块（512KiB），
    异或以整数在块上进行，绝不整卷载入内存。

XOR 性质：N 个等长卷 D1..DN，保护卷 P = D1 xor D2 xor ... xor DN。
  * 任一单卷 Dk 缺失：Dk = P xor 其余所有卷（共 N 个输入，读取 N*V 字节，N<=4 -> <=4V）；
  * 同时缺两件时输入不足，数学上不可恢复，调用方直接报错、不启动重建。
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

CHUNK_SIZE = 128 * 1024  # 128 KiB；重建最多 4 个输入块同时在内存 -> 峰值 512 KiB < 1 MiB


class ReadBudgetExceeded(RuntimeError):
    """正文读取量超过本工序预算（缺件下载 4V，含暂存回读）。"""


class ProtectionIntegrityError(RuntimeError):
    """保护卷/重建卷读回核对失败（长度或 SHA-256 不符）。"""


class ReadMeter:
    """逐字节统计正文读取量，落盘文件的任何回读都必须经过本计量器。"""

    def __init__(self, budget_bytes: int):
        self.budget = budget_bytes
        self.read_bytes = 0

    def charge(self, n: int) -> None:
        self.read_bytes += n
        if self.read_bytes > self.budget:
            raise ReadBudgetExceeded(
                f"正文读取量 {self.read_bytes} 超过工序预算 {self.budget} 字节"
                f"（一次缺件下载最多读取 4 个卷大小，暂存回读同样计入）")


def _xor_chunk(acc: int, chunk: bytes) -> int:
    return acc ^ int.from_bytes(chunk, "little")


def xor_streams(inputs: list[Path], output: Path, meter: ReadMeter,
                expected_size: int) -> str:
    """对 N 个等长文件分块异或，流式写入 output，返回输出正文的 SHA-256。

    每个输入块读取即计入 meter；输出边算哈希边写，不做整卷缓冲。
    调用方负责保证各输入长度一致（=expected_size）。
    """
    handles = [open(p, "rb") for p in inputs]
    written = 0
    digest = hashlib.sha256()
    out_f = open(output, "wb")
    try:
        while True:
            chunks: list[bytes] = []
            for f in handles:
                chunk = f.read(CHUNK_SIZE)
                meter.charge(len(chunk))
                chunks.append(chunk)
            if not chunks[0]:
                break
            length = len(chunks[0])
            if any(len(c) != length for c in chunks):
                raise ProtectionIntegrityError(
                    "输入卷长度不一致，无法进行等长异或（疑似留存卷被截断）")
            acc = 0
            for c in chunks:
                acc = _xor_chunk(acc, c)
            out = acc.to_bytes(length, "little")
            out_f.write(out)
            digest.update(out)
            written += length
    finally:
        out_f.close()
        for f in handles:
            f.close()
    if written != expected_size:
        raise ProtectionIntegrityError(
            f"异或输出长度 {written} 与期望卷大小 {expected_size} 不符")
    return digest.hexdigest()


def fsync_dir(path: Path) -> None:
    """目录项 fsync，保证改名/新建在掉电后仍可见。"""
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def durable_write_finish(tmp_path: Path, final_path: Path) -> None:
    """临时正文 fsync 后原子改名，并 fsync 目录，确保完整落盘后才可见。"""
    with open(tmp_path, "rb") as f:
        os.fsync(f.fileno())
    tmp_path.replace(final_path)
    fsync_dir(final_path.parent)


def readback_verify(path: Path, expected_size: int, expected_sha256: str,
                    meter: ReadMeter) -> None:
    """暂存回读核对：长度 + SHA-256；回读字节计入 meter。

    用于启用保护后的保护卷核对。缺件重建路径不使用本函数：N=4 时仅输入卷
    就已读满 4V 预算，独立回读会超限；重建改为在异或输出流上内联计算整卷
    SHA-256（见 xor_streams 返回值），落盘后再以 stat 复核长度。
    """
    if not path.exists():
        raise ProtectionIntegrityError(f"待核对正文不存在: {path}")
    h = hashlib.sha256()
    total = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            meter.charge(len(chunk))
            h.update(chunk)
            total += len(chunk)
    if total != expected_size:
        raise ProtectionIntegrityError(
            f"{path.name} 回读长度 {total} != 期望 {expected_size}")
    if h.hexdigest() != expected_sha256:
        raise ProtectionIntegrityError(
            f"{path.name} 回读 SHA-256 核对失败：期望 {expected_sha256}，实得 {h.hexdigest()}")
