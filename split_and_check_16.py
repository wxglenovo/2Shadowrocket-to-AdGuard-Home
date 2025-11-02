#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
功能说明（supports 16 分片）：
1) 下载与合并多个规则源
2) 拆分 16 个分片
3) DNS 并发验证每个分片
4) 无效规则计数策略：
   - 新增规则默认：计数 = 4
   - 失败一次：计数 +1
   - 计数 ≥ 4：删除
   - 任意一次成功：计数重置为 0
   - 若计数 > 7：跳过验证 10 次，10 次后自动恢复计数 = 4
"""

import os
import re
import argparse
import requests
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------------------
# 全局配置
# -----------------------------
URLS_FILE = "urls.txt"  # 规则源地址
TMP_DIR = "tmp"
DIST_DIR = "dist"
MERGED_FILE = os.path.join(DIST_DIR, "merged_rules.txt")
COUNTER_DIR = os.path.join(TMP_DIR, "counters")
SKIP_DIR = os.path.join(TMP_DIR, "skip_rounds")
PARTS = 16
DNS_WORKERS = 80     # DNS并发线程
BATCH_SIZE = 300     # 每批验证条数

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)
os.makedirs(COUNTER_DIR, exist_ok=True)
os.makedirs(SKIP_DIR, exist_ok=True)


# -----------------------------
# 工具方法：加载计数
# -----------------------------
def load_counter(rule: str) -> int:
    key = re.sub(r'[^A-Za-z0-9]+', '_', rule)
    fp = os.path.join(COUNTER_DIR, key + ".txt")
    if not os.path.exists(fp):
        return 4  # 新规则第一次出现计数 = 4
    try:
        return int(open(fp).read().strip())
    except:
        return 4


# -----------------------------
# 工具方法：保存计数
# -----------------------------
def save_counter(rule: str, value: int):
    key = re.sub(r'[^A-Za-z0-9]+', '_', rule)
    fp = os.path.join(COUNTER_DIR, key + ".txt")
    with open(fp, "w") as f:
        f.write(str(value))


# -----------------------------
# 工具方法：读取跳过轮次
# -----------------------------
def load_skip_round(rule: str) -> int:
    key = re.sub(r'[^A-Za-z0-9]+', '_', rule)
    fp = os.path.join(SKIP_DIR, key + ".txt")
    if not os.path.exists(fp): return 0
    try:
        return int(open(fp).read().strip())
    except:
        return 0


# -----------------------------
# 工具方法：保存跳过轮次
# -----------------------------
def save_skip_round(rule: str, rounds: int):
    key = re.sub(r'[^A-Za-z0-9]+', '_', rule)
    fp = os.path.join(SKIP_DIR, key + ".txt")
    with open(fp, "w") as f:
        f.write(str(rounds))


# -----------------------------
# 下载与合并规则
# -----------------------------
def download_and_merge():
    all_rules = []
    for url in open(URLS_FILE):
        url = url.strip()
        if not url:
            continue
        try:
            print(f"🔗 下载：{url}")
            txt = requests.get(url, timeout=15).text
            for line in txt.splitlines():
                line = line.strip()
                if line and not line.startswith("!"):
                    all_rules.append(line)
        except:
            print(f"❌ 失败：{url}")

    # 去重
    all_rules = list(sorted(set(all_rules)))
    with open(MERGED_FILE, "w") as f:
        f.write("\n".join(all_rules))
    print(f"✅ 合并完成，总数：{len(all_rules)}")


# -----------------------------
# 拆分 16 分片
# -----------------------------
def split_parts():
    rules = open(MERGED_FILE).read().splitlines()
    chunk = len(rules) // PARTS + 1
    for i in range(PARTS):
        part_rules = rules[i*chunk:(i+1)*chunk]
        fname = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(fname, "w") as f:
            f.write("\n".join(part_rules))
        print(f"📦 分片 {i+1:02d}：{len(part_rules)} 条")


# -----------------------------
# DNS验证函数
# -----------------------------
def dns_check(rule: str) -> bool:
    try:
        domain = rule.replace("||", "").replace("^", "")
        dns.resolver.resolve(domain, "A")
        return True
    except:
        return False


# -----------------------------
# 验证某个分片
# -----------------------------
def validate_part(part: int):
    src_file = os.path.join(TMP_DIR, f"part_{part:02d}.txt")
    dst_file = os.path.join(DIST_DIR, f"validated_part_{part:02d}.txt")

    rules = open(src_file).read().splitlines()
    valid = []

    print(f"⏱ 开始验证分片 {part}")

    for i in range(0, len(rules), BATCH_SIZE):
        batch = rules[i:i+BATCH_SIZE]

        # 并发执行 DNS
        with ThreadPoolExecutor(max_workers=DNS_WORKERS) as ex:
            futs = {ex.submit(dns_check, r): r for r in batch}

            for fut in as_completed(futs):
                rule = futs[fut]

                # 加载计数与跳过状态
                count = load_counter(rule)
                skip = load_skip_round(rule)

                # 如果计数 > 7，且 skip < 10 → 跳过验证，不删除不变
                if count > 7 and skip < 10:
                    valid.append(rule)
                    save_skip_round(rule, skip + 1)
                    continue

                # 真实验证
                ok = False
                try:
                    ok = fut.result()
                except:
                    ok = False

                if ok:
                    # 成功 → 计数清零 + 清除 skip，并加入结果
                    save_counter(rule, 0)
                    save_skip_round(rule, 0)
                    valid.append(rule)
                else:
                    # 失败 → 计数 +1
                    count += 1
                    save_counter(rule, count)

                    # 若计数 ≥ 4 → 真删
                    if count >= 4:
                        print(f"🗑 删除规则：{rule}（计数={count}）")
                        continue

                    # 否则暂时保留
                    valid.append(rule)

                # 若计数 > 7 → 进入跳过模式，记录 skip=1
                if count > 7:
                    save_skip_round(rule, 1)

    # 回写
    with open(dst_file, "w") as f:
        f.write("\n".join(valid))
    print(f"✅ 分片 {part} 验证完成：保留 {len(valid)} 条")


# -----------------------------
# main 入口
# -----------------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--part", help="只验证某个分片 1~16", type=int)
    p.add_argument("--force-update", action="store_true", help="强制重新下载合并")
    args = p.parse_args()

    # 如果没有 merged 文件或强制刷新 → 下载 + 合并
    if not os.path.exists(MERGED_FILE) or args.force_update:
        download_and_merge()
        split_parts()

    # 只验证某一片
    if args.part:
        validate_part(args.part)
    else:
        for k in range(1, PARTS+1):
            validate_part(k)


if __name__ == "__main__":
    main()
