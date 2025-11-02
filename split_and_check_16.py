#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import argparse
import requests
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------------------------------------------------
# ✅ 配置区域
# -------------------------------------------------
URLS_FILE = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")  # ✅ 计数保留，不覆盖
PARTS = 16
DNS_TIMEOUT = 3
DNS_WORKERS = 60
BATCH = 500

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# -------------------------------------------------
# ✅ 加载/初始化删除计数
# -------------------------------------------------
def load_delete_counter():
    if not os.path.exists(DELETE_COUNTER_FILE):
        return {}
    try:
        with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, ensure_ascii=False, indent=2)

# -------------------------------------------------
# ✅ 下载并合并全部规则源 → merged_rules.txt
# -------------------------------------------------
def download_rules():
    merged_file = os.path.join(TMP_DIR, "merged_rules.txt")

    if os.path.exists(merged_file):
        print("✅ merged_rules.txt 已存在，不重新下载")
        return merged_file

    all_rules = set()
    print("⏬ 下载规则源…")

    with open(URLS_FILE, "r", encoding="utf-8") as f:
        for url in f.read().splitlines():
            if not url.strip():
                continue
            try:
                print(f"🌐 正在下载：{url}")
                resp = requests.get(url, timeout=10)
                for line in resp.text.splitlines():
                    line = line.strip()
                    if line and not line.startswith("#"):
                        all_rules.add(line)
            except:
                print(f"⚠ 下载失败：{url}")

    with open(merged_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(all_rules)))

    print(f"✅ 合并完成，共 {len(all_rules)} 条")
    return merged_file

# -------------------------------------------------
# ✅ 拆分16片 → tmp/part_01.txt ~ part_16.txt
# ✅ ✅ 修复重点：强制写入，不会再出现 tmp 里没有分片
# -------------------------------------------------
def split_parts(merged_file):
    with open(merged_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()

    total = len(rules)
    per = max(1, total // PARTS)

    print(f"🔪 开始拆分：总 {total} | 每片约 {per}")

    for i in range(PARTS):
        part_rules = rules[i * per: (i + 1) * per]
        part_file = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")

        # ✅【改动】强制生成，不依赖旧文件
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))

        print(f"✅ 生成 {part_file} ({len(part_rules)})")

# -------------------------------------------------
# ✅ DNS 验证
# -------------------------------------------------
def dns_check(domain):
    try:
        dns.resolver.resolve(domain, "A", lifetime=DNS_TIMEOUT)
        return True
    except:
        return False

def validate_part(part_id):
    part_file = os.path.join(TMP_DIR, f"part_{part_id:02d}.txt")
    validated_file = os.path.join(DIST_DIR, f"validated_part_{part_id:02d}.txt")

    if not os.path.exists(part_file):
        print(f"⚠ 分片不存在：{part_file}")
        return

    print(f"⏱ 开始验证分片 {part_id}")

    with open(part_file, "r", encoding="utf-8") as f:
        rules = f.read().splitlines()

    delete_counter = load_delete_counter()
    keep = []
    deleted = 0

    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as pool:
        future_map = {pool.submit(dns_check, r): r for r in rules}

        for fut in as_completed(future_map):
            rule = future_map[fut]
            ok = fut.result()

            if ok:
                delete_counter[rule] = 0  # ✅ 成功 → 重置计数
                keep.append(rule)
            else:
                # ✅ 失败 → +1
                if rule not in delete_counter:
                    delete_counter[rule] = 4  # ✅ 新增初始值 = 4
                else:
                    delete_counter[rule] += 1

                if delete_counter[rule] >= 4:  # ✅ 达到阈值 → 真删除
                    deleted += 1
                else:
                    keep.append(rule)

    # ✅ 保存更新计数
    save_delete_counter(delete_counter)

    # ✅ 写入验证结果
    with open(validated_file, "w", encoding="utf-8") as f:
        f.write("\n".join(keep))

    print(f"✅ 分片 {part_id} 完成 | 保留 {len(keep)} | 删除 {deleted}")

# -------------------------------------------------
# ✅ 主入口
# -------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, default=0, help="仅验证指定分片")
    parser.add_argument("--force-download", action="store_true", help="强制重新下载全部规则源")
    args = parser.parse_args()

    merged_file = os.path.join(TMP_DIR, "merged_rules.txt")

    # ✅ 强制下载或文件不存在 → 下载
    if args.force_download or not os.path.exists(merged_file):
        merged_file = download_rules()
        split_parts(merged_file)

    if args.part:
        validate_part(args.part)
    else:
        for p in range(1, PARTS + 1):
            validate_part(p)

    print("✅ 全部分片验证结束")
