#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import requests
import dns.resolver
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------------------
# 配置
# -----------------------------
URLS_FILE = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MERGED_FILE = "merged_rules.txt"
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
PARTS = 16

DNS_WORKERS = int(os.environ.get("DNS_WORKERS", 50))
DNS_TIMEOUT = 3
DELETE_THRESHOLD = 4

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# -----------------------------
# 下载并合并
# -----------------------------
def load_urls():
    if not os.path.exists(URLS_FILE):
        print(f"❌ {URLS_FILE} 不存在")
        exit(1)
    return [i.strip() for i in open(URLS_FILE, "r", encoding="utf-8") if i.strip()]

def download_and_merge():
    print("📥 下载与合并规则源...")
    urls = load_urls()
    merged = set()
    for u in urls:
        try:
            r = requests.get(u, timeout=20)
            r.raise_for_status()
            for line in r.text.splitlines():
                line = line.strip()
                if line and not line.startswith("!"):
                    merged.add(line)
            print(f"✅ 获取成功: {u}")
        except Exception as e:
            print(f"⚠ 下载失败: {u} → {e}")

    merged = sorted(merged)
    with open(MERGED_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(merged))
    print(f"✅ 合并完成，共 {len(merged)} 条规则")
    return merged

# -----------------------------
# 写分片 → tmp/
# -----------------------------
def split_tmp():
    if not os.path.exists(MERGED_FILE):
        download_and_merge()

    rules = [i.strip() for i in open(MERGED_FILE, "r", encoding="utf-8") if i.strip()]

    total = len(rules)
    per = (total + PARTS - 1) // PARTS
    print(f"🪓 开始写入 tmp/ 分片，每片约 {per} 条")

    for i in range(PARTS):
        part_rules = rules[i * per : (i + 1) * per]
        path = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 tmp/part_{i+1:02d}.txt → {len(part_rules)} 条")
    return True

# -----------------------------
# 写初始 validated_part → dist/
# -----------------------------
def init_dist_from_tmp():
    for i in range(1, PARTS + 1):
        tmp_file = os.path.join(TMP_DIR, f"part_{i:02d}.txt")
        dist_file = os.path.join(DIST_DIR, f"validated_part_{i:02d}.txt")

        if not os.path.exists(tmp_file):
            continue

        if not os.path.exists(dist_file):
            data = open(tmp_file, "r", encoding="utf-8").read()
            with open(dist_file, "w", encoding="utf-8") as f:
                f.write(data)
            print(f"✅ 初始写入 {dist_file}")

# -----------------------------
# 删除计数
# -----------------------------
def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        return json.load(open(DELETE_COUNTER_FILE, "r", encoding="utf-8"))
    return {}

def save_delete_counter(data):
    json.dump(data, open(DELETE_COUNTER_FILE, "w", encoding="utf-8"), indent=2)

# -----------------------------
# DNS 解析
# -----------------------------
def dns_valid(rule):
    domain = rule.replace("||", "").replace("^", "").replace("*", "")
    if not domain:
        return False
    try:
        dns.resolver.resolve(domain, "A", lifetime=DNS_TIMEOUT)
        return True
    except:
        return False

# -----------------------------
# 验证分片
# -----------------------------
def validate_part(n):
    part_file = os.path.join(DIST_DIR, f"validated_part_{n:02d}.txt")
    if not os.path.exists(part_file):
        print(f"❌ 分片不存在: {part_file}")
        return 0, 0, 0

    rules = [i.strip() for i in open(part_file, "r", encoding="utf-8") if i.strip()]
    print(f"🚀 分片 {n:02d} 开始验证，共 {len(rules)} 条")

    delete_counter = load_delete_counter()
    keep, remove = [], []

    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as pool:
        futures = {pool.submit(dns_valid, r): r for r in rules}
        done = 0
        for future in as_completed(futures):
            rule = futures[future]
            ok = future.result()
            done += 1
            if done % 500 == 0 or done == len(rules):
                print(f"✅ 已验证 {done}/{len(rules)}")

            if ok:
                keep.append(rule)
                delete_counter.pop(rule, None)
            else:
                delete_counter[rule] = delete_counter.get(rule, 0) + 1
                if delete_counter[rule] >= DELETE_THRESHOLD:
                    remove.append(rule)
                else:
                    keep.append(rule)
                    print(f"⚠ {rule} 连续失败 {delete_counter[rule]}/{DELETE_THRESHOLD}")

    save_delete_counter(delete_counter)

    with open(part_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(keep)))

    print(f"✅ 分片 {n:02d} → 保留 {len(keep)}, 删除 {len(remove)}")
    return len(rules), len(keep), len(remove)

# -----------------------------
# 入口
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载与切片")
    args = parser.parse_args()

    # 若缺失 merged 或 tmp 分片 → 自动生成
    if args.force_update or not os.path.exists(MERGED_FILE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        download_and_merge()
        split_tmp()

    init_dist_from_tmp()

    parts = [args.part] if args.part else list(range(1, PARTS + 1))

    total = kept = removed = 0
    for p in parts:
        t, k, r = validate_part(p)
        total += t
        kept += k
        removed += r

    print(f"COMMIT_STATS: 总 {total}, 有效 {kept}, 删除 {removed}")
