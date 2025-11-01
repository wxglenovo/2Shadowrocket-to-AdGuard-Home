#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===============================
# 配置
# ===============================
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16
DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
DELETE_THRESHOLD = 4

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 下载与合并规则
# ===============================
def download_all_sources():
    merged = set()
    if os.path.exists(URLS_TXT):
        with open(URLS_TXT, "r", encoding="utf-8") as f:
            urls = [u.strip() for u in f if u.strip()]
        for url in urls:
            try:
                r = requests.get(url, timeout=20)
                r.raise_for_status()
                for line in r.text.splitlines():
                    line = line.strip()
                    if line and not line.startswith("#"):
                        merged.add(line)
            except Exception as e:
                print(f"⚠ 下载失败 {url}: {e}")
    else:
        print("⚠ urls.txt 不存在，使用空规则集")

    print(f"✅ 合并 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))
    return True

# ===============================
# 分片
# ===============================
def split_parts():
    rules = []
    if os.path.exists(MASTER_RULE):
        with open(MASTER_RULE, "r", encoding="utf-8") as f:
            rules = [l.strip() for l in f if l.strip()]

    total = len(rules)
    per_part = (total + PARTS - 1) // PARTS if total else 1  # 空规则时每片1条空

    print(f"🪓 分片 {total} 条，每片约 {per_part}")

    for i in range(PARTS):
        part_rules = rules[i * per_part:(i + 1) * per_part] if total else []
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        # 确保文件总是被创建，即使为空
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i+1}: {len(part_rules)} 条 → {filename}")
    return True

# ===============================
# DNS 验证（不变）
# ===============================
def check_domain(rule):
    resolver = dns.resolver.Resolver()
    resolver.timeout = DNS_TIMEOUT
    resolver.lifetime = DNS_TIMEOUT
    domain = rule.lstrip("|").split("^")[0].replace("*", "")
    if not domain:
        return None
    try:
        resolver.resolve(domain)
        return rule
    except:
        return None

def dns_validate(lines):
    valid = []
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = {executor.submit(check_domain, rule): rule for rule in lines}
        for future in as_completed(futures):
            result = future.result()
            if result:
                valid.append(result)
    return valid

# ===============================
# 删除计数管理（不变）
# ===============================
def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        try:
            with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    else:
        with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2, ensure_ascii=False)
        return {}

def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, indent=2, ensure_ascii=False)

# ===============================
# 分片处理
# ===============================
def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新生成")
        split_parts()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    valid = set(dns_validate(lines))
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")

    old_rules = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_delete_counter()
    new_delete_counter = {}

    final_rules = set()
    removed_count = 0
    added_count = 0

    for rule in old_rules | set(lines):
        if rule in valid:
            final_rules.add(rule)
            new_delete_counter[rule] = 0
        else:
            count = delete_counter.get(rule, 0) + 1
            new_delete_counter[rule] = count
            if count >= DELETE_THRESHOLD:
                removed_count += 1
            else:
                final_rules.add(rule)
        if rule not in old_rules and rule in valid:
            added_count += 1

    save_delete_counter(new_delete_counter)

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    print(f"COMMIT_STATS: 总 {len(final_rules)}, 新增 {added_count}, 删除 {removed_count}")

# ===============================
# 主函数
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载规则源并切片")
    args = parser.parse_args()

    if args.force_update:
        download_all_sources()
        split_parts()

    # 确保分片至少存在 part_01.txt
    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()
        split_parts()

    if args.part:
        process_part(args.part)
