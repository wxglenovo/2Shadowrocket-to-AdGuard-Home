#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import dns.resolver
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===============================
# 配置
# ===============================
URLS_FILE = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MERGED_FILE = "merged_rules.txt"
PARTS = 16
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
DELETE_THRESHOLD = 4

# 并发参数
DNS_WORKERS = int(os.environ.get("DNS_WORKERS", 50))
DNS_BATCH_SIZE = int(os.environ.get("DNS_BATCH_SIZE", 500))

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 下载规则并合并
# ===============================
def load_urls():
    if not os.path.exists(URLS_FILE):
        print(f"❌ {URLS_FILE} 不存在")
        exit(1)
    with open(URLS_FILE, "r", encoding="utf-8") as f:
        return [u.strip() for u in f if u.strip()]

def download_and_merge(urls):
    merged_rules = set()
    for url in urls:
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            for line in r.text.splitlines():
                line = line.strip()
                if line and not line.startswith("!"):
                    merged_rules.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")
    merged_list = sorted(merged_rules)
    with open(MERGED_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(merged_list))
    print(f"✅ 合并完成，共 {len(merged_list)} 条规则")
    return merged_list

# ===============================
# 分片生成
# ===============================
def split_rules():
    if not os.path.exists(MERGED_FILE):
        print("⚠ 缺少合并规则文件")
        return
    with open(MERGED_FILE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]
    total = len(rules)
    per_part = (total + PARTS - 1) // PARTS
    for i in range(PARTS):
        part_rules = rules[i * per_part:(i + 1) * per_part]
        path = os.path.join(DIST_DIR, f"validated_part_{i+1:02d}.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"✅ 分片 {i+1:02d} → {len(part_rules)} 条")

# ===============================
# 删除计数管理
# ===============================
def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        try:
            with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, indent=2, ensure_ascii=False)

# ===============================
# DNS 验证
# ===============================
def dns_check(domain):
    resolver = dns.resolver.Resolver()
    resolver.timeout = 3
    resolver.lifetime = 3
    try:
        resolver.resolve(domain)
        return True
    except:
        return False

def validate_rules(rules, concurrent=False):
    kept = []
    deleted = []
    delete_counter = load_delete_counter()

    if concurrent:
        print(f"🚀 并发验证 {DNS_WORKERS} 线程，每批 {DNS_BATCH_SIZE} 条")
        def check_rule(rule):
            domain = rule.replace("||", "").replace("^", "")
            ok = dns_check(domain)
            return rule, ok

        with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
            futures = [executor.submit(check_rule, r) for r in rules]
            for i, future in enumerate(as_completed(futures), 1):
                rule, ok = future.result()
                if ok:
                    kept.append(rule)
                    delete_counter.pop(rule, None)
                else:
                    delete_counter[rule] = delete_counter.get(rule, 0) + 1
                    if delete_counter[rule] < DELETE_THRESHOLD:
                        kept.append(rule)
                        print(f"⚠ 连续删除计数 {delete_counter[rule]}/{DELETE_THRESHOLD}: {rule}")
                    else:
                        deleted.append(rule)
                        print(f"❌ {rule} 已连续 {DELETE_THRESHOLD} 次失败 → 移除")
                if i % DNS_BATCH_SIZE == 0 or i == len(rules):
                    print(f"✅ 已验证 {i}/{len(rules)} 条，有效 {len(kept)} 条")
    else:
        for rule in rules:
            domain = rule.replace("||", "").replace("^", "")
            ok = dns_check(domain)
            if ok:
                kept.append(rule)
                delete_counter.pop(rule, None)
            else:
                delete_counter[rule] = delete_counter.get(rule, 0) + 1
                if delete_counter[rule] < DELETE_THRESHOLD:
                    kept.append(rule)
                else:
                    deleted.append(rule)

    save_delete_counter(delete_counter)
    return kept, deleted

# ===============================
# 分片处理
# ===============================
def process_part(part_index, concurrent=False):
    part_file = os.path.join(DIST_DIR, f"validated_part_{int(part_index):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part_index} 不存在 → 重新生成")
        split_rules()
        if not os.path.exists(part_file):
            print("❌ 分片生成失败，终止")
            return 0,0,0

    with open(part_file, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]

    print(f"⏱ 验证分片 {part_index} 共 {len(rules)} 条")
    kept, deleted = validate_rules(rules, concurrent=concurrent)

    with open(part_file, "w", encoding="utf-8") as f:
        f.write("\n".join(kept))

    print(f"✅ 分片 {part_index} → 保留 {len(kept)}, 删除 {len(deleted)}")
    print(f"COMMIT_STATS: 总 {len(rules)}, 新增 {len(kept)}, 删除 {len(deleted)}")
    return len(rules), len(kept), len(deleted)

# ===============================
# 主函数
# ===============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="验证指定分片")
    parser.add_argument("--concurrent", action="store_true", help="开启并发验证")
    parser.add_argument("--force", action="store_true", help="强制下载并生成分片")
    args = parser.parse_args()

    # 下载 & 合并规则
    if args.force or not os.path.exists(MERGED_FILE):
        urls = load_urls()
        download_and_merge(urls)
        split_rules()

    # 生成分片（如果缺失）
    for i in range(1, PARTS+1):
        part_file = os.path.join(DIST_DIR, f"validated_part_{i:02d}.txt")
        if not os.path.exists(part_file):
            split_rules()
            break

    # 验证分片
    parts_to_check = [args.part] if args.part else list(range(1, PARTS+1))
    total_rules, total_kept, total_deleted = 0, 0, 0

    for idx in parts_to_check:
        t,k,d = process_part(idx, concurrent=args.concurrent)
        total_rules += t
        total_kept += k
        total_deleted += d

    print(f"🤖 Auto update: validated part {parts_to_check[-1]} → 总 {total_rules}, 新增 {total_kept}, 删除 {total_deleted}")
