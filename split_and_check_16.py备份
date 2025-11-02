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

# ✅ 新逻辑：阈值 = 4 （计数 >=4 删除）
DELETE_THRESHOLD = 4

# 创建目录
os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ===============================
# 下载与合并规则
# ===============================
def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False
    print("📥 下载规则源...")
    merged = set()
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]
    for url in urls:
        print(f"🌐 获取 {url}")
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            for line in r.text.splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    merged.add(line)
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")
    print(f"✅ 合并 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))
    return True

# ===============================
# 分片
# ===============================
def split_parts():
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少合并规则文件")
        return False
    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]
    total = len(rules)
    per_part = (total + PARTS - 1) // PARTS
    print(f"🪓 分片 {total} 条，每片约 {per_part}")
    for i in range(PARTS):
        part_rules = rules[i * per_part:(i + 1) * per_part]
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i+1}: {len(part_rules)} 条 → {filename}")
    return True

# ===============================
# DNS 验证
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
    print(f"🚀 启动 {DNS_WORKERS} 并发验证")
    valid = []
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = {executor.submit(check_domain, rule): rule for rule in lines}
        total = len(lines)
        done = 0
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result:
                valid.append(result)
            if done % 500 == 0:
                print(f"✅ 已验证 {done}/{total} 条，有效 {len(valid)} 条")
    print(f"✅ 分片验证完成，有效 {len(valid)} 条")
    return valid

# ===============================
# 删除计数管理
# ===============================
def load_delete_counter():
    if os.path.exists(DELETE_COUNTER_FILE):
        try:
            with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            print(f"⚠ {DELETE_COUNTER_FILE} 解析失败，重建空计数")
            return {}
    else:
        print(f"⚠ {DELETE_COUNTER_FILE} 不存在，创建新文件")
        os.makedirs(DIST_DIR, exist_ok=True)
        with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2, ensure_ascii=False)
        return {}

def save_delete_counter(counter):
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(counter, f, indent=2, ensure_ascii=False)

# ===============================
# 分片处理（最终逻辑已改）
# ===============================
def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，重新下载并切片")
        download_all_sources()
        split_parts()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    print(f"⏱ 验证分片 {part}，共 {len(lines)} 条规则")
    valid = set(dns_validate(lines))
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")

    old_rules = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set([l.strip() for l in f if l.strip()])

    delete_counter = load_delete_counter()
    new_delete_counter = delete_counter.copy()

    final_rules = set()
    removed_count = 0
    added_count = 0

    # ✅ old_rules ∪ 当前分片的所有规则
    all_rules = old_rules | set(lines)

    for rule in all_rules:

        # ✅ 验证成功 → 保留 & 重置计数 = 0
        if rule in valid:
            final_rules.add(rule)
            ### ✅ 改动：成功一次 → 计数重置为 0
            new_delete_counter[rule] = 0

            # ✅ 新增的有效规则
            if rule not in old_rules:
                added_count += 1

        else:
            # 获取旧计数，没有则认为第一次失败
            old_count = delete_counter.get(rule, None)

            ### ✅ 改动：如果不存在旧计数，新增时计数 = 4
            if old_count is None:
                new_count = 4
            else:
                new_count = old_count + 1        # ✅ 失败一次 +1

            new_delete_counter[rule] = new_count
            print(f"⚠ 连续失败计数 = {new_count} ：{rule}")

            # ✅ 核心逻辑：计数 >= 4 → 删除，不保留
            if new_count >= DELETE_THRESHOLD:
                removed_count += 1
            else:
                final_rules.add(rule)  # ✅ 未达阈值继续保留

    save_delete_counter(new_delete_counter)

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    total_count = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")
    print(f"COMMIT_STATS: 总 {total_count}, 新增 {added_count}, 删除 {removed_count}")

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

    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则或分片，自动拉取")
        download_all_sources()
        split_parts()

    if args.part:
        process_part(args.part)
