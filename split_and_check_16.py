#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
import argparse
import dns.resolver
import json
from concurrent.futures import ThreadPoolExecutor

URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MERGED_FILE = "merged_rules.txt"
PARTS = 16
DNS_WORKERS = 60
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

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
    print(f"✅ 已更新删除计数器 → {DELETE_COUNTER_FILE}")

def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在！")
        return

    all_rules = set()

    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [l.strip() for l in f if l.strip()]

    print(f"🌐 开始下载 {len(urls)} 个规则源...")

    for url in urls:
        try:
            r = requests.get(url, timeout=20)
            if r.status_code == 200:
                for line in r.text.splitlines():
                    line = line.strip()
                    if line and not line.startswith("!"):
                        all_rules.add(line)
                print(f"✅ 下载成功: {url}")
            else:
                print(f"⚠ 下载失败: {url}")
        except Exception as e:
            print(f"⚠ 请求失败 {url}: {e}")

    with open(MERGED_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(all_rules)))

    print(f"✅ 已写入合并规则: {MERGED_FILE} 共 {len(all_rules)} 条")

def split_parts():
    if not os.path.exists(MERGED_FILE):
        print("❌ merged_rules.txt 不存在，无法分片")
        return

    with open(MERGED_FILE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]

    total = len(rules)
    size = total // PARTS + 1

    for i in range(PARTS):
        part_rules = rules[i*size:(i+1)*size]
        part_file = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📦 生成分片 {i+1:02d}，{len(part_rules)} 条")

def dns_check(domain):
    try:
        dns.resolver.resolve(domain, "A")
        return True
    except:
        return False

def extract_domain(rule):
    rule = rule.lstrip(".")
    if "/" in rule:
        rule = rule.split("/")[0]
    if "^" in rule:
        rule = rule.split("^")[0]
    if "*" in rule:
        rule = rule.replace("*", "")
    return rule

def process_part(part_index):
    part_file = os.path.join(TMP_DIR, f"part_{part_index:02d}.txt")
    if not os.path.exists(part_file):
        print(f"❌ 缺少分片文件: {part_file}")
        return

    print(f"⏱ 开始 DNS 验证分片 {part_index}")
    delete_counter = load_delete_counter()

    valid_rules = []
    removed_rules = []

    with open(part_file, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]

    results = {}
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        future_to_rule = {executor.submit(dns_check, extract_domain(rule)): rule for rule in rules}
        for future in future_to_rule:
            rule = future_to_rule[future]
            ok = future.result()
            results[rule] = ok

    for rule, ok in results.items():
        if ok:
            delete_counter[rule] = 0
            valid_rules.append(rule)
        else:
            delete_counter[rule] = delete_counter.get(rule, 0) + 1
            print(f"⚠ 连续删除计数 {delete_counter[rule]}/4: {rule}")

            if delete_counter[rule] >= 4:
                print(f"🗑 永久删除: {rule}")
                removed_rules.append(rule)
            else:
                valid_rules.append(rule)

    save_delete_counter(delete_counter)

    out_file = os.path.join(DIST_DIR, f"validated_part_{part_index:02d}.txt")
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(valid_rules))

    print(f"✅ 分片验证完成，有效 {len(valid_rules)} 条")
    print(f"🗑 永久删除 {len(removed_rules)} 条")
    print(f"📁 输出: {out_file}")
    print(f"COMMIT_STATS: Valid={len(valid_rules)} Removed={len(removed_rules)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="验证指定分片")
    parser.add_argument("--force-update", action="store_true", help="重新下载并分片")
    args = parser.parse_args()

    # ✅ 修复关键问题： force-update → force_update
    if args.force_update or not os.path.exists(MERGED_FILE):
        download_all_sources()
        split_parts()

    if args.part:
        process_part(args.part)
    else:
        print("✅ 规则已经准备完毕，可手动执行 --part 1~16")
