#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
import argparse
import dns.resolver
import concurrent.futures
import json

URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16
DNS_BATCH_SIZE = 800
CONCURRENCY = 50
DELETE_COUNT_FILE = os.path.join(DIST_DIR, "delete_counter.json")

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)
os.makedirs("logs", exist_ok=True)

def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False
    print("📥 开始下载规则源...")
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
    print(f"✅ 下载完成，共 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))
    return True

def split_parts():
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少合并规则文件")
        return False
    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]
    total = len(rules)
    per_part = (total + PARTS - 1) // PARTS
    for i in range(PARTS):
        part_rules = rules[i * per_part:(i + 1) * per_part]
        filename = os.path.join(TMP_DIR, f"part_{i + 1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
    return True

def dns_validate(lines):
    valid = []
    resolver = dns.resolver.Resolver()
    resolver.timeout = 2
    resolver.lifetime = 2

    def check_domain(rule):
        domain = rule.lstrip("|").split("^")[0].replace("*","")
        if not domain:
            return None
        try:
            resolver.resolve(domain)
            return rule
        except:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        for result in executor.map(check_domain, lines):
            if result:
                valid.append(result)
    return valid

def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片缺失：{part_file}, 自动下载规则源并切片")
        download_all_sources()
        split_parts()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    with open(part_file, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]

    print(f"⏱ 开始验证分片 {part}，共 {len(lines)} 条规则")
    valid = dns_validate(lines)
    print(f"✅ 分片验证完成，有效 {len(valid)} 条")

    # 连续删除机制
    delete_counter = {}
    counter_file = DELETE_COUNT_FILE
    if os.path.exists(counter_file):
        with open(counter_file, "r", encoding="utf-8") as f:
            delete_counter = json.load(f)

    to_delete = set(lines) - set(valid)
    updated_rules = []
    for rule in lines:
        if rule in to_delete:
            count = delete_counter.get(rule, 0) + 1
            delete_counter[rule] = count
            if count >= 4:
                print(f"删除规则：{rule}")
                continue
        else:
            delete_counter[rule] = 0
        updated_rules.append(rule)

    out_file = os.path.join(DIST_DIR, f"validated_part_{int(part):02d}.txt")
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(updated_rules))

    with open(counter_file, "w", encoding="utf-8") as f:
        json.dump(delete_counter, f, indent=2, ensure_ascii=False)

    print(f"✅ 分片 {part} 更新完成 → {out_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载所有规则源并切片")
    args = parser.parse_args()

    if args.force_update:
        download_all_sources()
        split_parts()

    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则文件或分片，自动拉取规则源并切片")
        download_all_sources()
        split_parts()

    if args.part:
        process_part(args.part)
