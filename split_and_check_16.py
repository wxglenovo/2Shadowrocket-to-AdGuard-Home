#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import requests
import argparse
import dns.resolver
import json
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
LOGS_DIR = "logs"
MASTER_RULE = "merged_rules.txt"
PARTS = 16
DNS_BATCH_SIZE = 800
THREADS = 50
DELETE_RECORD = os.path.join(LOGS_DIR, "delete_record.json")

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False
    print("📥 开始下载所有规则源...")
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

    print(f"✅ 下载完成，共合并 {len(merged)} 条规则")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))
    return True

def split_parts():
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少合并规则文件，无法切片")
        return False
    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]
    total = len(rules)
    per_part = (total + PARTS - 1) // PARTS
    print(f"🪓 分片 {total} 条，每片约 {per_part}")
    for i in range(PARTS):
        part_rules = rules[i*per_part:(i+1)*per_part]
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i+1}: {len(part_rules)} 条 → {filename}")
    return True

def dns_validate(rule):
    resolver = dns.resolver.Resolver()
    resolver.timeout = 2
    resolver.lifetime = 2
    domain = rule.lstrip("|").split("^")[0].replace("*", "")
    if not domain:
        return None
    try:
        resolver.resolve(domain)
        return rule
    except:
        return None

def load_delete_record():
    if os.path.exists(DELETE_RECORD):
        with open(DELETE_RECORD, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_delete_record(record):
    with open(DELETE_RECORD, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)

def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片缺失：{part_file}，自动重新下载规则源并切片")
        download_all_sources()
        split_parts()

    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    with open(part_file, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]

    print(f"⏱ 开始验证分片 {part}，共 {len(rules)} 条规则")
    valid_rules = []

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        for i, res in enumerate(executor.map(dns_validate, rules), 1):
            if res:
                valid_rules.append(res)
            if i % DNS_BATCH_SIZE == 0:
                print(f"✅ 已验证 {i}/{len(rules)} 条，有效 {len(valid_rules)} 条")

    print(f"✅ 分片验证完成，有效 {len(valid_rules)} 条")

    # 处理删除逻辑：连续4次无效才删除
    delete_record = load_delete_record()
    final_rules = []

    for rule in rules:
        if rule in valid_rules:
            delete_record[rule] = 0
            final_rules.append(rule)
        else:
            delete_record[rule] = delete_record.get(rule, 0) + 1
            # 打印每条规则连续无效次数
            count = delete_record[rule]
            print(f"⚠ 规则无效次数 {count}：{rule}")
            if count < 4:
                final_rules.append(rule)
            else:
                print(f"🗑 连续 4 次无效，删除：{rule}")

    save_delete_record(delete_record)

    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(final_rules))
    print(f"✅ 分片 {part} 保存完成 → {out_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制下载规则源并切片")
    args = parser.parse_args()

    if args.force_update:
        download_all_sources()
        split_parts()

    # 自动补缺分片
    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则文件或分片，自动拉取规则源并切片")
        download_all_sources()
        split_parts()

    # 默认自动轮替
    LAST_PART_FILE = ".last_part"
    if args.part:
        PART = int(args.part)
    else:
        if os.path.exists(LAST_PART_FILE):
            with open(LAST_PART_FILE, "r") as f:
                PART = int(f.read().strip())
            PART = (PART % PARTS) + 1
        else:
            PART = 1

    with open(LAST_PART_FILE, "w") as f:
        f.write(str(PART))

    process_part(PART)
