#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed

URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16
DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_THRESHOLD = 4  # 连续失败多少次才删除

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
if os.path.exists(DELETE_COUNTER_FILE):
    with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
        delete_counter = json.load(f)
else:
    delete_counter = {}

def save_delete_counter():
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(delete_counter, f, indent=2, ensure_ascii=False)

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
        print("⚠ 缺少合并规则文件")
        return False

    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]

    total = len(rules)
    per_part = (total + PARTS - 1) // PARTS
    print(f"🪓 正在分片 {total} 条，每片约 {per_part}")

    for i in range(PARTS):
        part_rules = rules[i*per_part:(i+1)*per_part]
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i+1}: {len(part_rules)} 条 → {filename}")

    return True

def check_domain(rule):
    resolver = dns.resolver.Resolver()
    resolver.timeout = DNS_TIMEOUT
    resolver.lifetime = DNS_TIMEOUT

    domain = rule.lstrip("|").split("^")[0].replace("*","")
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
            rule = futures[future]
            if result:
                valid.append(rule)
                delete_counter.pop(rule, None)  # 验证成功，计数清零
            else:
                count = delete_counter.get(rule, 0) + 1
                delete_counter[rule] = count
                if count < DELETE_THRESHOLD:
                    valid.append(rule)  # 不满阈值，保留

            if done % 500 == 0:
                print(f"✅ 已验证 {done}/{total} 条，有效 {len(valid)} 条")

    print(f"✅ 分片验证完成，有效 {len(valid)} 条")
    return valid

def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，自动重新下载")
        download_all_sources()
        split_parts()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在，终止")
        return

    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    print(f"⏱ 开始验证分片 {part}，共 {len(lines)} 条规则")

    old_rules = set()
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            old_rules = set(l.strip() for l in f if l.strip())

    valid = dns_validate(lines)
    valid_set = set(valid)
    removed_count = len(old_rules - valid_set)
    added_count = len(valid_set - old_rules)
    total_count = len(valid_set)

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(valid_set)))

    save_delete_counter()

    print(f"✅ 分片 {part} 完成: 总数 {total_count}, 新增 {added_count}, 删除 {removed_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载所有规则源并切片")
    args = parser.parse_args()

    if args.force_update:
        download_all_sources()
        split_parts()

    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR,"part_01.txt")):
        download_all_sources()
        split_parts()

    if args.part:
        process_part(args.part)
