#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import requests
import argparse
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

URLS_TXT = "urls.txt"   # 存放规则源地址
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16
DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_COUNTER_FILE = "delete_counter.json"

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ---------------- 下载并合并规则 ----------------
def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在，无法获取规则源")
        return False
    print("📥 开始下载规则源...")
    merged = set()
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
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))
    print(f"✅ 下载完成，共 {len(merged)} 条规则")
    return True

# ---------------- 分片 ----------------
def split_parts():
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少合并规则文件")
        return False
    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]
    total = len(rules)
    per_part = (total + PARTS - 1) // PARTS
    for i in range(PARTS):
        part_rules = rules[i*per_part:(i+1)*per_part]
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
    print(f"🪓 分片完成，每片约 {per_part} 条")

# ---------------- DNS 验证 ----------------
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
        done = 0
        total = len(lines)
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result:
                valid.append(result)
            if done % 500 == 0:
                print(f"✅ 已验证 {done}/{total} 条，有效 {len(valid)}")
    return valid

# ---------------- 处理分片 ----------------
def process_part(part, stats_json=False):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        download_all_sources()
        split_parts()
    if not os.path.exists(part_file):
        print("❌ 分片不存在")
        return

    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    print(f"⏱ 开始验证分片 {part}，共 {len(lines)} 条")

    valid = dns_validate(lines)
    old_rules = set()
    out_file = os.path.join(DIST_DIR, f"validated_part_{part:02d}.txt")

    if os.path.exists(out_file):
        old_rules = set(open(out_file, "r", encoding="utf-8").read().splitlines())

    # 加载删除计数
    delete_counter = {}
    if os.path.exists(DELETE_COUNTER_FILE):
        delete_counter = json.load(open(DELETE_COUNTER_FILE, "r", encoding="utf-8"))

    final_rules = []
    added = 0
    removed = 0

    for rule in valid:
        final_rules.append(rule)
        if rule not in old_rules:
            added += 1

    for rule in old_rules:
        if rule not in valid:
            count = delete_counter.get(rule, 0) + 1
            delete_counter[rule] = count
            if count < 4:
                final_rules.append(rule)
            removed += 1

    # 保存新的计数
    json.dump(delete_counter, open(DELETE_COUNTER_FILE, "w", encoding="utf-8"), indent=2)

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))

    total = len(final_rules)
    print(f"✅ 分片 {part} 完成: 总数 {total}, 新增 {added}, 删除 {removed}")

    if stats_json:
        return {"total": total, "added": added, "removed": removed}
    else:
        return valid, added, removed, total

# ---------------- 主程序 ----------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制重新下载所有规则源并切片")
    parser.add_argument("--stats-json", action="store_true", help="输出 JSON 统计")
    args = parser.parse_args()

    if args.force_update:
        download_all_sources()
        split_parts()

    if args.part:
        stats = process_part(args.part, stats_json=args.stats_json)
        if args.stats_json:
            print(json.dumps(stats))
        else:
            print(f"总数 {stats[3]}, 新增 {stats[1]}, 删除 {stats[2]}")
