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
DELETE_COUNTER_FILE = os.path.join(DIST_DIR, "delete_counter.json")
DELETE_THRESHOLD = 4  # 连续 4 次才删除

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# ======= 下载 & 切片 =======
def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False
    print("📥 开始下载所有规则源...")
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
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
        print(f"📄 分片 {i+1}: {len(part_rules)} 条 → {filename}")
    return True

# ======= DNS 验证 =======
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
        done = 0
        total = len(lines)
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result:
                valid.append(result)
            if done % 500 == 0:
                print(f"✅ 已验证 {done}/{total} 条，有效 {len(valid)} 条")
    print(f"✅ 分片验证完成，有效 {len(valid)} 条")
    return valid

# ======= 处理分片 & 连续删除 =======
def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片 {part} 缺失，自动下载切片")
        download_all_sources()
        split_parts()
        if not os.path.exists(part_file):
            print("❌ 分片仍不存在，终止")
            return

    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    print(f"⏱ 开始验证分片 {part}，共 {len(lines)} 条规则")
    valid = dns_validate(lines)
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")

    # 读取历史规则
    old_rules = set()
    if os.path.exists(out_file):
        old_rules = set(open(out_file, "r", encoding="utf-8").read().splitlines())

    # 读取 delete_counter.json
    delete_counter = {}
    if os.path.exists(DELETE_COUNTER_FILE):
        with open(DELETE_COUNTER_FILE, "r", encoding="utf-8") as f:
            delete_counter = json.load(f)

    # 更新 delete_counter
    current_rules = set(lines)
    to_delete = old_rules - set(valid)
    updated_rules = set(valid)
    for rule in old_rules:
        if rule in to_delete:
            delete_counter[rule] = delete_counter.get(rule, 0) + 1
        else:
            delete_counter[rule] = 0
            updated_rules.add(rule)

    # 实际删除满足 threshold 的规则
    final_rules = set()
    removed_count = 0
    for rule in updated_rules:
        count = delete_counter.get(rule, 0)
        if count >= DELETE_THRESHOLD:
            removed_count += 1
            delete_counter.pop(rule, None)
        else:
            final_rules.add(rule)

    # 写入文件
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(final_rules)))
    with open(DELETE_COUNTER_FILE, "w", encoding="utf-8") as f:
        json.dump(delete_counter, f, indent=2, ensure_ascii=False)

    print(f"✅ 分片 {part} 完成: 总 {len(final_rules)+removed_count}, 新增 {len(valid - old_rules)}, 删除 {removed_count}")

# ======= 主逻辑 =======
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="验证指定分片 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制更新规则源并切片")
    args = parser.parse_args()

    if args.force_update:
        download_all_sources()
        split_parts()

    # 缺失文件自动补
    if not os.path.exists(MASTER_RULE) or not os.path.exists(os.path.join(TMP_DIR, "part_01.txt")):
        print("⚠ 缺少规则文件或分片，自动拉取规则源并切片")
        download_all_sources()
        split_parts()

    if args.part:
        process_part(args.part)
