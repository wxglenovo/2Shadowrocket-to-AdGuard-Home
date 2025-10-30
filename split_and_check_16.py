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
DELETE_COUNTER = "delete_counter.json"
PARTS = 16
DNS_WORKERS = 50
DNS_TIMEOUT = 2
DELETE_THRESHOLD = 4  # 连续 4 次才删除

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

# 加载 delete_counter
if os.path.exists(DELETE_COUNTER):
    with open(DELETE_COUNTER, "r", encoding="utf-8") as f:
        delete_counter = json.load(f)
else:
    delete_counter = {}

def save_counter():
    with open(DELETE_COUNTER, "w", encoding="utf-8") as f:
        json.dump(delete_counter, f, ensure_ascii=False, indent=2)

def download_all_sources():
    if not os.path.exists(URLS_TXT):
        print("❌ urls.txt 不存在")
        return False
    merged = set()
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]
    for url in urls:
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            merged.update(line.strip() for line in r.text.splitlines() if line.strip() and not line.startswith("#"))
        except Exception as e:
            print(f"⚠ 下载失败 {url}: {e}")
    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(merged)))
    print(f"✅ 下载完成，共 {len(merged)} 条规则")
    return True

def split_parts():
    if not os.path.exists(MASTER_RULE):
        print("⚠ 缺少合并规则文件")
        return False
    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        rules = [l.strip() for l in f if l.strip()]
    per_part = (len(rules) + PARTS - 1) // PARTS
    for i in range(PARTS):
        part_rules = rules[i*per_part:(i+1)*per_part]
        with open(f"{TMP_DIR}/part_{i+1:02d}.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(part_rules))
    print(f"🪓 分片完成，每片约 {per_part} 条")
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
    valid = []
    with ThreadPoolExecutor(max_workers=DNS_WORKERS) as executor:
        futures = {executor.submit(check_domain, r): r for r in lines}
        total = len(lines)
        done = 0
        for f in as_completed(futures):
            done += 1
            r = f.result()
            if r:
                valid.append(r)
            if done % 500 == 0 or done == total:
                print(f"✅ 已验证 {done}/{total} 条，有效 {len(valid)} 条")
    return valid

def process_part(part):
    part_file = f"{TMP_DIR}/part_{int(part):02d}.txt"
    if not os.path.exists(part_file):
        print(f"⚠ 分片缺失 {part_file}, 自动重新下载切片")
        download_all_sources()
        split_parts()
    if not os.path.exists(part_file):
        print("❌ 分片仍不存在")
        return

    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    print(f"⏱ 验证分片 {part} 共 {len(lines)} 条规则")
    valid = dns_validate(lines)

    out_file = f"{DIST_DIR}/validated_part_{part}.txt"
    old_rules = set()
    if os.path.exists(out_file):
        with open(out_file,"r",encoding="utf-8") as f:
            old_rules = set(l.strip() for l in f if l.strip())

    # 连续删除逻辑
    to_delete = old_rules - set(valid)
    retained = set(valid)
    for r in to_delete:
        cnt = delete_counter.get(r,0) + 1
        delete_counter[r] = cnt
        print(f"⚠ 规则 {r} 连续失败 {cnt} 次")
        if cnt >= DELETE_THRESHOLD:
            print(f"❌ 删除规则 {r}")
        else:
            retained.add(r)  # 未达到阈值仍保留

    # 验证成功清零
    for r in valid:
        if r in delete_counter:
            delete_counter[r] = 0

    with open(out_file,"w",encoding="utf-8") as f:
        f.write("\n".join(sorted(retained)))

    save_counter()

    print(f"✅ 分片 {part} 完成：总 {len(retained)} 条，有效 {len(valid)} 条，待删除 {len(to_delete)} 条")

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part")
    parser.add_argument("--force-update", action="store_true")
    args = parser.parse_args()

    if args.force_update:
        download_all_sources()
        split_parts()

    if not os.path.exists(MASTER_RULE) or not os.path.exists(f"{TMP_DIR}/part_01.txt"):
        print("⚠ 缺少规则或分片，自动生成")
        download_all_sources()
        split_parts()

    if args.part:
        process_part(args.part)
