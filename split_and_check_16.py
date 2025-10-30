#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
split_and_check_16.py
用于 AdGuard Home 大型规则的分片、下载、DNS 有效性验证、增量更新统计。
"""

import os
import re
import sys
import time
import argparse
import dns.resolver
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# 规则源
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
MASTER_RULE = "merged_rules.txt"
PARTS = 16

# DNS 线程池
DNS_THREADS = 100
DNS_TIMEOUT = 2

def safe_domain(line: str):
    """提取域名"""
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    line = re.sub(r"^\|\|", "", line)
    line = re.sub(r"\^$", "", line)
    if re.match(r"^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", line):
        return line.lower()
    return None

def fetch_rules():
    """下载并合并所有规则"""
    if not os.path.exists(URLS_TXT):
        print(f"❌ 找不到 {URLS_TXT}")
        sys.exit(1)

    os.makedirs(TMP_DIR, exist_ok=True)
    all_rules = set()
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        urls = [x.strip() for x in f if x.strip()]

    for url in urls:
        print(f"📥 下载：{url}")
        try:
            res = requests.get(url, timeout=15)
            res.raise_for_status()
            for line in res.text.splitlines():
                d = safe_domain(line)
                if d:
                    all_rules.add(d)
        except Exception as e:
            print(f"⚠️ 无法下载 {url}: {e}")

    with open(MASTER_RULE, "w", encoding="utf-8") as f:
        for d in sorted(all_rules):
            f.write(d + "\n")

    print(f"✅ 合并完成，共 {len(all_rules)} 条规则。")
    return len(all_rules)

def split_rules():
    """按 16 份分片"""
    with open(MASTER_RULE, "r", encoding="utf-8") as f:
        all_lines = [x.strip() for x in f if x.strip()]
    chunk_size = len(all_lines) // PARTS + 1
    for i in range(PARTS):
        start = i * chunk_size
        end = start + chunk_size
        part_lines = all_lines[start:end]
        out_file = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(out_file, "w", encoding="utf-8") as f:
            f.write("\n".join(part_lines))
        print(f"🧩 生成分片 {i+1:02d}：{len(part_lines)} 条")

def dns_check(domain):
    """DNS 检查"""
    resolver = dns.resolver.Resolver()
    resolver.lifetime = DNS_TIMEOUT
    resolver.timeout = DNS_TIMEOUT
    try:
        resolver.resolve(domain, "A")
        return True
    except Exception:
        return False

def validate_part(part_index):
    """验证某个分片的域名有效性"""
    os.makedirs(DIST_DIR, exist_ok=True)
    part_file = os.path.join(TMP_DIR, f"part_{part_index:02d}.txt")
    out_file = os.path.join(DIST_DIR, f"validated_part_{part_index:02d}.txt")

    if not os.path.exists(part_file):
        print(f"❌ 未找到 {part_file}")
        return

    with open(part_file, "r", encoding="utf-8") as f:
        domains = [x.strip() for x in f if x.strip()]

    prev = set()
    if os.path.exists(out_file):
        with open(out_file, "r", encoding="utf-8") as f:
            prev = {x.strip() for x in f if x.strip() and not x.startswith("总数")}

    valid = set()
    print(f"🔍 正在验证分片 {part_index:02d} 共 {len(domains)} 条...")

    with ThreadPoolExecutor(max_workers=DNS_THREADS) as executor:
        futures = {executor.submit(dns_check, d): d for d in domains}
        for i, fut in enumerate(as_completed(futures)):
            d = futures[fut]
            if fut.result():
                valid.add(d)
            if i % 500 == 0 and i:
                print(f"  已验证 {i}/{len(domains)}")

    added = len(valid - prev)
    removed = len(prev - valid)

    with open(out_file, "w", encoding="utf-8") as f:
        f.write(f"总数: {len(valid)}\n新增: {added}\n删除: {removed}\n\n")
        for d in sorted(valid):
            f.write(d + "\n")

    print(f"✅ 分片 {part_index:02d} 验证完成：总数 {len(valid)} | 新增 {added} | 删除 {removed}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", type=int, help="指定验证分片号 1~16")
    parser.add_argument("--force-update", action="store_true", help="强制下载与分片")
    args = parser.parse_args()

    if args.force_update:
        print("🚀 强制更新规则源...")
        fetch_rules()
        split_rules()
        return

    if args.part:
        validate_part(args.part)
    else:
        print("⚠ 未指定分片。使用 --part 运行验证。")

if __name__ == "__main__":
    main()
