#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import requests
import dns.resolver
from datetime import datetime

# 配置
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
DNS_BATCH_SIZE = 800
NUM_PARTS = 16

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

def download_urls():
    print("📥 下载 urls.txt ...")
    url = "https://raw.githubusercontent.com/your-repo/urls.txt"  # 替换为你的源
    r = requests.get(url)
    r.raise_for_status()
    with open(URLS_TXT, "w", encoding="utf-8") as f:
        f.write(r.text)
    print(f"✅ 下载完成，共 {len(r.text.splitlines())} 条规则")
    return r.text.splitlines()

def split_urls(lines):
    total = len(lines)
    part_size = total // NUM_PARTS + 1
    part_files = []
    for i in range(NUM_PARTS):
        part_lines = lines[i*part_size:(i+1)*part_size]
        part_file = os.path.join(TMP_DIR, f"part_{i+1:02}.txt")
        with open(part_file, "w", encoding="utf-8") as f:
            f.write("\n".join(part_lines))
        print(f"📄 分片 {i+1} 保存 {len(part_lines)} 条规则 → {part_file}")
        print(f"前 10 条示例： {part_lines[:10]}")
        part_files.append(part_file)
    return part_files

def dns_check(lines):
    resolver = dns.resolver.Resolver()
    valid = []
    total = len(lines)
    for i in range(0, total, DNS_BATCH_SIZE):
        batch = lines[i:i+DNS_BATCH_SIZE]
        batch_valid = []
        for rule in batch:
            domain = rule.lstrip("|").rstrip("^")
            try:
                resolver.resolve(domain)
                batch_valid.append(rule)
            except:
                pass
        valid.extend(batch_valid)
        print(f"✅ 已验证 {min(i+DNS_BATCH_SIZE,total)}/{total} 条，本批有效 {len(batch_valid)} 条")
        print(f"前 10 条规则示例： {batch_valid[:10]}")
    return valid

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--update", action="store_true", help="更新 urls.txt 并切片")
    parser.add_argument("--part", type=int, help="手动验证指定分片 0~15")
    args = parser.parse_args()

    if args.update:
        lines = download_urls()
        split_urls(lines)
        return

    part_files = [os.path.join(TMP_DIR, f"part_{i+1:02}.txt") for i in range(NUM_PARTS)]

    if args.part is not None:
        idx = args.part
        if idx < 0 or idx >= NUM_PARTS:
            print("❌ 分片索引超出范围")
            return
        part_files = [part_files[idx]]

    all_valid = []
    for part_file in part_files:
        with open(part_file, "r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip()]
        print(f"⏱ 当前处理分片：{part_file}, 总规则 {len(lines)} 条")
        print(f"前 10 条规则示例： {lines[:10]}")
        valid = dns_check(lines)
        all_valid.extend(valid)

    # 保存全部有效规则
    valid_file = os.path.join(DIST_DIR, "blocklist_valid.txt")
    with open(valid_file, "w", encoding="utf-8") as f:
        f.write("\n".join(all_valid))
    print(f"📂 已保存 {len(all_valid)} 条有效规则 → {valid_file}")

if __name__ == "__main__":
    main()
