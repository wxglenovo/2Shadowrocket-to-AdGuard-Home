#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import requests
import argparse
import time
import dns.resolver
from concurrent.futures import ThreadPoolExecutor

DNS_BATCH_SIZE = 800  # 每批验证数量
URLS_TXT = "urls.txt"
TMP_DIR = "tmp"
DIST_DIR = "dist"
PARTS = 16

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

def download_urls():
    url = "https://raw.githubusercontent.com/wxglenovo/Shadowrocket-to-AdGuard-Home/main/urls.txt"
    print(f"📥 下载 urls.txt ...")
    r = requests.get(url)
    r.raise_for_status()
    with open(URLS_TXT, "w", encoding="utf-8") as f:
        f.write(r.text)
    print(f"✅ urls.txt 下载完成，{len(r.text.splitlines())} 条规则")

def split_parts():
    with open(URLS_TXT, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    total = len(lines)
    per_part = (total + PARTS - 1) // PARTS
    for i in range(PARTS):
        part_lines = lines[i*per_part:(i+1)*per_part]
        filename = os.path.join(TMP_DIR, f"part_{i+1:02d}.txt")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(part_lines))
        print(f"📄 分片 {i+1} 保存 {len(part_lines)} 条规则 → {filename}")

def dns_validate_parallel(lines, max_workers=50):
    valid = []
    resolver = dns.resolver.Resolver()
    resolver.timeout = 2
    resolver.lifetime = 2

    def check(domain_line):
        domain = domain_line.lstrip("|").split("^")[0].replace("*", "")
        if not domain:
            return None
        try:
            resolver.resolve(domain)
            return domain_line
        except Exception:
            return None

    start = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, result in enumerate(executor.map(check, lines), 1):
            if result:
                valid.append(result)
            if idx % DNS_BATCH_SIZE == 0 or idx == len(lines):
                print(f"✅ 已验证 {idx}/{len(lines)} 条，本批有效 {len(valid)} 条")
    print(f"🎯 分片处理完成，有效 {len(valid)}/{len(lines)} 条，用时 {time.time()-start:.1f} 秒")
    return valid

def process_part(part):
    part_file = os.path.join(TMP_DIR, f"part_{int(part):02d}.txt")
    if not os.path.exists(part_file):
        print(f"⚠ 分片文件不存在: {part_file}")
        return
    lines = open(part_file, "r", encoding="utf-8").read().splitlines()
    print(f"⏱ 当前处理分片：{part_file}, 总规则 {len(lines)} 条")
    valid = dns_validate_parallel(lines)
    out_file = os.path.join(DIST_DIR, f"validated_part_{part}.txt")
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(valid))
    print(f"📄 分片 {part} 验证完成，有效规则保存 → {out_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", help="指定分片 1~16", required=True)
    args = parser.parse_args()

    # 自动下载和分片（如果不存在 urls.txt）
    if not os.path.exists(URLS_TXT):
        download_urls()
        split_parts()

    process_part(args.part)
